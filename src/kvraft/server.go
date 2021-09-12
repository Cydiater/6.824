package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"context"
	"../raft"
	"sync"
	"bytes"
	"time"
	"sync/atomic"
	"github.com/google/uuid"
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Action				string
	Key						string
	Value					string
	UUID					string
	ClientID			string
	ReplyTo				int
	SessionID			string
}

type KVServer struct {
	mu						sync.Mutex
	me						int
	rf						*raft.Raft
	applyCh				chan raft.ApplyMsg
	dead					int32 // set by Kill()

	maxraftstate	int // snapshot if log grows this big
	kv						map[string]string
	ansChan				map[string]chan struct {string; bool} // make a chan for each session
	lastOpUUID		map[string]string
	cancel				context.CancelFunc
	lastApplied		raft.ApplyMsg
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	_, is_leader := kv.rf.GetState()
	if !is_leader {
		reply.Err = "not leader"
		kv.mu.Unlock()
		return
	}
	op := Op {
		Action: "Get",
		Key: args.Key,
		Value: "",
		UUID: args.OpID,
		ClientID: args.ClientID,
		ReplyTo: kv.me,
		SessionID: uuid.NewString(),
	}
	c := make(chan struct {string; bool}, 1)
	kv.ansChan[op.SessionID] = c
	kv.mu.Unlock()
	index, term, ok := kv.rf.Start(op.marshall())
	if !ok {
		reply.Err = "not leader"
		return
	}
	log.Printf("%v: start index = %v term = %v op = %+v", kv.me, index, term, op)
	notify := make(chan bool)
	go kv.checkCommit(term, op.UUID, index, notify)
	select {
	case res := <-c:
		reply.Err = "unknown err"
		if res.bool {
			reply.Err = "ok"
			reply.Value = res.string;
		}
	case <-notify:
		log.Printf("%v: not commit %+v", kv.me, op)
		reply.Err = "not commit"
	}
}

func (kv *KVServer) checkCommit(term int, OpID string, commitIndex int, notify chan bool) {
	for {
		kv.mu.Lock()
		currentTerm, is_leader := kv.rf.GetState()
		if currentTerm != term || !is_leader {
			kv.mu.Unlock()
			notify <- true
			return
		}
		if kv.lastApplied.CommandIndex > commitIndex {
			kv.mu.Unlock()
			notify <- true
			return
		}
		if kv.lastApplied.CommandIndex == commitIndex {
			op := Op {  }
			op.unmarshall(kv.lastApplied.Command.([]byte))
			kv.mu.Unlock()
			if op.UUID != OpID {
				notify <- true
			}
			return
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	_, is_leader := kv.rf.GetState()
	if !is_leader {
		reply.Err = "not leader"
		kv.mu.Unlock()
		return
	}
	op := Op {
		Action: args.Op,
		Key: args.Key,
		Value: args.Value,
		UUID: args.OpID,
		ClientID: args.ClientID,
		ReplyTo: kv.me,
		SessionID: uuid.NewString(),
	}
	c := make(chan struct {string; bool}, 1)
	kv.ansChan[op.SessionID] = c
	kv.mu.Unlock()
	index, term, ok := kv.rf.Start(op.marshall())
	if !ok {
		reply.Err = "not leader"
		return
	}
	log.Printf("%v: start index = %v term = %v op = %+v", kv.me, index, term, op)
	notify := make(chan bool)
	go kv.checkCommit(term, op.UUID, index, notify)
	select {
	case res := <-c:
		reply.Err = "unknown err"
		if res.bool {
			reply.Err = "ok"
		}
	case <-notify:
		log.Printf("%v: not commit %+v", kv.me, op)
		reply.Err = "not commit"
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.cancel()
	log.Printf("%v: killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (op *Op) marshall() []byte {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(op.Action)
	e.Encode(op.Key)
	e.Encode(op.Value)
	e.Encode(op.UUID)
	e.Encode(op.ClientID)
	e.Encode(op.ReplyTo)
	e.Encode(op.SessionID)
	return buf.Bytes()
}

func (op *Op) unmarshall(b []byte) {
	buf := bytes.NewBuffer(b)
	d := labgob.NewDecoder(buf)
	d.Decode(&op.Action)
	d.Decode(&op.Key)
	d.Decode(&op.Value)
	d.Decode(&op.UUID)
	d.Decode(&op.ClientID)
	d.Decode(&op.ReplyTo)
	d.Decode(&op.SessionID)
}

func (kv *KVServer) maintainKV(ctx context.Context) {
	var apply raft.ApplyMsg
	for {
		select {
		case apply = <-kv.applyCh:
		case <-ctx.Done():
			log.Printf("%v: cancel", kv.me)
			return
		}
		kv.lastApplied = apply
		op := Op{}
		b, ok := apply.Command.([]byte)
		if !ok {
			log.Panicf("Failed to unmarshall to op")
		}
		op.unmarshall(b)
		kv.mu.Lock()
		log.Printf("%v: applied index = %v op = %+v", kv.me, apply.CommandIndex, op)
		if op.Action == "Get" {
			v, ok := kv.kv[op.Key]
			if !ok {
				v = ""
			}
			kv.lastOpUUID[op.ClientID] = op.UUID
			if op.ReplyTo == kv.me {
				c, ok := kv.ansChan[op.SessionID]
				if !ok {
					log.Printf("%v: ansChan for %v not found", kv.me, op.SessionID)
					kv.mu.Unlock()
					continue
				}
				c <- struct {string; bool} {v, true}
				delete(kv.ansChan, op.SessionID)
			}
		} else if op.Action == "Put" {
			id, ok := kv.lastOpUUID[op.ClientID]
			if !ok || op.UUID != id {
				kv.kv[op.Key] = op.Value
				kv.lastOpUUID[op.ClientID] = op.UUID
			} else {
				log.Printf("%v: skipped %+v", kv.me, op)
			}
			if op.ReplyTo == kv.me {
				c, ok := kv.ansChan[op.SessionID]
				if !ok {
					log.Printf("%v: ansChan for %v not found", kv.me, op.SessionID)
					kv.mu.Unlock()
					continue
				}
				c <- struct {string; bool} {"", true}
				delete(kv.ansChan, op.SessionID)
			}
		} else if op.Action == "Append" {
			id, ok := kv.lastOpUUID[op.ClientID]
			if !ok || op.UUID != id {
				v, ok := kv.kv[op.Key]
				if !ok {
					v = ""
				} 
				v += op.Value
				kv.kv[op.Key] = v
				kv.lastOpUUID[op.ClientID] = op.UUID
			} else {
				log.Printf("%v: skipped %+v", kv.me, op)
			}
			if op.ReplyTo == kv.me {
				c, ok := kv.ansChan[op.SessionID]
				if !ok {
					log.Printf("%v: ansChan for %v not found", kv.me, op.SessionID)
					kv.mu.Unlock()
					continue;
				}
				c <- struct {string; bool} {"", true}
				delete(kv.ansChan, op.SessionID)
			}
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.kv = make(map[string]string)
	kv.lastOpUUID = make(map[string]string)
	kv.ansChan = make(map[string]chan struct {string; bool})
	kv.lastApplied.CommandIndex = -1

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	var ctx context.Context
	ctx, kv.cancel = context.WithCancel(context.Background())

	go kv.maintainKV(ctx)

	return kv
}
