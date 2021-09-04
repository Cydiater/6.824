package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"bytes"
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
}

type KVServer struct {
	mu						sync.Mutex
	me						int
	rf						*raft.Raft
	applyCh				chan raft.ApplyMsg
	dead					int32 // set by Kill()

	maxraftstate	int // snapshot if log grows this big
	kv						map[string]string
	ansChan				map[string]chan struct {string; bool}
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
		UUID: uuid.NewString(),
	}
	kv.ansChan[op.UUID] = make(chan struct {string; bool})
	c := kv.ansChan[op.UUID]
	kv.mu.Unlock()
	kv.rf.Start(op.marshall())
	res := <-c
	reply.Err = "unknown err"
	if res.bool {
		reply.Err = "ok"
		reply.Value = res.string;
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
		UUID: uuid.NewString(),
	}
	kv.ansChan[op.UUID] = make(chan struct {string; bool})
	c := kv.ansChan[op.UUID]
	kv.mu.Unlock()
	kv.rf.Start(op.marshall())
	res := <-c
	reply.Err = "unknown err"
	if res.bool {
		reply.Err = "ok"
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
	// Your code here, if desired.
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
	e.Encode(op.UUID);
	return buf.Bytes()
}

func (op *Op) unmarshall(b []byte) {
	buf := bytes.NewBuffer(b)
	d := labgob.NewDecoder(buf)
	d.Decode(&op.Action)
	d.Decode(&op.Key)
	d.Decode(&op.Value)
	d.Decode(&op.UUID)
}

func (kv *KVServer) maintainKV() {
	for apply := range kv.applyCh {
		op := Op{}
		b, ok := apply.Command.([]byte)
		if !ok {
			log.Panicf("Failed to unmarshall to op")
		}
		op.unmarshall(b)
		kv.mu.Lock()
		c, haveChan := kv.ansChan[op.UUID]
		if op.Action == "Get" {
			v, ok := kv.kv[op.Key]
			if !ok {
				v = ""
			}
			if haveChan {
				c <- struct {string; bool} {v, true}
			}
		} else if op.Action == "Put" {
			kv.kv[op.Key] = op.Value
			if haveChan {
				c <- struct {string; bool} {"", true}
			}
		} else if op.Action == "Append" {
			v, ok := kv.kv[op.Key]
			if !ok {
				v = ""
			} 
			v += op.Value
			kv.kv[op.Key] = v
			if haveChan {
				c <- struct {string; bool} {"", true}
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
	kv.ansChan = make(map[string]chan struct {string; bool})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.maintainKV();

	return kv
}
