package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "log"
import "github.com/google/uuid"


type Clerk struct {
	servers				[]*labrpc.ClientEnd
	recentLeader	int
	id						string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.recentLeader = 0
	ck.id = uuid.NewString()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	i := ck.recentLeader
	OpID := uuid.NewString()
	for {
		if i == len(ck.servers) {
			i = 0
		}
		args := GetArgs { key, ck.id, OpID, i }
		reply := GetReply {  }
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			log.Printf("%v: Get with key %v failed: network", i, key)
			i += 1
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "not leader" {
			log.Printf("%v: Get with key %v failed: not leader", i, key)
			i += 1
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "unknown err" || reply.Err == "not commit" {
			log.Printf("%v: Get with key %v failed: %v", i, key, reply.Err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ck.recentLeader = i;
		log.Printf("%v: Get with key %v success value = %v, OpID = %v", i, key, reply.Value, OpID)
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	i := ck.recentLeader
	OpID := uuid.NewString()
	for {
		if i == len(ck.servers) {
			i = 0
		}
		args := PutAppendArgs { key, value, op, ck.id, OpID, i }
		reply := PutAppendReply {  }
		log.Printf("i = %v args = %+v", i, args)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			log.Printf("%v: PutAppend with (%v, %v, %v) failed: network", i, op, key, value)
			i += 1
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "not leader" {
			log.Printf("%v: PutAppend with (%v, %v, %v) failed: not leader", i, op, key, value)
			i += 1
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "unknown err" || reply.Err == "not commit" {
			log.Printf("%v: PutAppend with (%v, %v, %v) failed: %v", i, op, key, value, reply.Err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ck.recentLeader = i
		log.Printf("%v: PutAppend with (%v, %v, %v) success OpID = %v", i, op, key, value, OpID)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
