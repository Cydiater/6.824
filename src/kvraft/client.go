package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "log"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
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
	i := 0
	for {
		args := GetArgs { key }
		reply := GetReply {  }
		if i == len(ck.servers) {
			i = 0
		}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			i += 1
			log.Printf("%v: Get with key %v failed: network", i, key)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "not leader" {
			i += 1
			log.Printf("%v: Get with key %v failed: not leader", i, key)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "unknown err" {
			log.Printf("%v: Get with key %v failed: unknown", i, key)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		log.Printf("%v: Get with key %v sucess value = %v", i, key, reply.Value)
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
	i := 0
	for {
		if i == len(ck.servers) {
			i = 0
		}
		args := PutAppendArgs { key, value, op }
		reply := PutAppendReply {  }
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			i += 1
			log.Printf("%v: PutAppend with (%v, %v, %v) failed: network", i, op, key, value)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "not leader" {
			i += 1
			log.Printf("%v: PutAppend with (%v, %v, %v) failed: not leader", i, op, key, value)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == "unknown err" {
			log.Printf("%v: PutAppend with (%v, %v, %v) failed: unknown", i, op, key, value)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		log.Printf("%v: PutAppend with (%v, %v, %v) success", i, op, key, value)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
