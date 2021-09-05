package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key					string
	Value				string
	Op					string // "Put" or "Append"
	ClientID		string
	OpID				string
	SendTo			int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key				string
	ClientID	string
	OpID			string
	SendTo		int
}

type GetReply struct {
	Err   Err
	Value string
}
