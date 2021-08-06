package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"log"
	"math/rand"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role			string			// leader follower candidate
	currentTerm		int				// the current term of this server
	votedFor		int				// -1 to indicate null
	heartbeat		chan int		// channel for heartbeat message 
}

// switch to next term, start as candidate
func (rf *Raft) nextTerm() {
	rf.mu.Lock()
	rf.role = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	go rf.duringElection()
}

// bump to a peer's term, start as follower
func (rf *Raft) bumpTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.role = "follower"
	rf.votedFor = -1
	rf.mu.Unlock()

	go rf.waitForElection()
}

func (rf *Raft) duringElection() {
	if rf.role != "candidate" {
		log.Panicf("%v during election", rf.role)
	}

	sleepAmount := time.Duration(rand.Intn(200) + 300) * time.Millisecond
	timeout := make(chan bool)
	isTimeout := false
	go func() {
		<-time.After(sleepAmount)
		timeout <- true
		isTimeout = true
	}()

	grantedCount := 1
	finished := make(chan bool)
	go func() {
		// request vote from every peer
		for index := range rf.peers {
			if index == rf.me || isTimeout {
				continue
			}
			args := &RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateID: rf.me,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(index, args, reply)
			if !ok {
				//log.Printf("sendRequestVote to %v from %v failed", index, rf.me)
				continue
			}
			if reply.Term > rf.currentTerm {
				finished <- false
				rf.bumpTerm(reply.Term)
				return
			}
			if reply.VoteGranted {
				grantedCount += 1
			}
		}
		finished <- true
	}()

	select {
	case <-timeout:
	case ok := <-finished:
		if !ok {
			return
		}
	}

	log.Printf("peer %v role %v term %v get %v vote", rf.me, rf.role, rf.currentTerm, grantedCount)
	if grantedCount * 2 > len(rf.peers) {
		rf.mu.Lock()
		rf.role = "leader"
		rf.mu.Unlock()

		go rf.underLeading()
	} else {
		if !isTimeout {
			<-timeout
		}
		rf.nextTerm()
	}
}

func (rf *Raft) underLeading() {
	sleepAmount := time.Duration(100) * time.Millisecond
	for {
		timeout := make(chan bool)
		go func() {
			<-time.After(sleepAmount)
			timeout <- true
		}()
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(index, args, reply)
			if !ok {
				//log.Printf("send AppendEntries to %v from %v failed", index, rf.me)
			}
		}
		select {
		case <-timeout:
		case peerTerm := <-rf.heartbeat:
			if peerTerm > rf.currentTerm {
				rf.bumpTerm(peerTerm)
				return
			}
		}
	}
}

func (rf *Raft) waitForElection() {
	if rf.role != "follower" {
		log.Panicf("%v waiting for election", rf.role)
	}

	sleepAmount := time.Duration(rand.Intn(200) + 300) * time.Millisecond
	timeout := make(chan bool)
	go func() {
		<-time.After(sleepAmount)
		timeout <- true
	}()

	log.Printf("peer %v role %v term %v start waiting %v ms for election", rf.me, rf.role, rf.currentTerm, sleepAmount)
	keepSleep := true
	for keepSleep {
		select {
		case <-timeout:
			keepSleep = false
			rf.nextTerm()
		case peerTerm := <-rf.heartbeat:
			// useless heartbeat
			if peerTerm < rf.currentTerm {
				continue
			}
			// heartbeat from leader
			if peerTerm == rf.currentTerm {
				log.Printf("peer %v role %v term %v continue", rf.me, rf.role, rf.currentTerm)
				go rf.waitForElection()
				return
			}
			// bump term
			keepSleep = false
			rf.bumpTerm(peerTerm)
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role == "leader"
}

//{{{
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}// }}}

//{{{
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}// }}}

type RequestVoteArgs struct {
	Term			int		// term of the candidate
	CandidateID		int		// id of the candidate	
}

type RequestVoteReply struct {
	Term			int		// term of the peer
	VoteGranted		bool	// is granted?
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// if peer's term is old, rejected
	if args.Term < rf.currentTerm {
		return
	}
	// if already voted and not for this peer, rejected
	if args.CandidateID != rf.votedFor && rf.votedFor != -1 {
		return
	}

	// otherwise we can simply granted for 2A
	reply.VoteGranted = true

	rf.mu.Lock()
	rf.votedFor = args.CandidateID
	rf.mu.Unlock()
}

//{{{
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//}}}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartbeat <- args.Term
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//{{{
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}// }}}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	log.Printf("New Round")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeat = make(chan int)	

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// all node start with follower, term = 0
	rf.role = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1

	go rf.waitForElection()

	return rf
}
