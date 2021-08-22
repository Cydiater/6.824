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
	"sort"
	"bytes"

	"../labrpc"
	"../labgob"
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

type BumpMsg struct {
	Term		int
}

type Token struct {
	term int
	role string
}

type Log struct {
	Term int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh		chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role					string					// leader follower candidate
	currentTerm		int							// the current term of this server
	votedFor			int							// -1 to indicate null
	heartbeat			chan int				// channel for heartbeat message 
	bumpchan			chan BumpMsg		// pass msg to bump the term
	log						[]Log
	commitIndex		int
	lastApplied		int
	commitCond		*sync.Cond

	// leader only
	nextIndex			[]int
	matchIndex		[]int
}

// switch to next term, start as candidate
func (rf *Raft) nextTerm() {// {{{
	rf.mu.Lock()
	rf.role = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	go rf.duringElection()
}// }}}

// bump to a peer's term, start as follower
func (rf *Raft) bumpTerm(term int) {// {{{
	rf.mu.Lock()
	if term < rf.currentTerm {
		log.Panicf("peer %v bump from %v to %v", rf.me, rf.currentTerm, term)
	}
	rf.currentTerm = term
	rf.role = "follower"
	rf.votedFor = -1
	rf.persist()
	rf.mu.Unlock()

	rf.waitForElection()
}// }}}

func (rf *Raft) duringElection() {// {{{
	if rf.role != "candidate" {
		log.Panicf("%v during election", rf.role)
	}
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	// setup token
	token := Token {
		term: rf.currentTerm,
		role: rf.role,
	}
	rf.mu.Unlock()

	// setup sleep
	sleepAmount := time.Duration(rand.Intn(200) + 300) * time.Millisecond
	timeout := make(chan bool)
	go func() {
		time.Sleep(sleepAmount)
		close(timeout)
	}()

	grantedCount := 1
	go func() {
		log.Printf("#%v: %v %v working in election expire in %v", token.term, rf.role, rf.me, sleepAmount)
		replychan := make(chan RequestVoteReply)
		var wg sync.WaitGroup
		for index := range rf.peers {
			// no need for self
			if index == rf.me {
				continue
			}
			// check token
			if rf.role != token.role || rf.currentTerm != token.term {
				log.Printf("token %+v invalid for %v", token, rf.me)
				return
			}
			// check killed
			if rf.killed() {
				return
			}
			wg.Add(1)
			go func (localIndex int) {
				rf.mu.Lock()
				// construct request and reply
				args := &RequestVoteArgs{
					Term: token.term,
					CandidateID: rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm: -1,
				}
				if args.LastLogIndex >= 0 {
					args.LastLogTerm = rf.log[args.LastLogIndex].Term
				}
				reply := &RequestVoteReply{}
				rf.mu.Unlock()
				for !rf.killed() {
					// check token
					if rf.role != token.role || rf.currentTerm != token.term {
						return
					}
					// fire the request
					log.Printf("#%v: %v RequestVote send to %v, args = %+v", token.term, rf.me, localIndex, args)
					ok := rf.sendRequestVote(localIndex, args, reply)
					// check token
					if rf.role != token.role || rf.currentTerm != token.term {
						return
					}
					log.Printf("#%v: %v RequestVote send to %v finished, ok = %v, reply = %+v", token.term, rf.me, localIndex, ok, reply)
					// vote granted, done
					if ok && reply.VoteGranted {
						replychan <- *reply
						wg.Done()
						return
					}
					// sleep and retry
					time.Sleep(time.Millisecond * 10)
				}
			}(index)
		}
		go func() {
			wg.Wait()
			close(replychan)
		}()
		for reply := range replychan {
			if reply.Term > token.term {
				// peer in new term, abort
				rf.bumpTerm(reply.Term)
				return
			}
			// vote granted
			if reply.VoteGranted {
				grantedCount += 1
			}

			// check vote count
			if grantedCount * 2 > len(rf.peers) {
				// win this election
				rf.mu.Lock()
				rf.role = "leader"
				rf.mu.Unlock()

				rf.underLeading()
				return
			}
		}
	}()

	continueWaiting:
	select {
	case <-timeout:
	case msg := <-rf.bumpchan:
		// bump to future term, abort current election
		rf.bumpTerm(msg.Term)
		return
	case peerTerm := <-rf.heartbeat:
		// heartbeat
		rf.mu.Lock()
		if peerTerm > rf.currentTerm {
			rf.mu.Unlock()
			rf.bumpTerm(peerTerm)
			return
		}
		rf.mu.Unlock()
		goto continueWaiting
	}

	// timeout, no winner, move to next term
	if rf.role == token.role && rf.currentTerm == token.term {
		rf.nextTerm()
	}
}// }}}

func (rf *Raft) underLeading() {// {{{
	log.Printf("#%v: %v %v under leading", rf.currentTerm, rf.role, rf.me)
	rf.mu.Lock()
	// init nextIndex and matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.mu.Unlock()
	for !rf.killed() {
		// setup sleep 
		timeout := make(chan bool)
		sleepAmount := time.Duration(100) * time.Millisecond
		go func() {
			time.Sleep(sleepAmount)
			close(timeout)
		}()

		rf.mu.Lock()
		token := Token {
			term: rf.currentTerm,
			role: rf.role,
		}
		var wg sync.WaitGroup
		// send heartbeat to every peer
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			wg.Add(1)
			go func(localIndex int) {
				preLogIndex := rf.nextIndex[localIndex] - 1
				args, nextIndex := rf.genAppendEntriesArgs(preLogIndex)
				log.Printf("#%v: %v heartbeat to %v ready to send, args = %+v, nextIndex = %v", rf.currentTerm, rf.me, localIndex, args, rf.nextIndex)
				rf.nextIndex[localIndex] = nextIndex + 1
				wg.Done()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(localIndex, args, reply)
				// check token
				rf.mu.Lock()
				if token.role != rf.role || token.term != rf.currentTerm {
					rf.mu.Unlock()
					return
				} 
				if !ok {
					log.Printf("#%v: %v heartbeat to %v failed", rf.currentTerm, rf.me, localIndex)
				} else if (reply.Success) {
					log.Printf("#%v: %v heartbeat to %v success, reply = %+v", rf.currentTerm, rf.me, localIndex, reply)
					rf.matchIndex[localIndex] = nextIndex
					rf.commitCond.Broadcast()
					log.Printf("#%v: %v %v updated matchIndex = %v", rf.currentTerm, rf.role, rf.me, rf.matchIndex)
				} else {
					log.Printf("#%v: %v heartbeat to %v failed, reply = %+v", rf.currentTerm, rf.me, localIndex, reply)
					rf.nextIndex[localIndex] = preLogIndex
					log.Printf("#%v: %v %v updated nextIndex = %v", rf.currentTerm, rf.role, rf.me, rf.nextIndex)
				}
				rf.mu.Unlock()
			}(index)
		}
		wg.Wait()
		rf.mu.Unlock()
		select {
		case <-timeout:
		case peerTerm := <-rf.heartbeat:
			rf.mu.Lock()
			if peerTerm > rf.currentTerm {
				rf.mu.Unlock()
				log.Printf("#%v: %v %v bump to new term %v", rf.currentTerm, rf.role, rf.me, peerTerm)
				rf.bumpTerm(peerTerm)
				return
			}
			rf.mu.Unlock()
		case msg := <-rf.bumpchan:
			log.Printf("#%v: %v %v bump to new term %v", rf.currentTerm, rf.role, rf.me, msg.Term)
			rf.bumpTerm(msg.Term)
			return
		}
	}
}// }}}

func (rf *Raft) waitForElection() {// {{{
	if rf.role != "follower" {
		log.Panicf("%v waiting for election", rf.role)
	}

	// set up sleep goroutine
	sleepAmount := time.Duration(rand.Intn(200) + 300) * time.Millisecond
	timeout := make(chan bool)
	go func() {
		time.Sleep(sleepAmount)
		close(timeout)
	}()

	log.Printf("#%v: %v %v waiting for election, expire in %v", rf.currentTerm, rf.role, rf.me, sleepAmount)
	keepSleep := true
	for keepSleep {
		if rf.killed() {
			return
		}
		select {
		case <-timeout:
			// check killed
			if rf.killed() {
				return
			}
			log.Printf("#%v: %v %v waiting for election: timeout", rf.currentTerm, rf.role, rf.me)
			keepSleep = false
			rf.nextTerm()
			return
		case peerTerm := <-rf.heartbeat:
			rf.mu.Lock()
			//log.Printf("#%v: %v %v waiting for election: heartbeat", rf.currentTerm, rf.role, rf.me)
			// useless heartbeat
			if peerTerm < rf.currentTerm {
				rf.mu.Unlock()
				continue
			}
			// heartbeat from leader
			if peerTerm == rf.currentTerm {
				//log.Printf("peer %v role %v term %v continue", rf.me, rf.role, rf.currentTerm)
				rf.mu.Unlock()
				rf.waitForElection()
				return
			}
			rf.mu.Unlock()
			// bump term
			keepSleep = false
			rf.bumpTerm(peerTerm)
		case msg := <-rf.bumpchan:
			log.Printf("#%v: %v %v waiting for election: bump to new term %v", rf.currentTerm, rf.role, rf.me, msg.Term)
			rf.bumpTerm(msg.Term)
			return
		}
	}
}// }}}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role == "leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//{{{
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
		 d.Decode(&logs) != nil {
			 log.Fatalf("decode failed")
	 } else {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
		 rf.log = logs
	}
}//

type RequestVoteArgs struct {
	Term					int		// term of the candidate
	CandidateID		int		// id of the candidate	
	LastLogIndex	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	Term			int		// term of the peer
	VoteGranted		bool	// is granted?
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {// {{{
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// if peer's term is older, rejected
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	// if peer's term is newer, bump to new term and let leader retry
	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.bumpchan <- BumpMsg{ Term: args.Term }
		return
	}
	// if already voted and not for this peer, rejected
	if args.CandidateID != rf.votedFor && rf.votedFor != -1 {
		rf.mu.Unlock()
		return
	}
	myLogIndex := len(rf.log) - 1
	myLogTerm := -1
	if myLogIndex >= 0 {
		myLogTerm = rf.log[myLogIndex].Term
	}
	// if my log is newer than candiate's log, rejected 
	log.Printf("#%v: %v %v log = %+v", rf.currentTerm, rf.role, rf.me, rf.log)
	if myLogTerm > args.LastLogTerm || (myLogTerm == args.LastLogTerm && myLogIndex > args.LastLogIndex) {
		rf.mu.Unlock()
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.persist()
	rf.mu.Unlock()
}// }}}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) (*AppendEntriesArgs, int) {
	args := &AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: -1,
		Entries: rf.log[prevLogIndex + 1 : ],
		LeaderCommit: rf.commitIndex,
	}
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	nextIndex := len(rf.log) - 1
	return args, nextIndex
}

type AppendEntriesArgs struct {
	Term					int
	LeaderId			int
	Entries				[]Log
	PrevLogIndex	int
	PrevLogTerm		int
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term					int
	Success				bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {// {{{
	rf.heartbeat <- args.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	// old request, useless
	if args.Term < rf.currentTerm {
		return
	}
	// request from leader, convert to follower
	if args.Term == rf.currentTerm {
		rf.role = "follower"
	}
	// don't have this index
	if args.PrevLogIndex >= len(rf.log) {
		return
	}
	// prev log index not match prev term
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[:args.PrevLogIndex]
		rf.persist()
		return
	}

	if len(args.Entries) > 0 {
		log.Printf("#%v: %v %v appended %v logs", rf.currentTerm, rf.role, rf.me, len(args.Entries))
	}

	rf.log = rf.log[:args.PrevLogIndex + 1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	candidateCommitIndex := args.LeaderCommit
	if len(rf.log) - 1 < candidateCommitIndex {
		candidateCommitIndex = len(rf.log) - 1
	}
	if  candidateCommitIndex > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= candidateCommitIndex; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command: rf.log[i].Command,
				CommandIndex: i + 1,
			}
			log.Printf("%v %v applied commit %v", rf.role, rf.me, i)
		}
		rf.commitIndex = candidateCommitIndex
	}
	reply.Success = true;
}// }}}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) { // (index, term, isLeader)
	// check for leading
	if rf.role != "leader" {
		return -1, -1, false;
	}
	// append to local log
	rf.mu.Lock();
	rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command})
	rf.persist()
	index := len(rf.log) - 1
	term := rf.currentTerm
	rf.matchIndex[rf.me] = len(rf.log) - 1
	log.Printf("#%v: %v %v updated matchIndex = %v with new command %v", rf.currentTerm, rf.role, rf.me, rf.matchIndex, index)
	rf.commitCond.Broadcast()
	rf.mu.Unlock()
	return index + 1, term, true
}

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

// just for leader, check for new commit index
func (rf *Raft) moniterCommit() {
	rf.commitCond.L.Lock()
	for !rf.killed() {
		// check for leader
		if rf.role != "leader" {
			rf.commitCond.Wait()
			continue
		}
		rf.mu.Lock()
		buf := make([]int, len(rf.peers))
		copy(buf[:], rf.matchIndex)
		sort.Ints(buf)
		majorityIndex := (len(rf.peers) + 1) / 2 - 1
		candidateIndex := buf[majorityIndex]
		if candidateIndex > rf.commitIndex {
			for i := rf.commitIndex + 1; i <= candidateIndex; i++ {
				rf.applyCh <- ApplyMsg {
					CommandValid: true,
					Command: rf.log[i].Command,
					CommandIndex: i + 1,
				}
				log.Printf("%v %v applied commit %v", rf.role, rf.me, i)
			}
			rf.commitIndex = candidateIndex
		}
		rf.mu.Unlock()
		rf.commitCond.Wait()
	}
	rf.commitCond.L.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeat = make(chan int)	
	rf.bumpchan = make(chan BumpMsg)
	rf.applyCh = applyCh

	// all node start with follower, term = 0
	rf.role = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.commitCond = sync.NewCond(new(sync.Mutex))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}

	go rf.waitForElection()
	go rf.moniterCommit()

	return rf
}
