// TODO 
// Reconstruct the structure
// one main background 
//	for leader:			main task: send AppendEntries RPC to every peer
//									- ready for bump term	-> become a candidate
//  for follower:		main task: election count down
//									- ready for bump term to newer term -> become follower in new term
//									- ready for heart beat from same term -> continue as follower
//	for candidate:	main task: send RequestVote RPC to every peer
//									- ready for bump term to newer term -> become follower in new term
//									- ready for heart beat from same term -> become a follower

package raft

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

// utils: min max setupSleep, latestLogIndexAndTerm, persist, GetState, readPersist {{{
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func setupSleep(amount time.Duration) chan bool {
	timeout := make(chan bool)
	go func() {
		time.Sleep(amount)
		close(timeout)
	}()
	return timeout
}

func (rf *Raft) latestLogIndexAndTerm() (int, int) {
	index := len(rf.log) - 1
	term := -1
	if index >= 0 {
		term = rf.log[index].Term
	}
	return index, term
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role == "leader"
}

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
}
// }}}

type ApplyMsg struct {// {{{
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
	heartbeat			chan bool				// channel for heartbeat message 
	log						[]Log
	commitIndex		int
	lastApplied		int
	commitCond		*sync.Cond

	// leader only
	nextIndex			[]int
	matchIndex		[]int
}// }}}

// switch to next term, start as candidate
func (rf *Raft) nextTerm() {// {{{
	rf.mu.Lock()
	rf.role = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	log.Printf("#%v: %v $%v state updated", rf.currentTerm, rf.role, rf.me)
	rf.mu.Unlock()

	rf.duringElection()
}// }}}

// candidate phase
func (rf *Raft) duringElection() {// {{{
	// sanity check
	if rf.role != "candidate" {
		log.Panicf("%v during election", rf.role)
	}

	rf.mu.Lock()
	// setup token
	token := Token {
		term: rf.currentTerm,
		role: rf.role,
	}
	// prepare args
	args := RequestVoteArgs {
		Term: rf.currentTerm,
		CandidateID: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm: -1,
	}
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	rf.mu.Unlock()

	// setup sleep
	timeout := setupSleep(time.Duration(rand.Intn(200) + 300) * time.Millisecond)

	// request vote from every peer
	go func() {
		voteCollected := 1
		for index := range rf.peers {
			// no need for self
			if index == rf.me {
				continue
			}
			go func (localIndex int) {
				reply := &RequestVoteReply{}
				for !rf.killed() {
					// fire the request
					ok := rf.sendRequestVote(localIndex, &args, reply)
					// check token
					if rf.role != token.role || rf.currentTerm != token.term {
						return
					}
					// network failed
					if !ok {
						return
					}
					// vote granted, done
					if reply.VoteGranted {
						// increase counter
						rf.mu.Lock()
						voteCollected += 1
						if voteCollected * 2 > len(rf.peers) && rf.role == token.role && rf.currentTerm == token.term {
							rf.mu.Unlock()
							rf.underLeading()
							return
						}
						rf.mu.Unlock()
						return
					}
					// sleep and retry
					time.Sleep(time.Millisecond * 50)
				}
			}(index)
		}
	}()

	<-timeout
	// check token
	if rf.role != token.role || rf.currentTerm != token.term {
		return
	}
	rf.nextTerm()
}// }}}

// leader phase
func (rf *Raft) underLeading() {// {{{
	rf.mu.Lock()
	// set role to leader
	rf.role = "leader"
	log.Printf("#%v: %v $%v state updated", rf.currentTerm, rf.role, rf.me)
	// init nextIndex and matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)
	// capture current state
	token := Token {
		term: rf.currentTerm,
		role: rf.role,
	}
	rf.mu.Unlock()

	for !rf.killed() {
		// check token
		if rf.role != token.role || rf.currentTerm != token.term {
			return
		}
		// setup sleep 
		timeout := setupSleep(time.Duration(100) * time.Millisecond)

		// prepare args for everyone
		rf.mu.Lock()
		pargs := make([]AppendEntriesArgs, len(rf.peers))
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			pargs[index] = AppendEntriesArgs {
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.nextIndex[index] - 1,
				PrevLogTerm: -1, // default value
				LeaderCommit: rf.commitIndex,
			}
			if pargs[index].PrevLogIndex >= 0 {
				pargs[index].PrevLogTerm = rf.log[pargs[index].PrevLogIndex].Term
			}
			pargs[index].Entries = rf.log[rf.nextIndex[index] : ]
			rf.nextIndex[index] = len(rf.log)
		}
		log.Printf("#%v: %v $%v updated nextIndex = %v", rf.currentTerm, rf.role, rf.me, rf.nextIndex)
		rf.mu.Unlock()

		// heartbeat to every peer
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(localIndex int) {
				reply := &AppendEntriesReply{}
				log.Printf("#%v: %v $%v AppendEntriesArgs = %+v", rf.currentTerm, rf.role, rf.me, pargs[localIndex])
				ok := rf.sendAppendEntries(localIndex, &pargs[localIndex], reply)
				// check token
				if token.role != rf.role || token.term != rf.currentTerm {
					return
				} 
				// network failed
				if !ok {
					return
				}
				// got reply
				rf.mu.Lock()
				log.Printf("#%v: %v $%v AppendEntriesReply = %+v", rf.currentTerm, rf.role, rf.me, reply)
				if (reply.Success) {
					rf.matchIndex[localIndex] = pargs[localIndex].PrevLogIndex + len(pargs[localIndex].Entries)
					log.Printf("#%v: %v $%v updated matchIndex = %v", rf.currentTerm, rf.role, rf.me, rf.matchIndex)
					rf.commitCond.Broadcast()
				} else {
					rf.nextIndex[localIndex] = max(pargs[localIndex].PrevLogIndex, 0)
					log.Printf("#%v: %v $%v updated nextIndex = %v", rf.currentTerm, rf.role, rf.me, rf.nextIndex)
				}
				rf.mu.Unlock()
			}(index)
		}
		<-timeout
		// check token
		if token.role != rf.role || token.term != rf.currentTerm {
			return
		} 
	}
}// }}}

// follower phase
func (rf *Raft) waitForElection() {// {{{
	// sanity check
	if rf.role != "follower" {
		log.Panicf("%v waiting for election", rf.role)
	}

	// capture term and role
	rf.mu.Lock()
	token := Token {
		term: rf.currentTerm,
		role: rf.role,
	}
	rf.mu.Unlock()

	// set up sleep goroutine
	timeout := setupSleep(time.Duration(rand.Intn(200) + 300) * time.Millisecond)

	// waiting for [timeout, heartbeat]
	select {
	case <-timeout:
		// check token
		if rf.role != token.role || rf.currentTerm != token.term {
			return
		}
		// move to next term
		rf.nextTerm()
		return
	case <-rf.heartbeat:
		// check token
		if rf.role != token.role || rf.currentTerm != token.term {
			return
		}
		// continue wating
		rf.waitForElection()
		return
	}
}// }}}

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
		rf.currentTerm = args.Term
		rf.role = "follower"
		rf.votedFor = -1
		rf.persist()
		go rf.waitForElection()
		rf.mu.Unlock()
		return
	}
	// if already voted and not for this peer, rejected
	if args.CandidateID != rf.votedFor && rf.votedFor != -1 {
		rf.mu.Unlock()
		return
	}
	myLogIndex, myLogTerm := rf.latestLogIndexAndTerm()
	// if my log is newer than candiate's log, rejected 
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	// old request, useless
	if args.Term < rf.currentTerm {
		return
	}
	// leader in same term 
	if args.Term == rf.currentTerm {
		if rf.role == "candiate" {
			rf.role = "follower"
			log.Printf("#%v: %v $%v state updated", rf.currentTerm, rf.role, rf.me)
		} else if rf.role == "follower" {
			rf.heartbeat <- true
		}
	}
	// bigger term
	if args.Term > rf.currentTerm {
		rf.role = "follower"
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
		log.Printf("#%v: %v $%v state updated", rf.currentTerm, rf.role, rf.me)
		go rf.waitForElection()
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
	rf.log = rf.log[:args.PrevLogIndex + 1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	candidateCommitIndex := min(args.LeaderCommit, len(rf.log) - 1)
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
	rf.nextIndex[rf.me] = len(rf.log)
	log.Printf("#%v: %v $%v updated matchIndex = %v with new command %v", rf.currentTerm, rf.role, rf.me, rf.matchIndex, index)
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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeat = make(chan bool)	
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
