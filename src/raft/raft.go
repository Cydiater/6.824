package raft

import (
	"sync"
	"sync/atomic"
	"math/rand"
	"time"
	"sort"
	"bytes"
	"log"

	"../labrpc"
	"../labgob"
)

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

func (rf *Raft) marshallState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logOffset)
	return w.Bytes()
}

func (rf *Raft) persist() {
	data := rf.marshallState()
	rf.persister.SaveRaftState(data)
	go func() {
		rf.applyMu.Lock()
		rf.applyCh <- ApplyMsg {
			CommandValid: false,
			Command: rf.persister.RaftStateSize(),
			CommandIndex: rf.commitIndex + 1,
			CommandType: "ReportStateSize",
		}
		rf.applyMu.Unlock()
	}()
}

func (rf *Raft) UpdateLogOffset(commandIndex int, b []byte) {
	rf.mu.Lock()
	newLogOffset := commandIndex
	if newLogOffset < rf.logOffset {
		log.Panicf("new log offset %v is smaller than current log offset %v", newLogOffset, rf.logOffset)
	}
	if newLogOffset - rf.logOffset > len(rf.log) {
		log.Panicf("don't have enough log to update log offset from %v to %v", rf.logOffset, newLogOffset)
	}
	rf.persister.SaveStateAndSnapshot(rf.marshallState(), b)
	// don't touch new log
	rf.log = rf.log[newLogOffset - rf.logOffset : ]
	rf.logOffset = newLogOffset
	rf.mu.Unlock()
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	role := rf.role
	rf.mu.Unlock()
	return currentTerm, role == "leader"
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
	var logOffset	int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
		 d.Decode(&logs) != nil ||
		 d.Decode(&logOffset) != nil {
			 //log.Fatalf("decode failed")
	 } else {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
		 rf.log = logs
		 rf.logOffset = logOffset
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	/* Available Types: Apply ReportStateSize InstallSnapshot */
	CommandType	 string
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
	applyMu		sync.Mutex

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role					string					// leader follower candidate
	currentTerm		int							// the current term of this server
	votedFor			int							// -1 to indicate null
	heartbeat			chan bool				// channel for heartbeat message 
	log						[]Log
	logOffset			int
	commitIndex		int
	applyIndex		int
	commitCond		*sync.Cond

	// leader only
	nextIndex			[]int
	matchIndex		[]int
}

// switch to next term, start as candidate
func (rf *Raft) nextTerm() { 
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	rf.role = "candidate"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	log.Printf("#%v: %v $%v state updated, bump by last term", rf.currentTerm, rf.role, rf.me)
	rf.mu.Unlock()

	rf.duringElection()
} 

// candidate phase
func (rf *Raft) duringElection() {
	// sanity check
	if rf.role != "candidate" {
		//log.Panicf("%v during election", rf.role)
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
		LastLogIndex: rf.real_log_size() - 1,
		LastLogTerm: -1,
	}
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.log_at(args.LastLogIndex).Term
	}
	rf.mu.Unlock()

	// setup sleep
	timeout := setupSleep(time.Duration(300 + rand.Intn(200)) * time.Millisecond)

	// request vote from every peer
	var voteCollected int64
	voteCollected = 1
	voteStatus := make([]string, len(rf.peers))
	go func() {
		for index := range rf.peers {
			// no need for self
			if index == rf.me {
				voteStatus[index] = "ok"
				continue
			}
			voteStatus[index] = "send"
			go func (localIndex int) {
				for !rf.killed() {
					reply := &RequestVoteReply{}
					// fire the request
					ok := rf.sendRequestVote(localIndex, &args, reply)
					// check token
					rf.mu.Lock()
					if rf.role != token.role || rf.currentTerm != token.term {
						rf.mu.Unlock()
						voteStatus[localIndex] = "token-failed"
						return
					}
					rf.mu.Unlock()
					// network failed
					if !ok {
						voteStatus[localIndex] = "network-failed"
						continue
					}
					// vote granted, done
					if reply.VoteGranted {
						// increase counter
						rf.mu.Lock()
						atomic.AddInt64(&voteCollected, 1)
						voteStatus[localIndex] = "ok"
						if int(voteCollected * 2) > len(rf.peers) && rf.role == token.role && rf.currentTerm == token.term {
							rf.mu.Unlock()
							rf.underLeading()
							return
						}
						rf.mu.Unlock()
						return
					}
					voteStatus[localIndex] = "rejected"
					// sleep and retry
					time.Sleep(time.Millisecond * 50)
				}
			}(index)
		}
	}()

	<-timeout
	// check token
	rf.mu.Lock()
	if rf.role != token.role || rf.currentTerm != token.term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	//log.Printf("#%v: %v $%v only got %v votes", rf.currentTerm, rf.role, rf.me, voteCollected)
	rf.nextTerm()
}//

// leader phase
func (rf *Raft) underLeading() {
	rf.mu.Lock()
	// set role to leader
	rf.role = "leader"
	log.Printf("#%v: %v $%v state updated", rf.currentTerm, rf.role, rf.me)
	// init nextIndex and matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.real_log_size()
		rf.matchIndex[i] = -1
	}
	rf.matchIndex[rf.me] = rf.real_log_size() - 1
	rf.nextIndex[rf.me] = rf.real_log_size()
	// capture current state
	token := Token {
		term: rf.currentTerm,
		role: rf.role,
	}
	rf.mu.Unlock()

	for !rf.killed() {
		// check token
		rf.mu.Lock()
		if rf.role != token.role || rf.currentTerm != token.term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// setup sleep 
		timeout := setupSleep(time.Duration(50) * time.Millisecond)

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
				pargs[index].PrevLogTerm = rf.log_at(pargs[index].PrevLogIndex).Term
			}
			pargs[index].Entries = rf.slice_log_suffix(rf.nextIndex[index])
			rf.nextIndex[index] = rf.real_log_size()
		}
		//log.Printf("#%v: %v $%v updated nextIndex = %v for everyone", rf.currentTerm, rf.role, rf.me, rf.nextIndex)
		rf.mu.Unlock()

		// heartbeat to every peer
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(localIndex int) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(localIndex, &pargs[localIndex], reply)
				// check token
				rf.mu.Lock()
				if token.role != rf.role || token.term != rf.currentTerm {
					rf.mu.Unlock()
					return
				} 
				rf.mu.Unlock()
				// network failed
				if !ok {
					return
				}
				// got reply
				rf.mu.Lock()
				if (reply.Success) {
					newMatchIndex := pargs[localIndex].PrevLogIndex + len(pargs[localIndex].Entries)
					if newMatchIndex > rf.matchIndex[localIndex] {
						rf.matchIndex[localIndex] = newMatchIndex
						//log.Printf("#%v: %v $%v updated matchIndex = %v for %v, append %v logs", rf.currentTerm, rf.role, rf.me, rf.matchIndex, localIndex, len(pargs[localIndex].Entries))
						rf.commitCond.Broadcast()
					}
				} else {
					rf.nextIndex[localIndex] = max(pargs[localIndex].PrevLogIndex, 0)
					if reply.ConflictTerm != -1 {
						st := -1
						for i := range rf.log {
							if rf.log_at(i).Term == reply.ConflictTerm {
								st = i
							}
						}
						if st == -1 {
							rf.nextIndex[localIndex] = reply.ConflictIndex
						} else {
							rf.nextIndex[localIndex] = st + 1
						}
					} else if reply.ConflictIndex != -1 {
						rf.nextIndex[localIndex] = reply.ConflictIndex
					}
					//log.Printf("#%v: %v $%v updated nextIndex = %v for %v", rf.currentTerm, rf.role, rf.me, rf.nextIndex, localIndex)
				}
				rf.mu.Unlock()
			}(index)
		}
		<-timeout
		// check token
		rf.mu.Lock()
		if token.role != rf.role || token.term != rf.currentTerm {
			rf.mu.Unlock()
			return
		} 
		rf.mu.Unlock()
	}
}

// follower phase
func (rf *Raft) waitForElection(token Token) {
	// set up sleep goroutine
	timeout := setupSleep(time.Duration(rand.Intn(300) + 100) * time.Millisecond)

	// waiting for [timeout, heartbeat]
	select {
	case <-timeout:
		// check token
		rf.mu.Lock()
		if rf.role != token.role || rf.currentTerm != token.term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// move to next term
		rf.nextTerm()
		return
	case <-rf.heartbeat:
		// check token
		rf.mu.Lock()
		if rf.role != token.role || rf.currentTerm != token.term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// continue wating
		rf.waitForElection(token)
		return
	}
}

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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// if peer's term is older, rejected
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	// if peer's term is newer, bump to new term 
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = "follower"
		rf.votedFor = -1
		rf.persist()
		log.Printf("#%v: %v $%v state updated, bump by RequestVote from %v", rf.currentTerm, rf.role, rf.me, args.CandidateID)
		go rf.waitForElection(Token {rf.currentTerm, rf.role})
	}
	// if already voted and not for this peer, rejected
	if args.CandidateID != rf.votedFor && rf.votedFor != -1 {
		rf.mu.Unlock()
		return
	}
	myLogIndex, myLogTerm := rf.latestLogIndexAndTerm()
	// if my log is newer than candidate's log, rejected 
	if myLogTerm > args.LastLogTerm || (myLogTerm == args.LastLogTerm && myLogIndex > args.LastLogIndex) {
		rf.mu.Unlock()
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term							int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Body							[]byte
}

type InstallSnapshotReply struct {
	Term					int
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
	ConflictIndex	int
	ConflictTerm	int
}

func (rf *Raft) log_at(idx int) Log {
	if idx < rf.logOffset {
		log.Panicf("access log at %v, but logOffset is %v", idx, rf.logOffset)
	}
	return rf.log[idx - rf.logOffset]
}

func (rf *Raft) real_log_size() int {
	return len(rf.log) + rf.logOffset
}

func (rf *Raft) write_log_start_at(idx int, entires []Log) {
	if idx < rf.logOffset {
		log.Panicf("access log at %v, but logOffset is %v", idx, rf.logOffset)
	}
	idx -= rf.logOffset
	rf.log = rf.log[ : idx]
	rf.log = append(rf.log, entires...)
}

func (rf *Raft) slice_log_suffix(idx int) []Log {
	if idx < rf.logOffset {
		log.Panicf("access log at %v, but logOffset is %v", idx, rf.logOffset)
	}
	idx -= rf.logOffset
	return rf.log[idx : ]
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	// old request, useless
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	// leader in same term 
	if args.Term == rf.currentTerm {
		if rf.role == "candidate" {
			rf.role = "follower"
			log.Printf("#%v: %v $%v state updated", rf.currentTerm, rf.role, rf.me)
			go rf.waitForElection(Token {rf.currentTerm, rf.role})
		} else if rf.role == "follower" {
			// not block
			go func() {
				rf.heartbeat <- true
			}()
		}
	}
	// bigger term
	if args.Term > rf.currentTerm {
		rf.role = "follower"
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
		log.Printf("#%v: %v $%v state updated", rf.currentTerm, rf.role, rf.me)
		go rf.waitForElection(Token {rf.currentTerm, rf.role})
	}
	// don't have this index
	if args.PrevLogIndex >= rf.real_log_size() {
		reply.ConflictIndex = rf.real_log_size()
		rf.mu.Unlock()
		return
	}
	// prev log index not match prev term
	if args.PrevLogIndex >= 0 && rf.log_at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log_at(args.PrevLogIndex).Term
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex > 0 && rf.log_at(reply.ConflictIndex - 1).Term == rf.log_at(reply.ConflictIndex).Term {
			reply.ConflictIndex -= 1
		}
		rf.mu.Unlock()
		return
	}
	// short the entries
	for len(args.Entries) > 0 && args.PrevLogIndex < rf.real_log_size() - 1 {
		if args.PrevLogIndex != -1 && rf.log_at(args.PrevLogIndex + 1).Term != args.Entries[0].Term {
			break
		}
		args.PrevLogIndex += 1
		args.Entries = args.Entries[1 : ]
	}
	if len(args.Entries) > 0 {
		rf.write_log_start_at(args.PrevLogIndex + 1, args.Entries)
		rf.persist()
	}
	newMsgs := []ApplyMsg{}
	candidateCommitIndex := min(args.LeaderCommit, rf.real_log_size() - 1)
	if  candidateCommitIndex > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= candidateCommitIndex; i++ {
			newMsgs = append(newMsgs, ApplyMsg{
				CommandValid: true,
				Command: rf.log_at(i).Command,
				CommandIndex: i + 1,
				CommandType: "Apply",
			})
		}
		rf.commitIndex = candidateCommitIndex
	}
	reply.Success = true;
	rf.mu.Unlock()
	go func() {
		for {
			if len(newMsgs) == 0 {
				return
			}
			rf.applyMu.Lock()
			if rf.applyIndex + 1 == newMsgs[0].CommandIndex {
				break
			}
			rf.applyMu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		for _, msg := range newMsgs {
			rf.applyCh <- msg
			rf.applyIndex = msg.CommandIndex
		}
		rf.applyMu.Unlock()
	}()
}

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
	rf.mu.Lock()
	if rf.role != "leader" {
		rf.mu.Unlock()
		return -1, -1, false;
	}
	// append to local log
	rf.log = append(rf.log, Log{Term: rf.currentTerm, Command: command})
	rf.persist()
	index := rf.real_log_size() - 1
	term := rf.currentTerm
	rf.matchIndex[rf.me] = rf.real_log_size() - 1
	rf.nextIndex[rf.me] = rf.real_log_size()
	//log.Printf("#%v: %v $%v updated matchIndex = %v with new command %v %v", rf.currentTerm, rf.role, rf.me, rf.matchIndex, index, command)
	rf.mu.Unlock()
	return index + 1, term, true
}

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
		rf.mu.Lock()
		if rf.role != "leader" {
			rf.mu.Unlock()
			rf.commitCond.Wait()
			continue
		}
		buf := make([]int, len(rf.peers))
		copy(buf[:], rf.matchIndex)
		sort.Ints(buf)
		majorityIndex := (len(rf.peers) + 1) / 2 - 1
		candidateIndex := buf[majorityIndex]
		newMsgs := []ApplyMsg{}
		if candidateIndex > rf.commitIndex && rf.log_at(candidateIndex).Term == rf.currentTerm {
			for i := rf.commitIndex + 1; i <= candidateIndex; i++ {
				newMsgs = append(newMsgs, ApplyMsg{
					CommandValid: true,
					Command: rf.log_at(i).Command,
					CommandIndex: i + 1,
					CommandType: "Apply",
				})
				rf.commitIndex = i
			}
		}
		rf.mu.Unlock()
		for {
			rf.applyMu.Lock()
			if len(newMsgs) == 0 {
				break
			}
			if rf.applyIndex + 1 == newMsgs[0].CommandIndex {
				break
			}
			rf.applyMu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		for _, msg := range newMsgs {
			rf.applyCh <- msg
			rf.applyIndex = msg.CommandIndex
		}
		rf.applyMu.Unlock()
		rf.commitCond.Wait()
	}
	rf.commitCond.L.Unlock()
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	rf.heartbeat = make(chan bool)	
	rf.applyCh = applyCh

	rf.role = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.commitCond = sync.NewCond(new(sync.Mutex))
	rf.logOffset = 0
	rf.applyIndex = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.real_log_size()
		rf.matchIndex[i] = -1
	}

	go rf.waitForElection(Token {rf.currentTerm, rf.role})
	go rf.moniterCommit()

	return rf
}
