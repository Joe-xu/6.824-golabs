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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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

const (
	minElectionTimeout    = 250
	electionRandtimeRange = 200
	heartbeatInterval     = 200
)

type Signal int32

const (
	requestVoteSignal Signal = iota
	voteGrantSignal
	heartbeatSignal
	// cancelElecSignal
	stepDownSignal
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentLeader int // peer's index, and -1 mean null
	currentTerm   int
	votedFor      int   // peer's index, and -1 mean null
	log           []Log // start from 1

	// volatile state
	commitIndex int
	lastApplied int // index

	applyCh chan ApplyMsg

	signalCh chan Signal

	resetElecTimeoutNoticy chan Signal // keep being follower
	stepDownNoticy         chan Signal // leader turn into follower
	cancelElecNoticy       chan Signal // candidate turn into follower

	// only for leader
	nextIndex  []int // next log entry index to send
	matchIndex []int
}

type Log struct {
	LogIdx  int // start from 1
	Term    int
	Content string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.currentLeader == rf.me
}

//
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
}

//
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int
	CandidateId  int // peer's index
	LastLogIndex int // index of log array
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	log.Printf("N[%d] requestvote", rf.me)
	defer log.Printf("N[%d] rv exit", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		// reject
		reply.VoteGranted = false
		return
	}

	rf.signalCh <- requestVoteSignal

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

		if rf.commitIndex <= args.LastLogIndex && rf.log[rf.commitIndex].Term <= args.LastLogTerm {

			rf.signalCh <- voteGrantSignal

			if rf.currentLeader == rf.me {
				rf.signalCh <- stepDownSignal
			}

			log.Printf("N[%d]T[%d] grant vote for N[%d]T[%d]",
				rf.me, rf.currentTerm, args.CandidateId, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			return
		}
	}

	return

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []Log

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reject

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.signalCh <- heartbeatSignal

	if rf.currentLeader == rf.me {
		rf.signalCh <- stepDownSignal
	}

	log.Printf("N[%d]T[%d] heartbeat from N[%d]T[%d]", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	rf.votedFor = -1
	rf.currentTerm = args.Term
	rf.currentLeader = args.LeaderId

	// TODO: more logic

	reply.Term = rf.currentTerm
	reply.Success = true

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok

}

//
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
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("N[%d]T[%d] killed", rf.me, rf.currentTerm)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentLeader = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1, 10)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	rf.signalCh = make(chan Signal, 100)

	rf.stepDownNoticy = make(chan Signal, 100)
	rf.resetElecTimeoutNoticy = make(chan Signal, 100)
	rf.cancelElecNoticy = make(chan Signal, 100)

	go rf.signalRouter()
	defer rf.runAsFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) signalRouter() {

	tryToSend := func(ch chan<- Signal, sig Signal) {
		select {
		case ch <- sig:
		default:
			log.Printf("N[%d] drop sig<%+v>", rf.me, sig)
		}
	}

	for {

		select {
		case sig := <-rf.signalCh:

			switch sig {
			case requestVoteSignal:
				// do nothing

				tryToSend(rf.resetElecTimeoutNoticy, sig)
			case voteGrantSignal:
			case heartbeatSignal:

				tryToSend(rf.cancelElecNoticy, sig)
				tryToSend(rf.resetElecTimeoutNoticy, sig)
			case stepDownSignal:

				tryToSend(rf.stepDownNoticy, sig)
			}

		}
	}

}

func (rf *Raft) electionTimer() {

	timeout := time.Millisecond * time.Duration(rand.Intn(electionRandtimeRange)+minElectionTimeout)
	for {

		timer := time.NewTicker(timeout)

		select {
		case <-rf.resetElecTimeoutNoticy:
			timer.Stop()
			timeout = time.Millisecond * time.Duration(rand.Intn(electionRandtimeRange)+minElectionTimeout)
		case <-timer.C:
			timer.Stop()
			// log.Printf(" timeout")
			go rf.runAsCandidate()
			return
		}
	}
}

const (
	asLeader = iota
	asFollower
	retry
)

func (rf *Raft) startNewElection() {

	replys := make([]RequestVoteReply, len(rf.peers))

	// log.Printf("N[%d]T[%d] new elec", rf.me, rf.currentTerm)
	rf.mu.Lock()
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].LogIdx,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	done := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {

			ok := rf.sendRequestVote(i, req, &replys[i])
			done <- ok

		}(i)

	}

	totalCnt := 1
	callSuccessNum := 1 // include itself
	t := minElectionTimeout * time.Millisecond
	timeout := time.NewTicker(t)
	defer timeout.Stop()
WAIT_VOTE:
	for {
		select {
		case <-timeout.C:

			go func() {
				for range done {
				}
			}()

			log.Printf("N[%d] wait vote timeout", rf.me)

			break WAIT_VOTE

		case <-rf.cancelElecNoticy:

			go func() {
				for range done {
				}
			}()

			log.Printf("N[%d] cancel elec#0", rf.me)
			go rf.runAsFollower()
			return

		case ok := <-done:
			if ok {
				callSuccessNum++
			}

			totalCnt++
			if totalCnt >= len(rf.peers) {
				break WAIT_VOTE
			}
		}
	}

	if callSuccessNum <= 1 {

		log.Printf("N[%d] cancel elec#1", rf.me)
		go rf.runAsFollower()

		return
	}

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	ret := rf.handleVoteResult(replys, callSuccessNum)

	switch ret {
	case asFollower:
		go rf.runAsFollower()
	case asLeader:
		go rf.runAsLeader()
	case retry:

		go func() {
			t := time.Duration(rand.Intn(electionRandtimeRange)+minElectionTimeout) * time.Millisecond
			log.Printf("N[%d] #1 %v", rf.me, t)
			time.Sleep(t)
			rf.runAsCandidate()
		}()

	}

}

func (rf *Raft) handleVoteResult(replys []RequestVoteReply, callSuccessNum int) int {

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	voteCount := 1 // include itself
	for i, reply := range replys {
		if i == rf.me {
			continue
		}

		// turn into follower
		if reply.Term > currentTerm {

			return asFollower
		}

		if reply.VoteGranted {
			voteCount++
		}
	}

	//  if get the majority vote
	voteRate := float32(voteCount) / float32(callSuccessNum)
	log.Printf("N[%d] %d/%d (%d) in T[%d]", rf.me, voteCount, callSuccessNum, len(rf.peers), rf.currentTerm)
	if voteRate == 0.5 {

		return retry
	}
	if voteRate > 0.5 {
		log.Printf("N[%d] become leader with %f in T[%d]", rf.me, voteRate, rf.currentTerm)

		// start to send heartbeat

		return asLeader
	}

	return asFollower
}

func (rf *Raft) heartBeatTimer() {

	timer := time.NewTicker(heartbeatInterval * time.Millisecond)
	defer timer.Stop()

	for {

		select {
		case <-rf.stepDownNoticy:
			log.Printf("leader %d stepdown", rf.me)
			rf.runAsFollower()
			return
		case <-timer.C:
			rf.sendHeartBeat()
		}

	}
}

func (rf *Raft) sendHeartBeat() {

	rf.mu.Lock()
	req := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  make([]Log, 0),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// with best effort
		go func(i int) {
			rf.sendAppendEntries(i, req, &AppendEntriesReply{})
		}(i)

	}

}

func (rf *Raft) runAsLeader() {

	rf.mu.Lock()

	// some init work
	rf.currentLeader = rf.me
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.mu.Unlock()

	rf.sendHeartBeat()

	go rf.heartBeatTimer()
}

func (rf *Raft) runAsFollower() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = -1

	go rf.electionTimer()

}

func (rf *Raft) runAsCandidate() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentLeader = -1
	rf.votedFor = rf.me
	rf.currentTerm++

	go rf.startNewElection()
}
