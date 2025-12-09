package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RaftState int

const (
	T_FOLLOWER RaftState = iota
	T_CANDIDATE
	T_LEADER
)

const HEARTBEAT_INTERVAL = 100 * time.Millisecond
const HEARTBEAT_TIMEOUT = 500 * time.Millisecond
const ELECTION_TIMEOUT = 500 * time.Millisecond

// Each log entry stores a state machine command along with the term
// number when the entry was received by the leader.
type LogEntry struct {
	Command interface{}
	Term    int
	// index   int // bias in log slice
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// private state
	state             RaftState
	nextHeartBeatTime time.Time
	nextElectionTime  time.Time
	applyCh           chan raftapi.ApplyMsg
	applyCond         *sync.Cond

	snapshot                  []byte
	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == T_LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	rf.persister.Save(raftstate, rf.snapshot)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	// log stuff
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	// check term and update state
	rf.checkTermChange(args.Term)
	// obtain lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fill in reply struct
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// check candidate's term
	if rf.currentTerm > args.Term {
		DPrintf("Server %v reject %v for currentTerm %v > term %v", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	// check if vote for anybody else
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	} else {
		DPrintf("Server %v reject %v for it has voted for %v", rf.me, args.CandidateId, rf.votedFor)
		return
	}

	// DPrintf("%v reply %v with term %v %v", rf.me, args.CandidateId, args.Term, reply.VoteGranted)

	// check if candidate's log is at least as up-to-date as mine
	lastLog := rf.log[len(rf.log)-1]
	if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex) {
		DPrintf("Server %v reject %v for it's log is more up-to-date", rf.me, args.CandidateId)
		DPrintf("lastLogTerm: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, lastLog.Term, args.CandidateId, args.LastLogTerm, lastLog.Term > args.LastLogTerm)
		DPrintf("lastLogIndex: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, len(rf.log)-1, args.CandidateId, args.LastLogIndex, lastLog.Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)

		return
	}
	// DPrintf("Follower %v's log: %v", rf.me, rf.log)
	// DPrintf("lastLogTerm: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, lastLog.Term, args.CandidateId, args.LastLogTerm, lastLog.Term > args.LastLogTerm)
	// DPrintf("lastLogIndex: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, len(rf.log)-1, args.CandidateId, args.LastLogIndex, lastLog.Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)

	rf.resetElectionTimer()
	rf.votedFor = args.CandidateId
	rf.persist()
	// DPrintf("Follower %v votes for %v", rf.me, args.CandidateId)
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XIndex int
	XTerm  int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.checkTermChange(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%v reject %v for currentTerm %v > %v", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		DPrintf("Follower %v reject AppendEntries from Leader for mismatched term %v with full reply = %+v", rf.me, args.LeaderId, reply)
		return
	}
	rf.resetElectionTimer()
	// 2. reply false if log doesn't an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// find the first conflicting term and its index
		reply.Success = false

		reply.XLen = len(rf.log)
		if args.PrevLogIndex < len(rf.log) {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.XTerm {
					reply.XIndex = i + 1
					break
				}
			}
		}
		DPrintf("Follower %v reject AppendEntries from Leader %v for log inconsistency at prevLogIndex %v and prevLogTerm %v with full reply = %+v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, reply)
		DPrintf("Follower %v reject AppendEntries from Leader %v for log inconsistency with full reply = %+v", rf.me, args.LeaderId, reply)
		return
	}

	reply.Success = true
	rf.state = T_FOLLOWER
	// DPrintf("Follower %v append entries from Leader %v with index %v", rf.me, args.LeaderId, args.PrevLogIndex)
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	DPrintf("Follower %v log before append: %v, and about to append entries %v at prevlogindex = %v", rf.me, rf.log, args.Entries, args.PrevLogIndex)

	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex < len(rf.log) {
			// if entry conflicts with existing entry
			if rf.log[logIndex].Term != entry.Term {
				rf.log = rf.log[:logIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	DPrintf("Follower %v log after append: %v", rf.me, rf.log)
	rf.persist()

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	// DPrintf("Follower %v receive leadercommit %v", rf.me, args.LeaderCommit)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
	DPrintf("Follower %v signal applier to work", rf.me)
	rf.applyCond.Signal()

	// rf.updateApplied()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == T_LEADER
	if !isLeader {
		return index, term, isLeader
	}

	index, term = len(rf.log), rf.currentTerm
	newLogEntry := LogEntry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, newLogEntry)
	rf.persist()
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	DPrintf("Leader %v receive a command from client with index %v with term %v", rf.me, index, term)

	// todo start to append to end
	// rf.resetHeatbeatTimer()
	go rf.boardcastNewEntry(index)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case T_LEADER:
			// check if it is time to send heartbeat
			if rf.checkHeartbeatTimeout() {
				rf.sendHeartbeats()
			}
			// sendheartbeat
		case T_CANDIDATE:
			fallthrough
		case T_FOLLOWER:
			// check if election timeout
			// state election
			if rf.checkElectionTimeout() {
				// DPrintf("%v starts an Election", rf.me)
				rf.startElection()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			DPrintf("Server %v wake up! wait for new log to apply from index %v to %v", rf.me, rf.lastApplied+1, rf.commitIndex)
		}
		DPrintf("Server %v begin to apply log from index %v to %v", rf.me, rf.lastApplied+1, rf.commitIndex)
		applyIndexStart := rf.lastApplied + 1
		applyIndexEnd := rf.commitIndex

		entriesCount := applyIndexEnd - applyIndexStart + 1
		entriesToApplied := make([]LogEntry, entriesCount)
		copy(entriesToApplied, rf.log[applyIndexStart:applyIndexEnd+1])

		lastApplied := rf.lastApplied
		rf.mu.Unlock()

		for _, entry := range entriesToApplied {
			lastApplied++
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied,
			}
			// DPrintf("Server %v apply message with index %v and command %v", rf.me, rf.commitIndex-len(entriesToApplied)+1+i, entry)
		}
		rf.mu.Lock()
		if lastApplied > rf.lastApplied {
			rf.lastApplied = lastApplied
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) checkTermChange(newTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < newTerm {
		DPrintf("%v find a higher term, change to FOLLOWER state", rf.me)
		rf.currentTerm = newTerm
		rf.votedFor = -1

		rf.state = T_FOLLOWER
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) resetElectionTimer() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	ms := 50 + (rand.Int63())%200
	rf.nextElectionTime = time.Now().Add(time.Duration(ms) * time.Millisecond).Add(ELECTION_TIMEOUT)
}

func (rf *Raft) checkElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().After(rf.nextElectionTime)
}

func (rf *Raft) resetHeatbeatTimer() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.nextHeartBeatTime = time.Now().Add(HEARTBEAT_INTERVAL)
}

func (rf *Raft) checkHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().After(rf.nextHeartBeatTime)
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	logLen := len(rf.log)
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.sendHeartbeat(peer, logLen)
		}
	}
	rf.mu.Lock()
	rf.resetHeatbeatTimer()
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(peer int, logIndex int) bool {
	// logIndex: the logIndex about to send to followers
	rf.mu.Lock()
	start := rf.nextIndex[peer]
	end := min(logIndex+1, len(rf.log))

	prevLogIndex := start - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	var entries []LogEntry
	if start <= end {
		entries = rf.log[start:end]
	} else {
		entries = nil
	}
	DPrintf("Leader %v send entries start = %v, end = %v to Follower %v and now its matchindex: %v with term = %v", rf.me, start, end, peer, rf.matchIndex, rf.currentTerm)
	// DPrintf("Leader %v send entries %v start = %v, end = %v to Follower %v", rf.me, entries, start, end, peer)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	if ok := rf.sendAppendEntries(peer, &args, &reply); !ok {
		return false
	}

	// // check success
	// check success
	if !reply.Success {
		DPrintf("Leader %v got rejection from %v in term %v with reply.term = %v", rf.me, peer, args.Term, reply.Term)
		rf.checkTermChange(reply.Term)
		rf.mu.Lock()
		if rf.state != T_LEADER || reply.XLen == 0 {
			// reject due to term change and outdated reply
			// if reply.xlen == 0 and !reply.success, it means that the request is rejected for outdated term
			rf.mu.Unlock()
			return false
		}
		DPrintf("Leader %v: before update, nextIndex[%v] = %v", rf.me, peer, rf.nextIndex[peer])
		// decrement nextIndex and retry
		// optimize nextIndex using conflict info
		//  Case 1: leader doesn't have XTerm:
		//     nextIndex = XIndex
		//   Case 2: leader has XTerm:
		//     nextIndex = (index of leader's last entry for XTerm) + 1
		//   Case 3: follower's log is too short:
		//     nextIndex = XLen
		DPrintf("Leader %v receive full reply: %+v", rf.me, reply)
		DPrintf("Leader %v receive additional info: XTerm = %v, XIndex = %v, XLen = %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		if reply.XLen <= rf.nextIndex[peer]-1 {
			// Case 3
			rf.nextIndex[peer] = reply.XLen
			DPrintf("Leader %v: update nextIndex[%v](case 3) = %v", rf.me, peer, reply.XLen)
		} else {
			// Case 1 and 2
			found := false
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					rf.nextIndex[peer] = i + 1
					found = true
					DPrintf("Leader %v: update nextIndex[%v](case 2) = %v", rf.me, peer, rf.nextIndex[peer])

					break
				}
			}
			if !found {
				// Case 1
				DPrintf("Leader %v: update nextIndex[%v](case 1) = %v", rf.me, peer, rf.nextIndex[peer])

				rf.nextIndex[peer] = reply.XIndex
			}
		}
		DPrintf("Leader %v: after update, nextIndex[%v] = %v", rf.me, peer, rf.nextIndex[peer])
		// original decrement
		// rf.nextIndex[peer] = max(start-1, 1)
		// start = rf.nextIndex[peer]

		rf.mu.Unlock()
		return false
	} else {

		rf.mu.Lock()
		rf.nextIndex[peer] = max(rf.nextIndex[peer], end)
		rf.matchIndex[peer] = prevLogIndex + len(args.Entries)
		DPrintf("Leader %v check Follower %v nextIndex = %v, matchIndex = %v, all matchIndex: %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.matchIndex)
		rf.advanceCommitIndex()
		rf.mu.Unlock()
		return true
	}
	// for !reply.Success {
	// 	DPrintf("Leader %v got rejection from %v", rf.me, peer)
	// 	rf.checkTerm(reply.Term)
	// 	rf.mu.Lock()
	// 	if rf.state != T_LEADER {
	// 		// if rf.state != T_LEADER || rf.nextIndex[peer] <= 1 {
	// 		rf.mu.Unlock()
	// 		return false
	// 	}
	// 	// decrement nextIndex and retry
	// 	// optimize nextIndex using conflict info
	// 	//  Case 1: leader doesn't have XTerm:
	// 	//     nextIndex = XIndex
	// 	//   Case 2: leader has XTerm:
	// 	//     nextIndex = (index of leader's last entry for XTerm) + 1
	// 	//   Case 3: follower's log is too short:
	// 	//     nextIndex = XLen
	// 	if reply.XLen <= rf.nextIndex[peer]-1 {
	// 		// Case 3
	// 		rf.nextIndex[peer] = reply.XLen
	// 	} else {
	// 		// Case 1 and 2
	// 		found := false
	// 		for i := len(rf.log) - 1; i >= 0; i-- {
	// 			if rf.log[i].Term == reply.XTerm {
	// 				rf.nextIndex[peer] = i + 1
	// 				found = true
	// 				break
	// 			}
	// 		}
	// 		if !found {
	// 			// Case 1
	// 			rf.nextIndex[peer] = reply.XIndex
	// 		}
	// 	}
	// 	// original decrement
	// 	// rf.nextIndex[peer] = max(start-1, 1)
	// 	// start = rf.nextIndex[peer]

	// 	start = rf.nextIndex[peer]
	// 	prevLogIndex = start - 1
	// 	prevLogTerm = rf.log[prevLogIndex].Term
	// 	end = min(logIndex+1, len(rf.log))
	// 	var entries []LogEntry
	// 	if start <= end {
	// 		entries = rf.log[start:end]
	// 	} else {
	// 		entries = nil
	// 	}
	// 	args = AppendEntriesArgs{
	// 		Term:         rf.currentTerm,
	// 		LeaderId:     rf.me,
	// 		PrevLogIndex: prevLogIndex,
	// 		PrevLogTerm:  prevLogTerm,
	// 		Entries:      entries,
	// 		LeaderCommit: rf.commitIndex,
	// 	}
	// 	DPrintf("Leader %v send entries %v start = %v, end = %v to Follower %v", rf.me, entries, start, end, peer)

	// 	rf.mu.Unlock()

	// 	if ok := rf.sendAppendEntries(peer, &args, &reply); !ok {
	// 		return false
	// 	}
	// }

	// rf.mu.Lock()
	// rf.nextIndex[peer] = max(rf.nextIndex[peer], end)
	// rf.matchIndex[peer] = prevLogIndex + len(args.Entries)
	// DPrintf("Leader %v check Follower %v nextIndex = %v, matchIndex = %v, all matchIndex: %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.matchIndex)
	// rf.advanceCommitIndex()
	// rf.mu.Unlock()
	// return true
}

func (rf *Raft) boardcastNewEntry(logIndex int) {
	// success := 1
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.sendHeartbeat(peer, logIndex)
		}
	}
}

func (rf *Raft) advanceCommitIndex() {
	N := len(rf.matchIndex)
	sortedMatchIndex := make([]int, N)
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)

	midIndex := sortedMatchIndex[N/2]
	if midIndex > rf.commitIndex && rf.log[midIndex].Term == rf.currentTerm {
		DPrintf("Leader %v advance commitIndex from %v to %v, all matchIndex: %v", rf.me, rf.commitIndex, midIndex, rf.matchIndex)
		rf.commitIndex = midIndex
	}
	DPrintf("Leader %v signal applier to work", rf.me)
	rf.applyCond.Signal()
	// rf.updateApplied()
}

func (rf *Raft) startElection() {
	// persist state change
	myVotes := 1
	rf.mu.Lock()
	rf.state = T_CANDIDATE
	rf.resetElectionTimer()
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.persist()
	DPrintf("Candidate %v starts an election with term %v", rf.me, rf.currentTerm)
	DPrintf("Candidate %v's log: %v", rf.me, rf.log)
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				lastLog := rf.log[len(rf.log)-1]
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  lastLog.Term,
				}
				reply := RequestVoteReply{}

				rf.mu.Unlock()
				// DPrintf("%v send RequestVote to %v with term %v", rf.me, peer, rf.currentTerm)

				rf.sendRequestVote(peer, &args, &reply)
				rf.checkTermChange(reply.Term)

				rf.mu.Lock()
				if rf.state == T_CANDIDATE && reply.VoteGranted {
					// DPrintf("%v got votes from %v with term %v", rf.me, peer, rf.currentTerm)
					myVotes++
					if myVotes > len(rf.peers)/2 { // receive majority votes
						rf.state = T_LEADER
						//reinit nextIndex and matchIndex
						for p := range rf.peers {
							rf.nextIndex[p] = len(rf.log)
							rf.matchIndex[p] = 0
						}
						rf.matchIndex[rf.me] = len(rf.log) - 1
						rf.mu.Unlock()
						rf.sendHeartbeats()
						return
						// rf.mu.Lock()
					}
				}
				rf.mu.Unlock()
			}(peer)
		}
	}
}

func (rf *Raft) updateApplied() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("Server %v apply message with index %v and command %v", rf.me, rf.lastApplied, rf.log[rf.lastApplied])
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	DPrintf("Server %v starts working...", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1 // null
	rf.log = append(rf.log, LogEntry{
		Command: nil,
		Term:    0,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for peer := range peers {
		rf.nextIndex[peer] = rf.commitIndex + 1
		rf.matchIndex[peer] = 0
	}

	rf.resetElectionTimer()
	rf.resetHeatbeatTimer()

	rf.state = T_FOLLOWER
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.snapshot = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
