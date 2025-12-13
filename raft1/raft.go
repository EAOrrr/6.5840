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

	firstLogIndex   int
	snapshotPending bool

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

	e.Encode(rf.snapshotLastIncludedIndex)
	e.Encode(rf.snapshotLastIncludedTerm)

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
	var currentTerm, votedFor int
	var log []LogEntry
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotLastIncludedIndex = lastIncludedIndex
		rf.snapshotLastIncludedTerm = lastIncludedTerm
		rf.firstLogIndex = lastIncludedIndex
		rf.snapshot = rf.persister.ReadSnapshot()

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("server %v snapshot called", rf.me)
	if index > rf.snapshotLastIncludedIndex {
		// DPrintf("server %v snapshot at index %v", rf.me, index)
		// DPrintf("server %v log before snapshot:%v", rf.me, rf.log)
		// DPrintf("server %v firstLogIndex before snapshot:%v", rf.me, rf.firstLogIndex)
		rf.snapshotLastIncludedIndex = index
		rf.snapshotLastIncludedTerm = rf.log[index-rf.firstLogIndex].Term
		rf.snapshot = snapshot
		rf.truncateLog(index)
		// DPrintf("server %v log after snapshot:%v", rf.me, rf.log)
		// DPrintf("server %v firstLogIndex after snapshot:%v", rf.me, rf.firstLogIndex)

		rf.persist()
	}
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
	// obtain lock
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer func(reply_ *RequestVoteReply) {
	// DPrintf("RPC RequestVote - from candidate %v to sever %v with args: %+v and reply:%+v", args.CandidateId, rf.me, args, reply_)
	// }(reply)
	rf.checkTermChange(args.Term)

	// fill in reply struct
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// check candidate's term
	if rf.currentTerm > args.Term {
		// DPrintf("Server %v reject %v for currentTerm %v > term %v", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	// check if vote for anybody else
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	} else {
		// DPrintf("Server %v reject %v for it has voted for %v", rf.me, args.CandidateId, rf.votedFor)
		return
	}

	// DPrintf("%v reply %v with term %v %v", rf.me, args.CandidateId, args.Term, reply.VoteGranted)

	// check if candidate's log is at least as up-to-date as mine
	lastLog := rf.log[len(rf.log)-1]
	if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && len(rf.log)-1+rf.firstLogIndex > args.LastLogIndex) {
		// DPrintf("Server %v reject %v for it's log is more up-to-date", rf.me, args.CandidateId)
		// DPrintf("lastLogTerm: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, lastLog.Term, args.CandidateId, args.LastLogTerm, lastLog.Term > args.LastLogTerm)
		// DPrintf("lastLogIndex: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, len(rf.log)-1+rf.firstLogIndex, args.CandidateId, args.LastLogIndex, lastLog.Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)

		return
	}
	// DPrintf("Follower %v's log: %v", rf.me, rf.log)
	// DPrintf("lastLogTerm: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, lastLog.Term, args.CandidateId, args.LastLogTerm, lastLog.Term > args.LastLogTerm)
	// DPrintf("lastLogIndex: Server %v: %v VS Candidate %v: %v, result: %v", rf.me, len(rf.log)-1+rf.firstLogIndex, args.CandidateId, args.LastLogIndex, lastLog.Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)

	rf.resetElectionTimer()
	if rf.votedFor != args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist()
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer func(reply_ *AppendEntriesReply) {
	// 	DPrintf("RPC AppendEntries - from leader %v to sever %v with args: %+v and reply:%+v", args.LeaderId, rf.me, args, reply_)
	// }(reply)

	rf.checkTermChange(args.Term)

	reply.Term = rf.currentTerm
	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// DPrintf("%v reject %v for currentTerm %v > %v", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		// DPrintf("Follower %v reject AppendEntries from Leader for mismatched term %v with full reply = %+v", rf.me, args.LeaderId, reply)
		return
	}
	rf.resetElectionTimer()
	// 2. reply false if log doesn't an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex < rf.firstLogIndex {
		// log entry before rf.firstLogIndex is commited and if receive a message with prevLogIndex
		// when raft works correctly it only happens when RPC arrived in wrong order
		reply.Success = false
		reply.XLen = len(rf.log) + rf.firstLogIndex
		return
	}
	if args.PrevLogIndex >= len(rf.log)+rf.firstLogIndex || rf.log[args.PrevLogIndex-rf.firstLogIndex].Term != args.PrevLogTerm {
		// find the first conflicting term and its index
		reply.Success = false

		reply.XLen = len(rf.log) + rf.firstLogIndex
		// DPrintf("Follower %v full log now: %v", rf.me, rf.log)
		if args.PrevLogIndex < len(rf.log)+rf.firstLogIndex {
			reply.XTerm = rf.log[args.PrevLogIndex-rf.firstLogIndex].Term
			reply.XIndex = rf.firstLogIndex + 1
			for i := args.PrevLogIndex - rf.firstLogIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.XTerm {
					reply.XIndex = i + 1 + rf.firstLogIndex
					break
				}
			}
		} else {
			// DPrintf("Follower %v reject AppendEntries from Leader %v for log inconsistency at prevLogIndex %v which is beyond my log len %v", rf.me, args.LeaderId, args.PrevLogIndex, len(rf.log))
		}
		// DPrintf("Follower %v reject AppendEntries from Leader %v for log inconsistency at prevLogIndex %v and prevLogTerm %v with full reply = %+v", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, reply)
		// DPrintf("Follower %v reject AppendEntries from Leader %v for log inconsistency with full reply = %+v", rf.me, args.LeaderId, reply)
		return
	}

	reply.Success = true
	rf.state = T_FOLLOWER
	// DPrintf("Follower %v append entries from Leader %v with index %v", rf.me, args.LeaderId, args.PrevLogIndex)
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	// DPrintf("Follower %v log before append: %v, and about to append entries %v at prevlogindex = %v from leader %v", rf.me, rf.log, args.Entries, args.PrevLogIndex, args.LeaderId)

	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex >= len(rf.log)+rf.firstLogIndex {
			rf.log = append(rf.log, args.Entries[i:]...)
			break // 追加完成，跳出循环
		}

		// B. logIndex 仍在 Follower log 长度内，但 term 不匹配: 发现冲突
		if rf.log[logIndex-rf.firstLogIndex].Term != entry.Term {
			// 冲突点：删除从 logIndex 开始的所有现有条目
			rf.log = rf.log[:logIndex-rf.firstLogIndex]
			// 追加从当前新条目开始的所有剩余条目
			rf.log = append(rf.log, args.Entries[i:]...)
			break // 冲突解决和追加完成，跳出循环
		}

		// C. logIndex 仍在 log 长度内，且 Term 匹配: 不做任何操作，继续检查下一个
	}

	// DPrintf("Follower %v log after append: %v", rf.me, rf.log)
	rf.persist()

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	// DPrintf("Follower %v receive leadercommit %v", rf.me, args.LeaderCommit)
	newCommitIndex := min(args.LeaderCommit, len(rf.log)-1+rf.firstLogIndex)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		// DPrintf("Follwer %v signal applier to apply from %v to %v", rf.me, rf.lastApplied+1, rf.commitIndex)
		rf.applyCond.Signal()

	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer func(reply_ *InstallSnapshotReply) {
	// 	DPrintf("RPC InstallSnapshot - from leader %v to sever %v with args: {term: %v, lastIncludedIndex:%v, lastIncludedTerm: %v} and reply:%+v", args.LeaderId, rf.me, args.Term, args.LastIncludedIndex, args.LastIncludedTerm, reply_)
	// }(reply)

	rf.checkTermChange(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex > rf.commitIndex {
		// DPrintf("Follower %v receive snapshot from Leader %v with lastIncludedIndex = %v, lastIncludedTerm = %v", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
		// accept only newer snapshot
		rf.snapshot = args.Data
		rf.snapshotLastIncludedIndex = args.LastIncludedIndex
		rf.snapshotLastIncludedTerm = args.LastIncludedTerm
		// apply data
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		// rf.truncateLog(args.LastIncludedIndex)
		// reset log
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{
			Term:    args.LastIncludedTerm,
			Command: nil,
		}
		rf.firstLogIndex = rf.snapshotLastIncludedIndex
		rf.persist()
		rf.snapshotPending = true
		rf.applyCond.Signal()

		// DPrintf("Follower %v after apply snapshot with lastincludedindex = %v and lastincludedterm = %v now firstlogidnex = %v, log = %v", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, rf.firstLogIndex, rf.log)
	}
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	index, term = len(rf.log)+rf.firstLogIndex, rf.currentTerm
	newLogEntry := LogEntry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, newLogEntry)
	rf.persist()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	// DPrintf("Leader %v receive a command: %v from client with index %v with term %v", rf.me, command, index, term)

	// todo start to append to end
	// go rf.boardcastNewEntry()
	go rf.sendHeartbeats()
	// rf.resetHeartbeatTimerImm()

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
	// close(rf.applyCh)
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
				go rf.sendHeartbeats()
			}
		case T_CANDIDATE:
			fallthrough
		case T_FOLLOWER:
			// check if election timeout
			// state election
			if rf.checkElectionTimeout() {
				// DPrintf("%v starts an Election", rf.me)
				go rf.startElection()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		// ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.snapshotPending && !rf.killed() {
			rf.applyCond.Wait()
			// DPrintf("Server %v wake up! wait for new log to apply from index %v to %v", rf.me, rf.lastApplied+1, rf.commitIndex)
		}
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		if rf.snapshotPending {
			applyMsg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotLastIncludedTerm,
				SnapshotIndex: rf.snapshotLastIncludedIndex,
			}
			rf.snapshotPending = false
			rf.mu.Unlock()

			// DPrintf("Server %v begin to apply snapshot with lastIncludedIndex = %v, lastIncludedTerm = %v", rf.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			rf.applyCh <- applyMsg
			// DPrintf("Server %v finish applying snapshot with lastIncludedIndex = %v, lastIncludedTerm = %v", rf.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				continue
			}
		}

		applyIndexStart := rf.lastApplied + 1 - rf.firstLogIndex
		applyIndexEnd := rf.commitIndex - rf.firstLogIndex

		var entriesToApplied []LogEntry
		entriesCount := applyIndexEnd - applyIndexStart + 1
		entriesToApplied = make([]LogEntry, entriesCount)
		copy(entriesToApplied, rf.log[applyIndexStart:applyIndexEnd+1])
		// DPrintf("Server %v begin to apply log from index %v to %v, %v in total, logs: %v", rf.me, rf.lastApplied+1, rf.commitIndex, entriesCount, entriesToApplied)

		lastApplied := rf.lastApplied
		rf.mu.Unlock()

		for _, entry := range entriesToApplied {
			lastApplied++
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied,
			}
		}
		rf.mu.Lock()
		// DPrintf("Server %v finish to apply log, and lastApplied = %v , commitIndex = %v", rf.me, rf.lastApplied+1, rf.commitIndex)

		if lastApplied > rf.lastApplied {
			rf.lastApplied = lastApplied
		}
		rf.mu.Unlock()

	}
	close(rf.applyCh)
}

func (rf *Raft) checkTermChange(newTerm int) {
	if rf.currentTerm < newTerm {
		// DPrintf("%v find a higher term, change to FOLLOWER state", rf.me)
		rf.currentTerm = newTerm
		rf.votedFor = -1

		rf.state = T_FOLLOWER
		rf.persist()
		return
	}
}

func (rf *Raft) resetElectionTimer() {
	ms := 50 + (rand.Int63())%200
	rf.nextElectionTime = time.Now().Add(time.Duration(ms) * time.Millisecond).Add(ELECTION_TIMEOUT)
}

func (rf *Raft) checkElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().After(rf.nextElectionTime)
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.nextHeartBeatTime = time.Now().Add(HEARTBEAT_INTERVAL)
}

func (rf *Raft) resetHeartbeatTimerImm() {
	rf.nextHeartBeatTime = time.Now()
}

func (rf *Raft) checkHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Now().After(rf.nextHeartBeatTime)
}

func (rf *Raft) sendHeartbeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.sendHeartbeat(peer)
		}
	}
	rf.mu.Lock()
	rf.resetHeartbeatTimer()
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(peer int) {
	rf.mu.Lock()
	if rf.state != T_LEADER {
		// 可能client Start的时候是leader，但是期间RPC处理抢到锁发现更高的任期，发送心跳的时候已经不是leader了
		rf.mu.Unlock()
		return
	}
	start := rf.nextIndex[peer]
	end := len(rf.log)

	if start < rf.firstLogIndex+1 {
		// need to send InstallSnapshot RPC
		// DPrintf("Leader %v send InstallSnapshot to Follower %v for nextIndex %v < firstLogIndex %v", rf.me, peer, start, rf.firstLogIndex)
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.snapshotLastIncludedIndex,
			LastIncludedTerm:  rf.snapshotLastIncludedTerm,
			Data:              rf.snapshot,
		}
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()

		if ok := rf.sendInstallSnapshot(peer, &args, &reply); !ok {
			return
		}

		rf.mu.Lock()
		rf.checkTermChange(reply.Term)
		if rf.state != T_LEADER {
			rf.mu.Unlock()
			return
		}
		// advance nextIndex and matchIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		rf.matchIndex[peer] = args.LastIncludedIndex
		// DPrintf("Leader %v update follower %v's matchindex = %v, nextindex = %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		rf.mu.Unlock()
		return
	}

	prevLogIndex := start - 1
	prevLogTerm := rf.log[prevLogIndex-rf.firstLogIndex].Term
	firstLogIndex := rf.firstLogIndex

	start = start - firstLogIndex
	var entries []LogEntry
	if start <= end {
		entries = rf.log[start:end]
	} else {
		entries = nil
	}
	// DPrintf("Leader %v send entries start = %v, end = %v to Follower %v and now its matchindex: %v with term = %v", rf.me, start, end, peer, rf.matchIndex, rf.currentTerm)
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
		return
	}

	// check success
	if !reply.Success {
		// AppendEntries fail
		// DPrintf("Leader %v got rejection from %v in term %v with reply.term = %v", rf.me, peer, args.Term, reply.Term)
		rf.mu.Lock()
		rf.checkTermChange(reply.Term)
		if rf.state != T_LEADER || reply.XLen == 0 {
			// reject due to term change and outdated reply
			// if reply.xlen == 0 and !reply.success, it means that the request is rejected for outdated term
			rf.mu.Unlock()
			return
		}
		// DPrintf("Leader %v: before update, nextIndex[%v] = %v", rf.me, peer, rf.nextIndex[peer])
		// fail due to outdated log
		// decrement nextIndex and retry
		// optimize nextIndex using conflict info
		//  Case 1: leader doesn't have XTerm:
		//     nextIndex = XIndex
		//   Case 2: leader has XTerm:
		//     nextIndex = (index of leader's last entry for XTerm) + 1
		//   Case 3: follower's log is too short:
		//     nextIndex = XLen
		// DPrintf("Leader %v receive full reply: %+v", rf.me, reply)
		// DPrintf("Leader %v receive additional info: XTerm = %v, XIndex = %v, XLen = %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		if reply.XIndex == 0 { // follower's log is too short, so they dont provide xindex && xterm info
			// Case 3
			rf.nextIndex[peer] = reply.XLen
			// DPrintf("Leader %v: update nextIndex[%v](case 3) = %v", rf.me, peer, reply.XLen)
		} else {
			// Case 1 and 2
			found := false
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					rf.nextIndex[peer] = i + 1 + rf.firstLogIndex
					found = true
					// DPrintf("Leader %v: update nextIndex[%v](case 2) = %v", rf.me, peer, rf.nextIndex[peer])
					break
				}
			}
			if !found {
				// Case 1
				// DPrintf("Leader %v: update nextIndex[%v](case 1) = %v", rf.me, peer, rf.nextIndex[peer])
				rf.nextIndex[peer] = reply.XIndex
			}
		}
		// DPrintf("Leader %v: after update, nextIndex[%v] = %v", rf.me, peer, rf.nextIndex[peer])
		// original decrement
		// rf.nextIndex[peer] = max(start-1, 1)
		// start = rf.nextIndex[peer]

		rf.mu.Unlock()
		return
	} else {
		// AppendEntries success
		rf.mu.Lock()
		// nextIndex and matchIndex update: only advance
		rf.nextIndex[peer] = max(rf.nextIndex[peer], end+firstLogIndex) // in case firstlogIndex change
		newMatchIndex := prevLogIndex + len(args.Entries)
		if newMatchIndex >= rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatchIndex
			rf.advanceCommitIndex()
		}
		// DPrintf("Leader %v check Follower %v nextIndex = %v, matchIndex = %v, all matchIndex: %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer], rf.matchIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) advanceCommitIndex() {
	N := len(rf.matchIndex)
	sortedMatchIndex := make([]int, N)
	copy(sortedMatchIndex, rf.matchIndex)
	sort.Ints(sortedMatchIndex)

	midIndex := sortedMatchIndex[N/2]
	if midIndex > rf.commitIndex && rf.log[midIndex-rf.firstLogIndex].Term == rf.currentTerm {
		// DPrintf("Leader %v advance commitIndex from %v to %v, all matchIndex: %v", rf.me, rf.commitIndex, midIndex, rf.matchIndex)
		rf.commitIndex = midIndex
		rf.applyCond.Signal()

	}
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
					LastLogIndex: len(rf.log) - 1 + rf.firstLogIndex,
					LastLogTerm:  lastLog.Term,
				}
				reply := RequestVoteReply{}

				rf.mu.Unlock()
				// DPrintf("%v send RequestVote to %v with term %v", rf.me, peer, rf.currentTerm)

				rf.sendRequestVote(peer, &args, &reply)

				rf.mu.Lock()
				rf.checkTermChange(reply.Term)

				if rf.state == T_CANDIDATE && reply.VoteGranted && args.Term == rf.currentTerm {
					// DPrintf("%v got votes from %v with term %v", rf.me, peer, rf.currentTerm)
					myVotes++
					if myVotes > len(rf.peers)/2 { // receive majority votes
						rf.state = T_LEADER
						//reinit nextIndex and matchIndex
						for p := range rf.peers {
							rf.nextIndex[p] = len(rf.log) + rf.firstLogIndex
							rf.matchIndex[p] = 0
						}
						rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.firstLogIndex
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

func (rf *Raft) truncateLog(index int) {
	// truncate log entries before index (inclusive)
	if index < rf.firstLogIndex {
		return
	}
	realIndex := index - rf.firstLogIndex
	if realIndex >= len(rf.log) {
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{
			Command: nil,
			Term:    rf.snapshotLastIncludedTerm,
		}
	} else {
		newLen := len(rf.log) - realIndex
		newLog := make([]LogEntry, newLen)
		copy(newLog, rf.log[realIndex:])
		rf.log = newLog
	}
	rf.firstLogIndex = index
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
	// DPrintf("Server %v starts working...", rf.me)
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
	rf.resetHeartbeatTimer()

	rf.state = T_FOLLOWER
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.snapshot = nil
	rf.snapshotPending = false

	rf.firstLogIndex = 0
	rf.snapshotLastIncludedIndex = 0
	rf.snapshotLastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
