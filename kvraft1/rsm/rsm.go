package rsm

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const DEBUG = false

// const DEBUG = true

func DPrintf(format string, a ...interface{}) {
	if DEBUG {
		log.Printf(format, a...)
	}
}

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type LogState int

const (
	PENDING LogState = iota
	FULFILLED
	REJECTED
	NONEXIST // non-existed
)

type LogInfo struct {
	state  LogState
	cmdId  int
	result any
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	dead int32
	// mu is used to protect following fields
	logPending      map[int]LogInfo
	nextClientCmdId int
	// commitIndex     int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:              me,
		maxraftstate:    maxraftstate,
		applyCh:         make(chan raftapi.ApplyMsg),
		sm:              sm,
		dead:            0,
		logPending:      make(map[int]LogInfo),
		nextClientCmdId: 0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		DPrintf("rsm %v call sm.restore(snapshot) at start with snapshot len %v", rsm.me, len(snapshot))
		rsm.sm.Restore(snapshot)
	}

	go rsm.reader()
	// go rsm.snapshoter()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	opReq := Op{
		Me:  rsm.me,
		Id:  rsm.nextClientCmdId,
		Req: req,
	}
	rsm.nextClientCmdId++
	// rsm.mu.Unlock()

	idx, term0, isLeader0 := rsm.rf.Start(opReq)
	if !isLeader0 {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	rsm.logPending[idx] = LogInfo{
		state: PENDING,
		cmdId: opReq.Id,
	}
	rsm.mu.Unlock()
	DPrintf("server %v receive cmd with idx %v from server", rsm.me, idx)
	// rsm.addPendingLog(idx, opReq)
	for {
		if rsm.killed() {
			return rpc.ErrWrongLeader, nil
		}
		term1, isLeader1 := rsm.rf.GetState()
		if term1 != term0 || !isLeader1 {
			return rpc.ErrWrongLeader, nil // i'm dead, try another server.
		}
		// check if req at index idx is submitted
		logState, result := rsm.checkLogAtIndex(idx)
		switch logState {
		case REJECTED:
			rsm.removeLogAtIndex(idx)
			fallthrough
		case NONEXIST:
			return rpc.ErrWrongLeader, nil
		case FULFILLED:
			rsm.removeLogAtIndex(idx)
			// result := rsm.sm.DoOp(req)
			return rpc.OK, result
		}
		time.Sleep(20 * time.Millisecond)
	}

}

func (rsm *RSM) checkLogAtIndex(index int) (LogState, any) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	logInfo, ok := rsm.logPending[index]
	if !ok {
		return NONEXIST, nil
	}
	return logInfo.state, logInfo.result
}

func (rsm *RSM) removeLogAtIndex(index int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	delete(rsm.logPending, index)
}

func (rsm *RSM) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

func (rsm *RSM) reader() {
	for !rsm.killed() {
		applyMsg, ok := <-rsm.applyCh
		if !ok { // rsm.rf is killed
			atomic.StoreInt32(&rsm.dead, 1)
			break
		}
		DPrintf("reader %v read message  %+v from channel", rsm.me, applyMsg)
		if applyMsg.CommandValid { // command return
			idx, cmd := applyMsg.CommandIndex, applyMsg.Command
			var result any
			cmdOp, ok0 := cmd.(Op)
			if ok0 {
				result = rsm.sm.DoOp(cmdOp.Req)
			} else {
				log.Fatalf("server %v find cmd without cmd.(Op)", rsm.me)
			}
			rsm.mu.Lock()
			// rsm.commitIndex = max(idx, rsm.commitIndex)
			logInfo, ok1 := rsm.logPending[idx]
			if ok1 {
				if cmdOp.Me == rsm.me && cmdOp.Id == logInfo.cmdId {
					rsm.logPending[idx] = LogInfo{
						state:  FULFILLED,
						cmdId:  logInfo.cmdId,
						result: result,
					}
					DPrintf("server %v change %v' state to fullfilled", rsm.me, idx)
				} else {
					rsm.logPending[idx] = LogInfo{
						state: REJECTED,
						cmdId: logInfo.cmdId,
					}
					DPrintf("server %v change %v' state to rejected", rsm.me, idx)
				}
			} else {
				DPrintf("server %v reject to process cmd with index %v", rsm.me, idx)
			}
			rsm.mu.Unlock()
			if rsm.maxraftstate != -1 && rsm.maxraftstate <= rsm.rf.PersistBytes() {
				DPrintf("rsm %v call sm.snapshot(snapshot) when snapshotsize > maxraftstate with snapshot len %v", rsm.me, rsm.rf.PersistBytes())

				rsm.rf.Snapshot(idx, rsm.sm.Snapshot())
			}
		}
		if applyMsg.SnapshotValid {
			DPrintf("rsm %v call sm.restore(snapshot) when receiving snapshot with snapshot len %v", rsm.me, len(applyMsg.Snapshot))

			rsm.sm.Restore(applyMsg.Snapshot)
		}
	}
}
