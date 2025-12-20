package kvraft

import (
	"bytes"
	"log"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type DataValue struct {
	Version rpc.Tversion
	Value   string
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	// mu   sync.Mutex
	data map[string]DataValue

	// record map[int64]rpc.Err
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	DPrintf("SERVER %v now with map %+v", kv.me, kv.data)
	switch args := req.(type) {
	case rpc.GetArgs:
		// kv.mu.Lock()
		reply := rpc.GetReply{}
		dataValue, ok := kv.data[args.Key]
		if !ok {
			reply.Err = rpc.ErrNoKey
		} else {
			reply.Err = rpc.OK
			reply.Value = dataValue.Value
			reply.Version = dataValue.Version
		}
		// kv.mu.Unlock()
		DPrintf("SERVER %d DoOp Get %v and Reply %v", kv.me, args, reply)
		return reply
	case rpc.PutArgs:
		// kv.mu.Lock()
		reply := rpc.PutReply{}
		dataValue, ok := kv.data[args.Key]
		if !ok {
			if args.Version == 0 {
				reply.Err = rpc.OK
				kv.data[args.Key] = DataValue{
					Version: 1,
					Value:   args.Value,
				}
			} else {
				reply.Err = rpc.ErrNoKey
			}
		} else {
			if dataValue.Version != args.Version {
				reply.Err = rpc.ErrVersion
			} else {
				reply.Err = rpc.OK
				kv.data[args.Key] = DataValue{
					Version: args.Version + 1,
					Value:   args.Value,
				}
			}
		}
		// kv.record[args.Id] = reply.Err
		// kv.mu.Unlock()
		DPrintf("SERVER %d DoOp Put %v and Reply %v", kv.me, args, reply)
		return reply
	default:
		// wrong type! expecting an GetArgs or PutArgs.
		log.Fatalf("DoOp should execute only GetArgs/PutArgs and not %T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	DPrintf("SERVER %d: snapshot", kv.me)
	// return nil
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	DPrintf("SERVER %d: restore", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.data) != nil {
		DPrintf("%v couldn't decode counter", kv.me)

		log.Fatalf("%v couldn't decode counter", kv.me)
	}
	// log.Printf("after restore data: %+v", kv.data)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.PutReply)

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	if kv.data == nil {
		kv.data = make(map[string]DataValue)
	}
	// kv.record = make(map[int64]rpc.Err)
	return []tester.IService{kv, kv.rsm.Raft()}
}
