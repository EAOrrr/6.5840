package shardgrp

import (
	"bytes"
	"log"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type DataValue struct {
	Version rpc.Tversion
	Value   string
}

type ShardData struct {
	Data   map[string]DataValue
	Frozen bool
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	shardInfo          map[shardcfg.Tshid]ShardData
	shardMaxConfigNume map[shardcfg.Tshid]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// DPrintf("SERVER %v now with map %+v", kv.me, kv.shardInfo)
	switch args := req.(type) {
	case rpc.GetArgs:
		key := args.Key
		shard := shardcfg.Key2Shard(key)
		shardInfo, ok0 := kv.shardInfo[shard]
		if !ok0 {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		dataValue, ok1 := shardInfo.Data[args.Key]
		if !ok1 {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		} else {
			return rpc.GetReply{
				Value:   dataValue.Value,
				Version: dataValue.Version,
				Err:     rpc.OK,
			}
		}
	case rpc.PutArgs:
		key := args.Key
		shard := shardcfg.Key2Shard(key)
		shardInfo, ok0 := kv.shardInfo[shard]
		if !ok0 {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		if shardInfo.Frozen {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		dataValue, ok := shardInfo.Data[args.Key]
		if !ok {
			if args.Version == 0 {
				kv.shardInfo[shard].Data[key] = DataValue{
					Version: 1,
					Value:   args.Value,
				}
				return rpc.PutReply{Err: rpc.OK}
			} else {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
		} else {
			if dataValue.Version != args.Version {
				return rpc.PutReply{Err: rpc.ErrVersion}
			} else {
				kv.shardInfo[shard].Data[key] = DataValue{
					Version: args.Version + 1,
					Value:   args.Value,
				}
				return rpc.PutReply{Err: rpc.OK}
			}
		}
	case shardrpc.FreezeShardArgs:
		maxConfigNum, ok0 := kv.shardMaxConfigNume[args.Shard]
		if ok0 && args.Num < maxConfigNum {
			DPrintf("SERVER %v freezeshard fail due to wrong version number", kv.me)
			return shardrpc.FreezeShardReply{Err: rpc.ErrVersion}
		} else {
			kv.shardMaxConfigNume[args.Shard] = args.Num
		}
		shardInfo, ok1 := kv.shardInfo[args.Shard]
		if ok1 {
			kv.shardInfo[args.Shard] = ShardData{
				Data:   shardInfo.Data,
				Frozen: true,
			}
		}
		DPrintf("SERVER %v freeze shard %v success with shard = %+v", kv.me, args.Shard, kv.shardInfo[args.Shard])
		shardState := EncodeShardState(&shardInfo.Data)
		return shardrpc.FreezeShardReply{
			State: shardState,
			Num:   kv.shardMaxConfigNume[args.Shard],
			Err:   rpc.OK,
		}
		// } else {
		// 	return shardrpc.FreezeShardReply{Err: rpc.ErrNoKey}
		// }
	case shardrpc.InstallShardArgs:
		maxConfigNum, ok0 := kv.shardMaxConfigNume[args.Shard]
		if ok0 && args.Num < maxConfigNum {
			DPrintf("SERVER %v installshard fail due to wrong version number", kv.me)
			return shardrpc.InstallShardReply{Err: rpc.ErrVersion}
		} else {
			kv.shardMaxConfigNume[args.Shard] = args.Num
		}
		var shardData map[string]DataValue
		DecodeShardState(args.State, &shardData)
		if shardData == nil {
			shardData = make(map[string]DataValue)
		}
		kv.shardInfo[args.Shard] = ShardData{
			Data:   shardData,
			Frozen: false,
		}
		DPrintf("SERVER %v install shard %v success with shard = %+v", kv.me, args.Shard, kv.shardInfo[args.Shard])

		return shardrpc.InstallShardReply{Err: rpc.OK}
	case shardrpc.DeleteShardArgs:
		maxConfigNum, ok0 := kv.shardMaxConfigNume[args.Shard]
		if ok0 && args.Num < maxConfigNum {
			DPrintf("SERVER %v deleteshard fail due to wrong version number", kv.me)
			return shardrpc.DeleteShardReply{Err: rpc.ErrVersion}
		} else {
			kv.shardMaxConfigNume[args.Shard] = args.Num
		}
		delete(kv.shardInfo, args.Shard)
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	default:
		// wrong type! expecting an GetArgs or PutArgs.
		log.Fatalf("DoOp should not execute %T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	DPrintf("SERVER %d: snapshot", kv.me)
	// return nil
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardInfo)
	e.Encode(kv.shardMaxConfigNume)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	DPrintf("SERVER %d: restore", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.shardInfo) != nil ||
		d.Decode(&kv.shardMaxConfigNume) != nil {
		log.Fatalf("%v couldn't decode counter", kv.me)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
	DPrintf("SERVER %v Get return %+v", kv.me, reply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	// Your code here
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.shardInfo = make(map[shardcfg.Tshid]ShardData)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// Your code here
	kv.shardMaxConfigNume = make(map[shardcfg.Tshid]shardcfg.Tnum)
	return []tester.IService{kv, kv.rsm.Raft()}
}
