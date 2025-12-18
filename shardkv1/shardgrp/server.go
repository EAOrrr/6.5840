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
	shardData         map[shardcfg.Tshid]ShardData
	shardMaxConfigNum map[shardcfg.Tshid]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	DPrintf("SERVER gid %v id %v now receive req %v, DOOP BEGIN", kv.gid, kv.me, req)
	defer DPrintf("SERVER gid %v id %v now receive req %v, DOOP END", kv.gid, kv.me, req)
	switch args := req.(type) {
	case rpc.GetArgs:
		DPrintf("SERVER gid %v id %v receive GetArgs: %+v", kv.gid, kv.me, args)
		key := args.Key
		shard := shardcfg.Key2Shard(key)
		shardInfo, ok0 := kv.shardData[shard]
		if !ok0 {
			DPrintf("SERVER gid %v id %v reject get for wronggroup ", kv.gid, kv.me)
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		dataValue, ok1 := shardInfo.Data[key]
		if !ok1 {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		} else {
			DPrintf("SERVER gid %v id %v shard %d info %+v", kv.gid, kv.me, shard, shardInfo)
			return rpc.GetReply{
				Err:     rpc.OK,
				Value:   dataValue.Value,
				Version: dataValue.Version,
			}
		}
	case rpc.PutArgs:
		DPrintf("SERVER gid %v id %v receive PutArgs: %+v", kv.gid, kv.me, args)
		key := args.Key
		shard := shardcfg.Key2Shard(key)
		shardInfo, ok0 := kv.shardData[shard]
		if !ok0 || shardInfo.Frozen {
			DPrintf("SERVER gid %v id %v reject put for wronggroup ok: %v, frozen: %v", kv.gid, kv.me, ok0, shardInfo.Frozen)
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		dataValue, ok1 := shardInfo.Data[key]
		if !ok1 {
			if args.Version == 0 {
				kv.shardData[shard].Data[key] = DataValue{
					Version: 1,
					Value:   args.Value,
				}
				DPrintf("SERVER gid %v id %v shard %d info %+v", kv.gid, kv.me, shard, kv.shardData[shard])
				return rpc.PutReply{Err: rpc.OK}
			} else {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
		} else {
			DPrintf("SERVER gid %v id %v shard %d info %+v so rpc.ERRV", kv.gid, kv.me, shard, shardInfo)

			if dataValue.Version != args.Version {
				return rpc.PutReply{Err: rpc.ErrVersion}
			} else {
				kv.shardData[shard].Data[key] = DataValue{
					Version: args.Version + 1,
					Value:   args.Value,
				}
				DPrintf("SERVER gid %v id %v shard %d info %+v so rpc.ok", kv.gid, kv.me, shard, kv.shardData[shard])

				return rpc.PutReply{Err: rpc.OK}
			}
		}
	case shardrpc.FreezeShardArgs:
		DPrintf("SERVER gid %v id %v receive FreezeShardArgs: %+v", kv.gid, kv.me, args)
		shard := args.Shard
		myConfigNum := kv.shardMaxConfigNum[shard]
		if args.Num < myConfigNum {
			return shardrpc.FreezeShardReply{
				Err: rpc.ErrVersion,
				Num: kv.shardMaxConfigNum[shard],
			}
		} else {
			kv.shardMaxConfigNum[shard] = args.Num
		}
		shardInfo, ok := kv.shardData[shard]
		if ok {
			if args.Num > myConfigNum {
				kv.shardData[shard] = ShardData{
					Data:   shardInfo.Data,
					Frozen: true,
				}
			}
			state := EncodeShardState(&shardInfo.Data)
			DPrintf("SERVER gid %v id %v after freezeshard %v data: %+v", kv.gid, kv.me, shard, kv.shardData)

			return shardrpc.FreezeShardReply{
				Err:   rpc.OK,
				Num:   kv.shardMaxConfigNum[shard],
				State: state,
			}

		} else {
			return shardrpc.FreezeShardReply{
				Err: rpc.ErrVersion,
				Num: kv.shardMaxConfigNum[shard],
			}
		}
	case shardrpc.InstallShardArgs:
		DPrintf("SERVER gid %v id %v receive InstallShardArgs: %+v", kv.gid, kv.me, args)

		shard := args.Shard
		if args.Num < kv.shardMaxConfigNum[shard] {
			return shardrpc.InstallShardReply{
				Err: rpc.ErrVersion,
			}
		} else {
			kv.shardMaxConfigNum[shard] = args.Num
		}
		var shardState map[string]DataValue
		DecodeShardState(args.State, &shardState)
		shardInfo, ok := kv.shardData[shard]
		if !ok || shardInfo.Frozen {
			kv.shardData[shard] = ShardData{
				Data:   shardState,
				Frozen: false,
			}
		}
		DPrintf("SERVER gid %v id %v after installshard %v data: %+v", kv.gid, kv.me, shard, kv.shardData)

		return shardrpc.InstallShardReply{
			Err: rpc.OK,
		}
	case shardrpc.DeleteShardArgs:
		DPrintf("SERVER gid %v id %v receive DeleteShardArgs: %+v", kv.gid, kv.me, args)

		shard := args.Shard
		if args.Num < kv.shardMaxConfigNum[shard] {
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrVersion,
			}
		} else {
			kv.shardMaxConfigNum[shard] = args.Num
		}
		if shardInfo, ok := kv.shardData[shard]; ok {
			if shardInfo.Frozen {
				delete(kv.shardData, shard)
			}
		}
		DPrintf("SERVER gid %v id %v after deleteshard %v data: %+v", kv.gid, kv.me, shard, kv.shardData)
		return shardrpc.DeleteShardReply{
			Err: rpc.OK,
		}
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
	e.Encode(kv.shardData)
	e.Encode(kv.shardMaxConfigNum)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	DPrintf("SERVER %d: restore", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.shardData) != nil ||
		d.Decode(&kv.shardMaxConfigNum) != nil {
		log.Fatalf("%v couldn't decode restore data", kv.me)
	}
	// log.Printf("after restore data: %+v", kv.data)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
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
	kv.shardData = make(map[shardcfg.Tshid]ShardData)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	// init shards entry for first shardgrp with gid = shardcfg.Gid1
	if gid == shardcfg.Gid1 {
		for shard0 := range shardcfg.NShards {
			shard := shardcfg.Tshid(shard0)
			kv.shardData[shard] = ShardData{
				Data:   make(map[string]DataValue),
				Frozen: false,
			}
		}
	}
	kv.shardMaxConfigNum = make(map[shardcfg.Tshid]shardcfg.Tnum)

	return []tester.IService{kv, kv.rsm.Raft()}
}
