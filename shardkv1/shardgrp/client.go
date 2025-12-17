package shardgrp

import (
	"math/rand"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
	clientId int64 // for debug
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.clientId = rand.Int63()
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args, reply := rpc.GetArgs{Key: key}, rpc.GetReply{}
	defer func() {
		DPrintf("CLIENT %d Get with args %v and returns reply %+v", ck.clientId, args, reply)
	}()
	serverId := ck.leaderId
	for {
		// DPrintf("CLIENT %d try to Get key %s from server %d", ck.clientId, key, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			// time.Sleep(10 * time.Millisecond)
		} else {
			// DPrintf("CLIENT %d Get succeed with serverId %d", ck.clientId, serverId)
			ck.leaderId = serverId
			return reply.Value, reply.Version, reply.Err
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args, reply := rpc.PutArgs{Key: key, Value: value, Version: version}, rpc.PutReply{}
	defer func() {
		DPrintf("CLIENT %d Put with args %v and returns reply %+v", ck.clientId, args, reply)
	}()
	serverId := ck.leaderId
	ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}
	serverId = (serverId + 1) % len(ck.servers)
	for {
		DPrintf("CLIENT %d try to Put key %s from server %d", ck.clientId, key, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrVersion {
				// DPrintf("CLIENT %d receive err = rpc.ErrVersion after retry, so return err = rpc.ErrMaybe", ck.clientId)
				// DPrintf("CLIENT %d Put succeed with serverId %d", ck.clientId, serverId)
				ck.leaderId = serverId
				return rpc.ErrMaybe
			}
			if reply.Err != rpc.ErrWrongLeader {
				ck.leaderId = serverId
				return reply.Err
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		// time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here

	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	reply := shardrpc.FreezeShardReply{}
	defer func() {
		DPrintf("CLIENT %d FreezeShard with args %v and returns reply %+v", ck.clientId, args, reply)
	}()
	serverId := ck.leaderId
	for {
		// DPrintf("CLIENT %d try to Get key %s from server %d", ck.clientId, key, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.FreezeShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
		} else {
			return reply.State, reply.Err
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	reply := shardrpc.InstallShardReply{}
	DPrintf("CLIENT %d InstallShard with args %v", ck.clientId, args)

	defer func() {
		DPrintf("CLIENT %d InstallShard with args %v and returns reply %+v", ck.clientId, args, reply)
	}()
	serverId := ck.leaderId
	for {
		DPrintf("CLIENT %d InstallShard with args %v from serverId%v", ck.clientId, args, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.InstallShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			DPrintf("CLIENT %d InstallShard with args %v and fail with ok %v and  reply %+v", ck.clientId, args, ok, reply)

			serverId = (serverId + 1) % len(ck.servers)
		} else {
			return reply.Err
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	reply := shardrpc.DeleteShardReply{}
	defer func() {
		DPrintf("CLIENT %d DeleteShard with args %v and returns reply %+v", ck.clientId, args, reply)
	}()
	serverId := ck.leaderId
	for {
		// DPrintf("CLIENT %d try to Get key %s from server %d", ck.clientId, key, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.DeleteShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
		} else {
			return reply.Err
		}
	}
}
