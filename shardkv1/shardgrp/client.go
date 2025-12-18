package shardgrp

import (
	"math/rand"
	"time"

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
	clientId int64
}

const SLEEP_INTERVAL = 5

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leaderId = 0
	ck.clientId = rand.Int63()
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args, reply := rpc.GetArgs{Key: key}, rpc.GetReply{}
	defer func() {
		DPrintf("CLIENT %d Get command with key %s and returns reply %+v", ck.clientId, key, reply)
	}()
	retry := 0
	serverId := ck.leaderId
	DPrintf("CLIENT %d try to Get key %s from server %d", ck.clientId, key, serverId)

	for {
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leaderId = serverId
			return reply.Value, reply.Version, reply.Err
		} else {
			// DPrintf("CLIENT %d Get succeed with serverId %d", ck.clientId, serverId)
			serverId = (serverId + 1) % len(ck.servers)
			if !ok {
				retry++
				if retry > 5 {
					return "", 0, rpc.ErrWrongGroup
				}
			}
			time.Sleep(SLEEP_INTERVAL * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args, reply := rpc.PutArgs{Key: key, Value: value, Version: version}, rpc.PutReply{}
	serverId := ck.leaderId
	retry := 0

	defer func() {
		DPrintf("CLIENT %d Put command with key %s and returns reply %+v", ck.clientId, key, reply)
	}()
	// send first time
	DPrintf("CLIENT %d try to Put key %s from server %d", ck.clientId, key, serverId)

	ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}
	serverId = (serverId + 1) % len(ck.servers)
	for {
		// DPrintf("CLIENT %d try to Put key %s from server %d", ck.clientId, key, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrVersion {
				// DPrintf("CLIENT %d receive err = rpc.ErrVersion after retry, so return err = rpc.ErrMaybe", ck.clientId)
				// DPrintf("CLIENT %d Put succeed with serverId %d", ck.clientId, serverId)

				ck.leaderId = serverId
				return rpc.ErrMaybe
			}
			if reply.Err != rpc.ErrWrongLeader {
				// DPrintf("CLIENT %d Put succeed with serverId %d", ck.clientId, serverId)
				ck.leaderId = serverId
				return reply.Err
			}
		} else {
			retry++
			if retry > 5 {
				return rpc.ErrWrongGroup
			}
		}
		DPrintf("client %v Put retry, ok %v reply.Err %v", ck.clientId, ok, reply.Err)
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(SLEEP_INTERVAL * time.Millisecond)

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
		DPrintf("CLIENT %d FreezeShard command with args %+v and returns reply %+v", ck.clientId, args, reply)
	}()

	serverId := ck.leaderId
	DPrintf("CLIENT %d try to FreezeShard %+v from server %d", ck.clientId, args, serverId)

	for {
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.FreezeShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			time.Sleep(SLEEP_INTERVAL * time.Millisecond)

		} else {
			DPrintf("CLIENT %d Get succeed with serverId %d", ck.clientId, serverId)
			ck.leaderId = serverId
			return reply.State, reply.Err
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		Shard: s,
		Num:   num,
		State: state,
	}
	reply := shardrpc.InstallShardReply{}
	defer func() {
		DPrintf("CLIENT %d InstallShard command with args %+v and returns reply %+v", ck.clientId, args, reply)
	}()

	serverId := ck.leaderId
	DPrintf("CLIENT %d try to InstallShard %+v from server %d", ck.clientId, args, serverId)

	for {
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.InstallShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			time.Sleep(SLEEP_INTERVAL * time.Millisecond)

		} else {
			DPrintf("CLIENT %d InstallShard succeed with serverId %d", ck.clientId, serverId)
			ck.leaderId = serverId
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
		DPrintf("CLIENT %d DeleteShard command with args %+v and returns reply %+v", ck.clientId, args, reply)
	}()

	serverId := ck.leaderId
	DPrintf("CLIENT %d try to DeleteShard %+v from server %d", ck.clientId, args, serverId)

	for {
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.DeleteShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			time.Sleep(SLEEP_INTERVAL * time.Millisecond)
			serverId = (serverId + 1) % len(ck.servers)
		} else {
			DPrintf("CLIENT %d DeleteShard succeed with serverId %d", ck.clientId, serverId)
			ck.leaderId = serverId
			return reply.Err
		}
	}
}
