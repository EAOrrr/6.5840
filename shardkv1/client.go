package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const Debug = false

// const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
	// gid2clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()
		// DPrintf("SHARDKV config %+v", cfg.String())
		_, srvs, _ := cfg.GidServers(shard)
		clerk := shardgrp.MakeClerk(ck.clnt, srvs)
		value, version, err := clerk.Get(key)
		if err != rpc.ErrWrongGroup {
			return value, version, err
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	first := true
	var prevGid tester.Tgid = -1
	for {
		shard := shardcfg.Key2Shard(key)
		cfg := ck.sck.Query()

		gid, srvs, _ := cfg.GidServers(shard)
		if gid == prevGid {
			first = false
		}
		clerk := shardgrp.MakeClerk(ck.clnt, srvs)
		err := clerk.Put(key, value, version)

		if err != rpc.ErrWrongGroup {
			if err == rpc.ErrVersion && !first {
				return rpc.ErrMaybe
			}
			return err
		}
		prevGid = gid
	}
}
