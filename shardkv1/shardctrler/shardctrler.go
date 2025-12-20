package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// const Debug = true

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const CONFIGURATION_KEY = "config_key"
const NEXT_CONFIG_KEY = "next_config_key"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// check my config
	curr, version0, err0 := sck.IKVClerk.Get(CONFIGURATION_KEY)
	next, _, err1 := sck.IKVClerk.Get(NEXT_CONFIG_KEY)

	// if err0 == rpc.ErrNoKey || err1 == rpc.ErrNoKey {
	// 	return
	// }
	if err0 == rpc.ErrNoKey || err1 == rpc.ErrNoKey {
		// should not happen
		log.Fatalf("nextCfg or oldCfg not exist, which should not happen")
	}
	currCfg, nextCfg := shardcfg.FromString(curr), shardcfg.FromString(next)

	// check if nextCfg have a higher Num
	if nextCfg.Num > currCfg.Num {
		// sck.ChangeConfigTo(nextCfg)
		sck.migrateTo(currCfg, nextCfg, version0)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	DPrintf("CTRL init config: %s", cfg.String())
	sck.IKVClerk.Put(CONFIGURATION_KEY, cfg.String(), 0)
	sck.IKVClerk.Put(NEXT_CONFIG_KEY, cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.

	for {
		oldCfg, version0, err0 := sck.IKVClerk.Get(CONFIGURATION_KEY)
		nextCfg, version1, err1 := sck.IKVClerk.Get(NEXT_CONFIG_KEY)

		if err0 == rpc.ErrNoKey || err1 == rpc.ErrNoKey {
			// should not happen
			log.Fatalf("nextCfg or oldCfg not exist, which should not happen")
		}

		next, old := shardcfg.FromString(nextCfg), shardcfg.FromString(oldCfg)

		if new.Num <= next.Num {
			// already a config with a Num >= new.Num obtain next key, fail
			return
		}
		// new.Num > nextCfg.Num
		if nextCfg == oldCfg { // next config finish change
			// try to obtain nextkey like lock lab
			err2 := sck.IKVClerk.Put(NEXT_CONFIG_KEY, new.String(), version1)
			switch err2 {
			case rpc.OK:
				// obtain lock successfully
				sck.migrateTo(old, new, version0)
			case rpc.ErrMaybe:
				cfg, _, err3 := sck.IKVClerk.Get(NEXT_CONFIG_KEY)
				if err3 == rpc.ErrNoKey {
					log.Fatalf("nextCfg or oldCfg not exist, which should not happen")
				}
				if cfg == new.String() {
					// obtain lock success
					sck.migrateTo(old, new, version0)
				}
			}

		} else { // wait for next config change finish
			time.Sleep(10 * time.Millisecond)
		}

	}

	// next, version0, err0 := sck.IKVClerk.Get(NEXT_CONFIG_KEY)

	// if err0 == rpc.ErrNoKey {
	// 	// version0 = 0
	// 	log.Fatalf("nextCfg or oldCfg not exist, which should not happen")
	// } else {
	// 	nextCfg := shardcfg.FromString(next)
	// 	if nextCfg.Num >= new.Num && next != new.String() {
	// 		// there's already a config with a higher num obtain nextconfigkey
	// 		return
	// 	}
	// }

	// err1 := sck.IKVClerk.Put(NEXT_CONFIG_KEY, new.String(), version0)
	// _ = err1
	// switch err1 {

	// case rpc.ErrMaybe:
	// 	next, _, _ := sck.IKVClerk.Get(NEXT_CONFIG_KEY)
	// 	if next != new.String() {
	// 		return
	// 	}
	// case rpc.ErrVersion:
	// 	return
	// }

	// cfgstr, version, _ := sck.IKVClerk.Get(CONFIGURATION_KEY)
	// old := shardcfg.FromString(cfgstr)
	// sck.migrateTo(old, new, version)

	DPrintf("CTRL change config successfully %v", new.Num)
}

func (sck *ShardCtrler) migrateTo(old *shardcfg.ShardConfig, new *shardcfg.ShardConfig, version rpc.Tversion) {
	DPrintf("CTRL change config from %+v to %+v", old, new)
	var wg sync.WaitGroup
	for shard0 := range shardcfg.NShards {
		shard := shardcfg.Tshid(shard0)
		wg.Add(1)

		go func(shard shardcfg.Tshid, old *shardcfg.ShardConfig, new *shardcfg.ShardConfig) {
			defer wg.Done()
			_, srvs0, _ := old.GidServers(shard)
			_, srvs1, _ := new.GidServers(shard)

			oldCrk := shardgrp.MakeClerk(sck.clnt, srvs0)
			newCrk := shardgrp.MakeClerk(sck.clnt, srvs1)

			state, err := oldCrk.FreezeShard(shard, new.Num)
			if err == rpc.ErrVersion || err == rpc.ErrNoKey {
				return
			}
			err = newCrk.InstallShard(shard, state, new.Num)
			if err == rpc.ErrVersion {
				return
			}
			oldCrk.DeleteShard(shard, new.Num)
		}(shard, old, new)
	}
	wg.Wait()

	sck.IKVClerk.Put(CONFIGURATION_KEY, new.String(), version)
	DPrintf("CTRL change config successfully %v", new.Num)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfg, _, _ := sck.IKVClerk.Get(CONFIGURATION_KEY)
	return shardcfg.FromString(cfg)
}
