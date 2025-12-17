package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"

	kvsrv "6.5840/kvsrv1"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const CONFIG_KEY = "configuration_key"
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// ShardCtrler for the controller and kv clerk.
type ShardGrpCrk struct {
	ck *shardgrp.Clerk
	mu *sync.Mutex
}

type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	gid2clerk map[tester.Tgid]ShardGrpCrk
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
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	DPrintf("InitConfig called with initialcfg: %+v", cfg)
	sck.IKVClerk.Put(CONFIG_KEY, cfg.String(), 0)

	sck.gid2clerk = make(map[tester.Tgid]ShardGrpCrk)
	for gid, srvs := range cfg.Groups {
		ck := shardgrp.MakeClerk(sck.clnt, srvs)
		sck.gid2clerk[gid] = ShardGrpCrk{
			ck: ck,
			mu: &sync.Mutex{},
		}
	}
	DPrintf("init gid2clerk: %+v", sck.gid2clerk)
	encodedInitMap := shardgrp.EncodeEmptyState()
	var wg sync.WaitGroup
	for shard, gid := range cfg.Shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sck.gid2clerk[gid].mu.Lock()
			sck.gid2clerk[gid].ck.InstallShard(shardcfg.Tshid(shard), encodedInitMap, cfg.Num)
			sck.gid2clerk[gid].mu.Unlock()
		}()
	}
	wg.Wait()
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	// old := sck.Query()
	DPrintf("CTRL: changeconfigto called")
	config, version, _ := sck.IKVClerk.Get(CONFIG_KEY)
	old := shardcfg.FromString(config)

	DPrintf("old config: %+v", old)
	DPrintf("new config: %+v", new)
	var wg sync.WaitGroup
	for gid, srvs := range new.Groups {
		if _, ok := sck.gid2clerk[gid]; !ok {
			sck.gid2clerk[gid] = ShardGrpCrk{
				ck: shardgrp.MakeClerk(sck.clnt, srvs),
				mu: &sync.Mutex{},
			}
			DPrintf("new clk %v created", gid)
		}
	}
	for shard0 := range old.Shards {
		// wg.Add(1)
		shard := shardcfg.Tshid(shard0)
		// go func(shard shardcfg.Tshid, old *shardcfg.ShardConfig, new *shardcfg.ShardConfig) {
		// 	defer wg.Done()
		num := new.Num
		gid0, _, ok0 := old.GidServers(shard)
		gid1, _, ok1 := new.GidServers(shard)

		data := shardgrp.EncodeEmptyState()
		if ok0 {
			sck.gid2clerk[gid0].mu.Lock()
			data, _ = sck.gid2clerk[gid0].ck.FreezeShard(shard, num)
			sck.gid2clerk[gid0].mu.Unlock()
		}
		if ok1 {
			sck.gid2clerk[gid1].mu.Lock()
			sck.gid2clerk[gid1].ck.InstallShard(shard, data, num)
			sck.gid2clerk[gid1].mu.Unlock()
		}
		DPrintf("shard %v old:%v ok0:%v, new:%v ok1:%v", shard, gid0, ok0, gid1, ok1)

		// }(shard, old, new)
	}

	// wg.Wait()

	for shard0 := range old.Shards {
		// wg.Add(1)
		shard := shardcfg.Tshid(shard0)
		// go func(shard shardcfg.Tshid, old *shardcfg.ShardConfig, new *shardcfg.ShardConfig) {
		// 	defer wg.Done()
		num := new.Num
		gid0, _, ok0 := old.GidServers(shard)

		if ok0 {
			sck.gid2clerk[gid0].mu.Lock()
			sck.gid2clerk[gid0].ck.DeleteShard(shard, num)
			sck.gid2clerk[gid0].mu.Unlock()

		}

		// }(shard, old, new)
	}
	wg.Wait()
	sck.IKVClerk.Put(CONFIG_KEY, new.String(), version)
	DPrintf("CTRL: changeconfigto sent to remote")
	DPrintf("CTRL: change config success")
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	config, _, _ := sck.IKVClerk.Get(CONFIG_KEY)
	return shardcfg.FromString(config)
}
