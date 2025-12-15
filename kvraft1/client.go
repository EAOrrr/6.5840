package kvraft

import (
	"math/rand"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
	cmdId    int
	clientId int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leaderId = 0
	ck.cmdId = 0
	ck.clientId = rand.Int63()
	DPrintf("CLIENT %d starts", ck.clientId)
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	// return "", 0, ""
	args, reply := rpc.GetArgs{Key: key}, rpc.GetReply{}
	retry := 0
	DPrintf("CLIENT %d receive Get command with key %s and cmdId = %d", ck.clientId, key, ck.cmdId)
	ck.cmdId++
	defer func(reply_ rpc.GetReply, retry_ int) {
		DPrintf("CLIENT %d Get command with key %s and cmdId = %d returns reply %v with retry count %d", ck.clientId, key, ck.cmdId, reply_, retry_)
	}(reply, retry)

	serverId := ck.leaderId
	for {
		DPrintf("CLIENT %d try to Get key %s from server %d", ck.clientId, key, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			retry++
			// time.Sleep(10 * time.Millisecond)
		} else {
			DPrintf("CLIENT %d Get succeed with serverId %d", ck.clientId, serverId)
			ck.leaderId = serverId
			return reply.Value, reply.Version, reply.Err
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	// return ""
	args, reply := rpc.PutArgs{Key: key, Value: value, Version: version}, rpc.PutReply{}
	serverId := ck.leaderId

	retry := 0
	DPrintf("CLIENT %d receive Put command with key %s and cmdId = %d", ck.clientId, key, ck.cmdId)
	ck.cmdId++
	defer func(reply_ rpc.PutReply, retry_ int) {
		DPrintf("CLIENT %d Put command with key %s and cmdId = %d returns reply %v with retry count %d", ck.clientId, key, ck.cmdId, reply_, retry_)
	}(reply, retry)
	// send first time
	DPrintf("CLIENT %d try to Put key %s from server %d", ck.clientId, key, serverId)

	ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}
	serverId = (serverId + 1) % len(ck.servers)
	retry++
	for {
		DPrintf("CLIENT %d try to Put key %s from server %d", ck.clientId, key, serverId)
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrVersion {
				DPrintf("CLIENT %d receive err = rpc.ErrVersion after retry, so return err = rpc.ErrMaybe", ck.clientId)
				DPrintf("CLIENT %d Put succeed with serverId %d", ck.clientId, serverId)

				ck.leaderId = serverId
				return rpc.ErrMaybe
			}
			if reply.Err != rpc.ErrWrongLeader {
				DPrintf("CLIENT %d Put succeed with serverId %d", ck.clientId, serverId)

				ck.leaderId = serverId
				return reply.Err
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		// time.Sleep(10 * time.Millisecond)
		retry++
	}
}
