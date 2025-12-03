package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

// const Debug = true
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type DataValue struct {
	version rpc.Tversion
	value   string
}
type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data map[string]DataValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.data = make(map[string]DataValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dataValue, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Err = rpc.OK
		reply.Value = dataValue.value
		reply.Version = dataValue.version
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dataValue, ok := kv.data[args.Key]
	if !ok {
		if args.Version == 0 {
			DPrintf("SUCCESS: receive PUT request with key = %v, version = %v for key nonexist", args.Key, args.Version)

			reply.Err = rpc.OK
			kv.data[args.Key] = DataValue{
				version: 1,
				value:   args.Value,
			}
		} else {
			DPrintf("FAILED: receive PUT request with key = %v, version = %v for key nonexist", args.Key, args.Version)

			reply.Err = rpc.ErrNoKey
		}
	} else {
		if dataValue.version != args.Version {
			DPrintf("FAILED: receive PUT request with key = %v, version = %v for key now with version = %v", args.Key, args.Version, dataValue.version)

			reply.Err = rpc.ErrVersion
		} else {
			DPrintf("SUCCEE: receive PUT request with key = %v, version = %v for key now with version = %v", args.Key, args.Version, dataValue.version)

			reply.Err = rpc.OK
			kv.data[args.Key] = DataValue{
				version: args.Version + 1,
				value:   args.Value,
			}
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
