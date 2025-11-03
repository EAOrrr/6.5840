package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const SLEEP_INTERVAL = time.Second
const DIR_PATH = "./mr-tmp/"

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	stop := false
	for !stop {
		askArgs, askReply := AskTaskArgs{}, AskTaskReply{}
		CallAskTask(&askArgs, &askReply)
		switch askReply.TaskType {
		case TT_IDLE:
			time.Sleep(SLEEP_INTERVAL)
		case TT_EXIT:
			stop = true
		case TT_MAP:
			taskId := askReply.TaskId
			doMapTask(askReply, mapf)
			CallFinishTask(TT_MAP, taskId)
		case TT_REDUCE:
			taskId := askReply.TaskId
			doReduceTask(askReply, reducef)
			CallFinishTask(TT_REDUCE, taskId)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(reply AskTaskReply, mapf func(string, string) []KeyValue) {
	filename := reply.Filename
	nReduce := reply.NReduce
	taskId := reply.TaskId

	// read input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	file.Close()
	kva := mapf(filename, string(content))
	// sort.Sort(ByKey(kva))
	buckets := make(map[int]([]KeyValue))
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}
	for reduceId := range nReduce {
		// sort.Sort(ByKey(buckets[reduceId]))
		oname := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(reduceId)

		oTmpFile, err := os.CreateTemp(".", oname+"*.tmp")
		if err != nil {
			log.Fatalf("cannot create temp file for %v", oname)
		}
		enc := json.NewEncoder(oTmpFile)
		for _, kv := range buckets[reduceId] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode kv %v", kv)
			}
		}

		if err := oTmpFile.Sync(); err != nil {
			log.Fatalf("cannot sync temp file for %v", oname)
		}

		if err := oTmpFile.Close(); err != nil {
			log.Fatalf("cannot close temp file for %v", oname)
		}

		if err := os.Rename(oTmpFile.Name(), oname); err != nil {
			log.Fatalf("cannot rename temp file for %v", oname)
		}

	}

}

func doReduceTask(reply AskTaskReply, reducef func(string, []string) string) {
	nMap := reply.NMap

	intermediate := []KeyValue{}
	for mapId := range nMap {
		iname := "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reply.TaskId)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}

		dec := json.NewDecoder(file)
		var kva []KeyValue
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.TaskId)
	oTmpFile, err := os.CreateTemp(".", oname+"*.tmp")
	if err != nil {
		log.Fatalf("cannot create temp file for %v", oname)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(oTmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	if err := oTmpFile.Sync(); err != nil {
		log.Fatalf("cannot sync temp file for %v", oname)
	}

	if err := oTmpFile.Close(); err != nil {
		log.Fatalf("cannot close temp file for %v", oname)
	}

	if err := os.Rename(oTmpFile.Name(), oname); err != nil {
		log.Fatalf("cannot rename temp file for %v", oname)
	}

}

func CallAskTask(args *AskTaskArgs, reply *AskTaskReply) {

	call("Coordinator.AskTask", args, reply)
	// ok := call("Coordinator.AskTask", args, reply)

	// if ok {
	// 	fmt.Printf("reply.taskType %v\n", reply.TaskType)
	// } else {
	// 	fmt.Printf("call failed!\n")
	// }
}

func CallFinishTask(taskType TaskType, taskId int) {
	args, reply := FinishTaskArgs{}, FinishTaskReply{}
	args.TaskType = taskType
	args.TaskId = taskId
	call("Coordinator.FinishTask", &args, &reply)
	// ok := call("Coordinator.FinishTask", &args, &reply)

	// if ok {
	// 	fmt.Printf("Finished task %v of type %v\n", args.TaskId, args.TaskType)
	// } else {
	// 	fmt.Printf("call failed!\n")
	// }
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
