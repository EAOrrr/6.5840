package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TIMEOUT = time.Second * 5

type TaskStatus int

const (
	T_READY TaskStatus = iota
	T_PROCESSING
	T_FINISHED
)

type Task struct {
	status  TaskStatus
	timeout time.Time
}

type CoordinatorStatus int

const (
	C_READY CoordinatorStatus = iota
	C_MAPPING
	C_REDUCING
	C_FINISHED
)

type Coordinator struct {
	// Your definitions here.
	taskMutex sync.Mutex

	mapTasks []Task
	nMap     int

	reduceTasks []Task
	nReduce     int

	filenames []string

	statusMutex sync.Mutex
	status      CoordinatorStatus
}

func (c *Coordinator) checkTimeoutTask() {
	for !c.Done() {
		c.statusMutex.Lock()

		switch c.status {
		case C_MAPPING:
			c.taskMutex.Lock()

			for i := 0; i < c.nMap; i++ {
				if c.mapTasks[i].status == T_PROCESSING &&
					time.Now().After(c.mapTasks[i].timeout) {
					log.Printf("map task %v timed out, resetting to T_READY", i)
					c.mapTasks[i].status = T_READY
				}
			}
			c.taskMutex.Unlock()

		case C_REDUCING:
			c.taskMutex.Lock()

			for i := 0; i < c.nReduce; i++ {
				if c.reduceTasks[i].status == T_PROCESSING &&
					time.Now().After(c.reduceTasks[i].timeout) {
					log.Printf("reduce task %v timed out, resetting to T_READY", i)
					c.reduceTasks[i].status = T_READY
				}
			}
			c.taskMutex.Unlock()

		}
		c.statusMutex.Unlock()
		time.Sleep(time.Second)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(_ *AskTaskArgs, reply *AskTaskReply) error {
	c.statusMutex.Lock()
	defer c.statusMutex.Unlock()

	reply.TaskType = TT_IDLE
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	switch c.status {
	case C_MAPPING:
		c.taskMutex.Lock()
		for i := 0; i < c.nMap; i++ {
			if c.mapTasks[i].status == T_READY { // assign task
				c.mapTasks[i].status = T_PROCESSING
				c.mapTasks[i].timeout = time.Now().Add(TIMEOUT)

				reply.TaskType = TT_MAP
				reply.Filename = c.filenames[i]
				reply.TaskId = i
				break
			}
		}
		c.taskMutex.Unlock()
	case C_REDUCING:
		c.taskMutex.Lock()
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTasks[i].status == T_READY { // assign task
				c.reduceTasks[i].status = T_PROCESSING
				c.reduceTasks[i].timeout = time.Now().Add(TIMEOUT)

				reply.TaskType = TT_REDUCE
				reply.TaskId = i
				break
			}
		}
		c.taskMutex.Unlock()
	case C_FINISHED:
		reply.TaskType = TT_EXIT
	}
	return nil
}
func (c *Coordinator) FinishTask(args *FinishTaskArgs, _ *FinishTaskReply) error {
	// traverse to mark task finish
	c.statusMutex.Lock()
	defer c.statusMutex.Unlock()
	// log.Printf("Coordinator received FinishTask for taskId %v of type %v", args.TaskId, args.TaskType)
	taskId, taskType := args.TaskId, args.TaskType
	taskFinished := true
	switch c.status {
	case C_MAPPING:
		if taskType != TT_MAP {
			break
		}
		c.taskMutex.Lock()
		c.mapTasks[taskId].status = T_FINISHED

		for i := 0; i < c.nMap; i++ {
			if c.mapTasks[i].status != T_FINISHED {
				taskFinished = false
				// log.Printflog.Printf("Map task %v not finished yet", i)
			}
		}

		c.taskMutex.Unlock()
		if taskFinished {
			c.status = C_REDUCING
			// log.Printf("Coordinator status changed to C_REDUCING")
		}
	case C_REDUCING:
		if taskType != TT_REDUCE {
			break
		}
		c.taskMutex.Lock()
		c.reduceTasks[taskId].status = T_FINISHED
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTasks[i].status != T_FINISHED {
				taskFinished = false
			}
		}
		c.taskMutex.Unlock()
		if taskFinished {
			// log.Printf("Coordinator status changed to C_FINISHED")
			c.status = C_FINISHED
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.statusMutex.Lock()
	defer c.statusMutex.Unlock()

	ret = (c.status == C_FINISHED)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// initialize mapTask and reduceTask slices
	c.status = C_READY

	c.nMap = len(files)
	c.mapTasks = make([]Task, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i].status = T_READY
	}

	c.nReduce = nReduce
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i].status = T_READY
	}

	c.filenames = make([]string, c.nMap)
	copy(c.filenames, files)

	go c.checkTimeoutTask()
	c.status = C_MAPPING
	// log.Printf("Coordinator status changed to C_MAPPING")
	// log.Printf("Coordinator initialized with %v map tasks and %v reduce tasks", c.nMap, c.nReduce)

	c.server()
	return &c
}
