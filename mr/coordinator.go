package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

const TIMEOUT = time.Second * 10

type TaskStatus int

const (
	T_READY TaskStatus = iota
	T_PROCESSING
	T_FINISHED
)

type TaskInfo struct {
	taskReply AskTaskReply
	timer     *time.Timer
}

type CoordinatorStatus int

const (
	C_MAPPING CoordinatorStatus = iota
	C_REDUCING
	C_FINISHED
)

type Coordinator struct {
	// Your definitions here.

	// Tasks only need to be tracked in coordinator main loop
	nMap        int
	mapTasks    []TaskStatus
	mapFinished int

	nReduce        int
	reduceTasks    []TaskStatus
	reduceFinished int

	filenames []string

	status CoordinatorStatus

	// var may need concurrent access
	done atomic.Bool

	taskChan    chan TaskInfo
	finishChan  chan FinishTaskArgs
	timeoutChan chan TaskInfo
}

func (c *Coordinator) handleFinishArg() {
	select {
	case finishArg := <-c.finishChan:
		switch finishArg.TaskType {
		case TT_MAP:
			if c.mapTasks[finishArg.TaskId] != T_FINISHED {
				c.mapFinished++
			}
			c.mapTasks[finishArg.TaskId] = T_FINISHED
		case TT_REDUCE:
			if c.reduceTasks[finishArg.TaskId] != T_FINISHED {
				c.reduceFinished++
			}
			c.reduceTasks[finishArg.TaskId] = T_FINISHED
		}
	case taskInfo := <-c.timeoutChan:
		switch taskInfo.taskReply.TaskType {
		case TT_MAP:
			if c.mapTasks[taskInfo.taskReply.TaskId] != T_FINISHED {
				c.mapTasks[taskInfo.taskReply.TaskId] = T_READY
				c.taskChan <- taskInfo
			}
		case TT_REDUCE:
			if c.reduceTasks[taskInfo.taskReply.TaskId] != T_FINISHED {
				c.reduceTasks[taskInfo.taskReply.TaskId] = T_READY
				c.taskChan <- taskInfo
			}
		}
	default:
		return
	}
}

func (c *Coordinator) handleStateChange() {
	// finish := true
	switch c.status {
	case C_MAPPING:
		// for i := range c.nMap {
		// 	if c.mapTasks[i] != T_FINISHED {
		// 		finish = false
		// 		break
		// 	}
		// }
		finish := (c.nMap == c.mapFinished)
		if finish {
			// add all reduce task to taskchan
			for i := range c.nReduce {
				c.taskChan <- TaskInfo{
					taskReply: AskTaskReply{
						NMap:     c.nMap,
						NReduce:  c.nReduce,
						TaskId:   i,
						TaskType: TT_REDUCE,
					},
					timer: nil,
				}
			}
			c.status = C_REDUCING
		}
	case C_REDUCING:

		// for i := range c.nReduce {
		// 	if c.reduceTasks[i] != T_FINISHED {
		// 		finish = false
		// 		break
		// 	}
		// }
		finish := (c.nReduce == c.reduceFinished)
		if finish {
			// change state and done
			c.status = C_FINISHED
			c.done.Store(true)
		}
	}
}

func (c *Coordinator) mainLoop() {
	c.status = C_MAPPING

	for !c.Done() {
		// handle finish args
		c.handleFinishArg()
		c.handleStateChange()
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(_ *AskTaskArgs, reply *AskTaskReply) error {
	select {
	case taskInfo := <-c.taskChan:
		*reply = taskInfo.taskReply
		taskInfo.timer = time.NewTimer(TIMEOUT)
		go func(taskInfo TaskInfo, t *time.Timer) {
			<-t.C
			// log.Printf("Task %v of type %v timed out", taskId, taskType)
			// c.timeoutChan <- taskInfo
			c.timeoutChan <- taskInfo
		}(taskInfo, taskInfo.timer)

	default:
		if c.done.Load() {
			reply.TaskType = TT_EXIT
		} else {
			reply.TaskType = TT_IDLE
		}
	}
	return nil
}
func (c *Coordinator) FinishTask(args *FinishTaskArgs, _ *FinishTaskReply) error {
	// traverse to mark task finish
	c.finishChan <- (*args)
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
	ret = c.done.Load()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// initialize mapTask and reduceTask slices
	log.Printf("initializing coordinator..")
	c.nMap = len(files)
	c.nReduce = nReduce
	// channel
	c.taskChan = make(chan TaskInfo, c.nMap+c.nReduce)
	c.finishChan = make(chan FinishTaskArgs, c.nMap+c.nReduce)
	c.timeoutChan = make(chan TaskInfo, c.nMap+c.nReduce)

	c.mapTasks = make([]TaskStatus, c.nMap)
	log.Printf("map tasks slice created with size %v", c.nMap)
	for i := range c.mapTasks {
		c.mapTasks[i] = T_READY

		c.taskChan <- TaskInfo{
			taskReply: AskTaskReply{
				Filename: files[i],
				NMap:     c.nMap,
				NReduce:  nReduce,
				TaskId:   i,
				TaskType: TT_MAP,
			},
			timer: nil,
		}

	}
	log.Printf("map tasks initialized and added to task channel")
	c.reduceTasks = make([]TaskStatus, nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i] = T_READY
	}

	c.mapFinished, c.reduceFinished = 0, 0

	c.filenames = make([]string, c.nMap)
	copy(c.filenames, files)

	go c.mainLoop()

	c.server()
	return &c
}
