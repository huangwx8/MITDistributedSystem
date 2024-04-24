package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TASK_STATE_PENDING int = 0
	TASK_STATE_RUNNING int = 1
	TASK_STATE_DONE    int = 2
)

type TaskDescriptor struct {
	File               string
	TaskState          int
	StartTaskTimestamp int64
}

type Coordinator struct {
	// Your definitions here.
	mapTaskDescriptors    []TaskDescriptor
	reduceTaskDescriptors []TaskDescriptor
	nMapTasksDone         int
	nReduceTasksDone      int
	lock                  sync.Mutex
}

const workerTimeout = 20

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {

	for !c.Done() {
		c.lock.Lock()
		if !c.hasAllMapTasksDone() {
			for i, _ := range c.mapTaskDescriptors {
				if c.mapTaskDescriptors[i].TaskState == TASK_STATE_PENDING {
					c.mapTaskDescriptors[i].TaskState = TASK_STATE_RUNNING
					c.mapTaskDescriptors[i].StartTaskTimestamp = time.Now().Unix()
					reply.TaskID = i
					reply.NReduce = len(c.reduceTaskDescriptors)
					reply.TaskType = TASK_TYPE_MAP
					reply.Filename = c.mapTaskDescriptors[i].File
					c.lock.Unlock()
					return nil
				}
			}
		} else {
			for j, _ := range c.reduceTaskDescriptors {
				if c.reduceTaskDescriptors[j].TaskState == TASK_STATE_PENDING {
					c.reduceTaskDescriptors[j].TaskState = TASK_STATE_RUNNING
					c.reduceTaskDescriptors[j].StartTaskTimestamp = time.Now().Unix()
					reply.TaskID = j
					reply.NReduce = len(c.reduceTaskDescriptors)
					reply.TaskType = TASK_TYPE_REDUCE
					reply.Filename = fmt.Sprintf("mr-*-%d", j)
					c.lock.Unlock()
					return nil
				}
			}
		}
		c.lock.Unlock()

		time.Sleep(100 * time.Millisecond)
	}

	return &RPCError{"No rest task"}
}

func (c *Coordinator) NotifyMapDone(args *NotifyMapDoneArgs, reply *NotifyMapDoneReply) error {
	c.lock.Lock()
	if c.mapTaskDescriptors[args.TaskID].TaskState == TASK_STATE_RUNNING {
		c.mapTaskDescriptors[args.TaskID].TaskState = TASK_STATE_DONE
		c.nMapTasksDone++
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) NotifyReduceDone(args *NotifyReduceDoneArgs, reply *NotifyReduceDoneReply) error {
	c.lock.Lock()
	if c.reduceTaskDescriptors[args.TaskID].TaskState == TASK_STATE_RUNNING {
		c.reduceTaskDescriptors[args.TaskID].TaskState = TASK_STATE_DONE
		c.nReduceTasksDone++
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) hasAllMapTasksDone() bool {
	return c.nMapTasksDone == len(c.mapTaskDescriptors)
}

// Sweep thread
func (c *Coordinator) sweep() {
	go func() {
		for {
			c.lock.Lock()
			// scan all tasks, determine which of them are dead
			for i, _ := range c.mapTaskDescriptors {
				if c.mapTaskDescriptors[i].TaskState == TASK_STATE_RUNNING {
					deltaSecond := time.Now().Unix() - c.mapTaskDescriptors[i].StartTaskTimestamp
					if deltaSecond >= workerTimeout {
						fmt.Printf("sweep map tesk %d\n", i)
						c.mapTaskDescriptors[i].TaskState = TASK_STATE_PENDING
					}
				}
			}
			for i, _ := range c.reduceTaskDescriptors {
				if c.reduceTaskDescriptors[i].TaskState == TASK_STATE_RUNNING {
					deltaSecond := time.Now().Unix() - c.reduceTaskDescriptors[i].StartTaskTimestamp
					if deltaSecond >= workerTimeout {
						fmt.Printf("sweep reduce tesk %d\n", i)
						c.reduceTaskDescriptors[i].TaskState = TASK_STATE_PENDING
					}
				}
			}
			c.lock.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()
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
	c.lock.Lock()
	ret = c.nReduceTasksDone == len(c.reduceTaskDescriptors)
	c.lock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTaskDescriptors = make([]TaskDescriptor, len(files))
	for i, _ := range c.mapTaskDescriptors {
		c.mapTaskDescriptors[i].TaskState = TASK_STATE_PENDING
		c.mapTaskDescriptors[i].File = files[i]
		c.mapTaskDescriptors[i].StartTaskTimestamp = 0
	}

	c.reduceTaskDescriptors = make([]TaskDescriptor, nReduce)
	for i, _ := range c.reduceTaskDescriptors {
		c.reduceTaskDescriptors[i].TaskState = TASK_STATE_PENDING
		c.reduceTaskDescriptors[i].File = ""
		c.reduceTaskDescriptors[i].StartTaskTimestamp = 0
	}

	c.nMapTasksDone = 0
	c.nReduceTasksDone = 0

	c.sweep()

	c.server()
	return &c
}
