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

// const SUCCESS = math.MaxInt32
const taskTimeout = 10 * time.Second

type Coordinator struct {
	// Your definitions here.

	nMap        int
	nReduce     int
	mapTasks    []TaskStatus // len nMap
	reduceTasks []TaskStatus // len nReduce
	wgMap       sync.WaitGroup
	wgReduce    sync.WaitGroup
	tasksCh     chan TaskReply

	mu   sync.RWMutex
	done bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	if len(c.tasksCh) == 0 {
		reply.TaskType = NONE
		return nil
	}

	*reply = <-c.tasksCh
	c.mu.Lock()

	switch reply.TaskType {
	case MAP:
		if c.mapTasks[reply.TaskIndex] == IDLE {
			c.mapTasks[reply.TaskIndex] = PROGRESS
		}
	case REDUCE:
		if c.reduceTasks[reply.TaskIndex] == IDLE {
			c.reduceTasks[reply.TaskIndex] = PROGRESS
		}
	case NONE:
		return nil
	}

	c.mu.Unlock()

	go func() {
		timer := time.NewTimer(time.Second)
		defer timer.Stop()

		select {
		case <-timer.C:
			c.mu.RLock()
			defer c.mu.RUnlock()
			if c.getStatusFromReply(reply) == DONE {
				return
			}
		case <-time.After(taskTimeout):
			c.mu.RLock()
			defer c.mu.RUnlock()
			if c.getStatusFromReply(reply) == DONE {
				return
			}
			c.tasksCh <- *reply
		}
	}()

	return nil
}

func (c *Coordinator) TaskReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	switch args.TaskType {
	case MAP:
		if c.mapTasks[args.TaskIndex] != PROGRESS {
			reply.OK = false
			return nil
		}
		c.mapTasks[args.TaskIndex] = DONE
		c.wgMap.Done()
	case REDUCE:
		if c.reduceTasks[args.TaskIndex] != PROGRESS {
			reply.OK = false
			return nil
		}
		c.reduceTasks[args.TaskIndex] = DONE
		c.wgReduce.Done()
	case NONE:
		reply.OK = true
		return nil
	}

	reply.OK = true
	return nil
}

func (c *Coordinator) getStatusFromReply(reply *TaskReply) TaskStatus {
	switch reply.TaskType {
	case MAP:
		return c.mapTasks[reply.TaskIndex]
	case REDUCE:
		return c.reduceTasks[reply.TaskIndex]
	// never happen
	default:
		return IDLE
	}
}

func StartReduceTasks(c *Coordinator) {
	c.wgMap.Wait()
	c.wgReduce.Add(c.nReduce)

	for i := 0; i < c.nReduce; i++ {
		c.tasksCh <- TaskReply{
			TaskType:  REDUCE,
			TaskIndex: i,
			NMap:      c.nMap,
			NReduce:   c.nReduce,
		}
	}
	go CoordinatorDone(c)
}

func CoordinatorDone(c *Coordinator) {
	c.wgReduce.Wait()
	c.done = true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	buflen := max(len(files), nReduce)

	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]TaskStatus, len(files)),
		reduceTasks: make([]TaskStatus, nReduce),
		wgMap:       sync.WaitGroup{},
		wgReduce:    sync.WaitGroup{},
		tasksCh:     make(chan TaskReply, buflen),
	}

	c.wgMap.Add(c.nMap)
	for i, file := range files {
		c.tasksCh <- TaskReply{
			TaskType:  MAP,
			File:      file,
			TaskIndex: i,
			NMap:      c.nMap,
			NReduce:   c.nReduce,
		}
	}
	go StartReduceTasks(&c)
	c.server(sockname)

	return &c
}
