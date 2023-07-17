package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mutex sync.Mutex

	files       []string
	nWorker     int
	nWorkerExit int

	/* --- Metadata for map phase --- */
	nFiles       int
	pendingFiles *list.List
	FileStatus   []int // -2: not assigned, -1: done, >=0: assigned

	/* --- Metadata for reduce phase --- */
	nReducer        int
	pendingReducers *list.List
	ReducerStatus   []int // -2: not assigned, -1: done, >=0: assigned
	nReducerDone    int
}

func (c *Coordinator) Register(args *RegisterRequest, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.Id = c.nWorker
	reply.Nreducer = c.nReducer
	c.nWorker++

	return nil
}

func (c *Coordinator) MapGetTask(args *MapGetTaskRequest, reply *MapGetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.pendingFiles.Len() == 0 {
		reply.Filename = ""
		reply.Done = true
		for i := 0; i < c.nFiles; i++ {
			if c.FileStatus[i] != -1 {
				reply.Done = false
				break
			}
		}

		return nil
	}

	e := c.pendingFiles.Front()
	c.pendingFiles.Remove(e)

	file_id := e.Value.(int)
	reply.Filename = c.files[file_id]
	reply.Done = false

	c.FileStatus[file_id] = args.Id

	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneRequest, reply *MapTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	i := 0
	for ; i < c.nFiles; i++ {
		if c.FileStatus[i] == args.Id {
			c.FileStatus[i] = -1
			break
		}
	}

	if i == c.nFiles {
		log.Fatalf("MapDone: worker %d not found in pending_files\n", args.Id)
	}

	return nil
}

func (c *Coordinator) ReduceGetTask(args *ReduceGetTaskRequest, reply *ReduceGetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.Nworker = c.nWorker
	reply.AllReducerDone = c.nReducerDone == c.nReducer
	if c.pendingReducers.Len() == 0 {
		reply.ReducerId = -1
		return nil
	}

	e := c.pendingReducers.Front()
	c.pendingReducers.Remove(e)

	reducer_id := e.Value.(int)
	c.ReducerStatus[reducer_id] = args.Id
	reply.ReducerId = reducer_id

	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneRequest, reply *ReduceTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reducer_id := args.ReducerId
	if c.ReducerStatus[reducer_id] != args.Id {
		log.Fatalf("ReduceTaskDone: worker %d not found in ReducerStatus\n", args.Id)
	}

	c.ReducerStatus[reducer_id] = -1
	c.nReducerDone++

	return nil
}

func (c *Coordinator) GraceExit(args *GraceExitRequest, reply *GraceExitReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.nWorkerExit++

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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.nReducerDone == c.nReducer && c.nWorkerExit == c.nWorker
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReducer:        nReduce,
		files:           files,
		mutex:           sync.Mutex{},
		nFiles:          len(files),
		pendingFiles:    list.New(),
		FileStatus:      make([]int, len(files)),
		pendingReducers: list.New(),
		ReducerStatus:   make([]int, nReduce),
		nWorker:         0,
		nReducerDone:    0,
		nWorkerExit:     0,
	}

	for i := 0; i < len(files); i++ {
		c.pendingFiles.PushBack(i)
		c.FileStatus[i] = -2
	}

	for i := 0; i < nReduce; i++ {
		c.pendingReducers.PushBack(i)
		c.ReducerStatus[i] = -2
	}

	c.server()
	return &c
}
