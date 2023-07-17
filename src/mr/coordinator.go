package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	WorkerTimeout     = 10 * time.Second
	TASK_NOT_ASSIGNED = -2
	TASK_DONE         = -1
)

type Coordinator struct {
	mutex sync.Mutex

	nWorker     int    // Number of workers (including crashed workers)
	nWorkerExit int    // How many workers has exited (crash is recognized as exit)
	workerExit  []bool // Whether the worker has exited

	/* --- Metadata for map phase --- */
	nFiles         int         // Number of input files
	pendingFiles   *list.List  // List of files that are not assigned
	fileStatus     []int       // -2: not assigned, -1: done, >=0: assigned
	fileAssignTime []time.Time // Time when the file is assigned
	nFilesDone     int         // Number of files that are done
	files          []string    // Array of input filenames

	/* --- Metadata for reduce phase --- */
	nReducer          int         // Number of reducers
	pendingReducers   *list.List  // List of reducers that are not assigned
	reducerStatus     []int       // -2: not assigned, -1: done, >=0: assigned
	reducerAssignTime []time.Time // Time when the reducer is assigned
	nReducerDone      int         // Number of reducers that are done
}

func (c *Coordinator) Register(args *RegisterRequest, reply *RegisterReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.Id = c.nWorker
	reply.Nreducer = c.nReducer
	c.nWorker++
	c.workerExit = append(c.workerExit, false)

	return nil
}

func (c *Coordinator) MapGetTask(args *MapGetTaskRequest, reply *MapGetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	/* DO NOT respond to a zombie worker */
	if c.workerExit[args.Id] {
		reply.FileId = -1
		reply.Filename = ""
		reply.AllFileDone = true

		return nil
	}

	reply.AllFileDone = c.nFilesDone == c.nFiles
	if c.pendingFiles.Len() == 0 {
		reply.FileId = -1
		reply.Filename = ""

		return nil
	}

	e := c.pendingFiles.Front()
	c.pendingFiles.Remove(e)

	file_id := e.Value.(int)
	reply.FileId = file_id
	reply.Filename = c.files[file_id]

	c.fileStatus[file_id] = args.Id
	c.fileAssignTime[file_id] = time.Now()

	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneRequest, reply *MapTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	/* DO NOT respond to a zombie worker */
	if c.workerExit[args.Id] {
		return nil
	}

	if c.fileStatus[args.FileId] != args.Id {
		log.Fatalf("MapDone: worker %d is not the mapper of file %d\n", args.Id, args.FileId)
	}

	c.fileStatus[args.FileId] = TASK_DONE
	c.fileAssignTime[args.FileId] = time.Time{}
	c.nFilesDone++

	return nil
}

func (c *Coordinator) ReduceGetTask(args *ReduceGetTaskRequest, reply *ReduceGetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	/* DO NOT respond to a zombie worker */
	if c.workerExit[args.Id] {
		reply.ReducerId = -1
		reply.AllReducerDone = true
		reply.NFiles = 0

		return nil
	}

	reply.AllReducerDone = c.nReducerDone == c.nReducer
	reply.NFiles = c.nFiles
	if c.pendingReducers.Len() == 0 {
		reply.ReducerId = -1
		return nil
	}

	e := c.pendingReducers.Front()
	c.pendingReducers.Remove(e)

	reducer_id := e.Value.(int)
	reply.ReducerId = reducer_id

	c.reducerStatus[reducer_id] = args.Id
	c.reducerAssignTime[reducer_id] = time.Now()

	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneRequest, reply *ReduceTaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	/* DO NOT respond to a zombie worker */
	if c.workerExit[args.Id] {
		return nil
	}

	if c.reducerStatus[args.ReducerId] != args.Id {
		log.Fatalf("ReduceTaskDone: worker %d not found in ReducerStatus\n", args.Id)
	}

	c.reducerStatus[args.ReducerId] = TASK_DONE
	c.reducerAssignTime[args.ReducerId] = time.Time{}
	c.nReducerDone++

	return nil
}

func (c *Coordinator) GraceExit(args *GraceExitRequest, reply *GraceExitReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	/* DO NOT respond to a zombie worker */
	if c.workerExit[args.Id] {
		return nil
	}

	c.nWorkerExit++
	c.workerExit[args.Id] = true

	return nil
}

func (c *Coordinator) checkWorker() {
	for {
		time.Sleep(3 * time.Second)
		c.mutex.Lock()

		for i := 0; i < c.nWorker; i++ {
			if !c.workerExit[i] {
				for j := 0; j < c.nFiles; j++ {
					if c.fileStatus[j] == i && time.Since(c.fileAssignTime[j]) > WorkerTimeout {
						c.pendingFiles.PushBack(j)
						c.fileStatus[j] = TASK_NOT_ASSIGNED
						c.fileAssignTime[j] = time.Time{}
						c.workerExit[i] = true
						break
					}
				}

				for j := 0; j < c.nReducer; j++ {
					if c.reducerStatus[j] == i && time.Since(c.reducerAssignTime[j]) > WorkerTimeout {
						c.pendingReducers.PushBack(j)
						c.reducerStatus[j] = TASK_NOT_ASSIGNED
						c.reducerAssignTime[j] = time.Time{}
						c.workerExit[i] = true
						break
					}
				}

				if c.workerExit[i] {
					c.nWorkerExit++
				}
			}
		}

		c.mutex.Unlock()
	}
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
	go c.checkWorker()
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
		mutex:             sync.Mutex{},
		nWorker:           0,
		nWorkerExit:       0,
		workerExit:        []bool{},
		nFiles:            len(files),
		pendingFiles:      list.New(),
		fileStatus:        make([]int, len(files)),
		fileAssignTime:    make([]time.Time, len(files)),
		nFilesDone:        0,
		files:             files,
		nReducer:          nReduce,
		pendingReducers:   list.New(),
		reducerStatus:     make([]int, nReduce),
		reducerAssignTime: make([]time.Time, nReduce),
		nReducerDone:      0,
	}

	for i := 0; i < c.nFiles; i++ {
		c.pendingFiles.PushBack(i)
		c.fileStatus[i] = TASK_NOT_ASSIGNED
		c.fileAssignTime[i] = time.Time{}
	}

	for i := 0; i < nReduce; i++ {
		c.pendingReducers.PushBack(i)
		c.reducerStatus[i] = TASK_NOT_ASSIGNED
		c.reducerAssignTime[i] = time.Time{}
	}

	c.server()
	return &c
}
