package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	nReduce int
	files   []string
	nFiles  int

	mutex sync.Mutex

	/* --- Metadata for map phase --- */
	curFile      int   // Next file to be assigned
	pendingFiles []int // -2: not assigned, -1: done, >=0: assigned

	/* --- Metadata for reduce phase --- */

	/* --- Metadata for workers --- */
	nWorkers int
}

func (c *Coordinator) Register(args *RegisterRequest, reply *RegisterReply) error {
	c.mutex.Lock()

	reply.Id = c.nWorkers
	reply.Nreducer = c.nReduce
	c.nWorkers++

	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) MapTaskAssign(args *MapGetTaskRequest, reply *MapGetTaskReply) error {
	c.mutex.Lock()

	if c.curFile == c.nFiles {
		reply.Filename = ""
		reply.Done = true
		for i := 0; i < c.nFiles; i++ {
			if c.pendingFiles[i] != -1 {
				reply.Done = false
				break
			}
		}

		c.mutex.Unlock()
		return nil
	}

	reply.Filename = c.files[c.curFile]
	reply.Done = false

	c.pendingFiles[c.curFile] = args.Id
	c.curFile++

	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneRequest, reply *MapTaskDoneReply) error {
	c.mutex.Lock()

	i := 0
	for ; i < c.nFiles; i++ {
		if c.pendingFiles[i] == args.Id {
			c.pendingFiles[i] = -1
			break
		}
	}

	if i == c.nFiles {
		log.Fatalf("MapDone: worker %d not found in pending_files\n", args.Id)
	}

	c.mutex.Unlock()
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:      nReduce,
		files:        files,
		mutex:        sync.Mutex{},
		nFiles:       len(files),
		curFile:      0,
		pendingFiles: make([]int, len(files)),
		nWorkers:     0,
	}

	for i := 0; i < len(files); i++ {
		c.pendingFiles[i] = -2
	}

	c.server()
	return &c
}
