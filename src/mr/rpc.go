package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type RegisterRequest struct {
}

type RegisterReply struct {
	Id       int
	Nreducer int
}

type MapGetTaskRequest struct {
	Id int
}

type MapGetTaskReply struct {
	Filename string
	/* If all the mapper has done */
	Done bool
}

type MapTaskDoneRequest struct {
	Id       int
	Filename string
}

type MapTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
