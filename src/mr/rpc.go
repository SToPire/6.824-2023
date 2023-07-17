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
	FileId      int
	Filename    string
	AllFileDone bool
}

type MapTaskDoneRequest struct {
	Id     int
	FileId int
}

type MapTaskDoneReply struct {
}

type ReduceGetTaskRequest struct {
	Id int
}

type ReduceGetTaskReply struct {
	ReducerId      int
	AllReducerDone bool
	NFiles         int
}

type ReduceTaskDoneRequest struct {
	Id        int
	ReducerId int
}

type ReduceTaskDoneReply struct {
}

type GraceExitRequest struct {
	Id int
}

type GraceExitReply struct {
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
