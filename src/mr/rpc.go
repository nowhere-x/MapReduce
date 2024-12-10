package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	// "os"
	// "strconv"
	"time"
)

// definition of task status/workedr status
const (
	UNASSIGNED = iota
	MAP_IN_PROGRESS
	REDUCE_IN_PROGRESS
	FINISHED
	WAIT
	WORKER_ID_EXIST
	EXIT
)

const (
	MAP = iota
	REDUCE
)

type TaskRequest struct {
	WorkerID string
	TaskID   int
	Status   int
	TaskType int
	Address  string
}

type TaskResponse struct {
	TaskID      int
	WorkerID    string
	Status      int
	TaskType    int
	TimeStamp   time.Time
	TargetFiles []string
	NReduce     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// func coordinatorSock() string {
// 	s := "/var/tmp/5840-mr-"
// 	s += strconv.Itoa(os.Getuid())
// 	return s
// }
