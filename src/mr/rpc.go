package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// WorkerReply Add your RPC definitions here.
type TaskArgs struct {
	WId string //worker id
}
type TaskReply struct {
	TaskId   int
	TaskType string //map or reduce
	FileName string
	NReduce  int //number of bucket need in reduce phase
}

//worker apply task to coordinator
type askTaskArgs struct {
	WId               string //worker id
	FinishedTask      string //last finished task type
	FinishedTaskIndex int    //last finished task index
}

//worker get reply from coordinator
type askTaskReply struct {
	Done     bool //all task finished
	Type     string
	index    int
	FileName string
	NMap     int //number of maps
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
