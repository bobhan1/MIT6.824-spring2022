package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
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

// TaskArgs is single tasks arguments
type TaskArgs struct {
	Index    int    //current task index
	TaskType string //map or reduce
	FileName string
	NReduce  int //number of bucket need in reduce phase

	WId string    //worker id
	Due time.Time //need to wait 10 secs if worker not reply
}

//worker apply task to coordinator
type AskTaskArgs struct {
	WId               string //worker id
	FinishedTask      string //last finished task type
	FinishedTaskIndex int    //last finished task index
}

//worker get reply from coordinator
type AskTaskReply struct {
	Done     bool   //all task finished
	Type     string //its map or reduce
	Index    int
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
