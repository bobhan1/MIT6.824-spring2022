package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	NReduce      int
	NMap         int                 //one input file = a Map task
	CurrentStage string              //current is in Map or Reduce stage
	tasks        map[string]TaskArgs //coordinator control all tasks

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//initialize coordinator
	c := Coordinator{
		CurrentStage: "map", //initially should be Map stage
		NReduce:      nReduce,
		NMap:         len(files),
		tasks:        make(map[string]TaskArgs),
	}
	// Your code here.
	//loop through each input filename, put it into tasks map collection
	for i, file := range files {
		//store each file args into RPC
		task := TaskArgs{
			index:    i,
			TaskType: "map",
			FileName: file,
		}
		c.tasks[fmt.Sprintf("%s-%d", task.TaskType, task.index)] = task
	}

	c.server()
	return &c
}
