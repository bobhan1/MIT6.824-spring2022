package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
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
	tasks        map[string]TaskArgs //coordinator can control all tasks, so it needs to store each TaskArgs
	channelTask  chan TaskArgs       //create a channel that will receive and send available Tasks
	finishedAll  bool
	mutex        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//worker call this function to ask for tasks
func (c *Coordinator) AskReply(args *AskTaskArgs, reply *AskTaskReply) error {

	//periodically check if worker exceeds 10s and not return their work
	//if so, redeliver this task
	go func() {
		for {
			time.Sleep(600 * time.Millisecond)

			c.mutex.Lock()
			for _, task := range c.tasks {
				if task.WId != "" && time.Now().After(task.Due) {
					log.Printf("timed-out %s task %d on worker %s. Rediliever this task",
						task.TaskType, task.Index, task.WId)
					task.WId = ""
					c.channelTask <- task
				}
			}
			c.mutex.Unlock()
		}
	}()

	//clear finished task from task collection
	if args.FinishedTask != "" {
		c.mutex.Lock()
		//defer c.mutex.Lock()
		//isWorker return true if task belongs to current worker
		taskID := fmt.Sprintf("%s-%d", args.FinishedTask, args.FinishedTaskIndex)
		task, isWorker := c.tasks[taskID]
		//make sure the task has not been rearranged to other worker
		if isWorker && task.WId == args.WId {
			//print("11111111111111111")
			//log.Printf("%s task %d finished for worker %s\n", task.TaskType, task.Index, task.WId)

			//delete finished task from tasks collection
			delete(c.tasks, taskID)
			if len(c.tasks) == 0 {
				c.changeStage(reply)
			}
		}
		c.mutex.Unlock()
	}

	//get one available task in channel
	task, ok := <-c.channelTask
	if !ok {
		reply.Done = true
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	log.Printf("Assign %s task %d to worker %s \n", task.TaskType, task.Index, args.WId)
	task.Due = time.Now().Add(time.Second * 10) //waiting time is 10s
	//
	task.WId = args.WId
	tID := fmt.Sprintf("%s-%d", task.TaskType, task.Index)
	c.tasks[tID] = task
	//assign reply value using task
	reply.Type = task.TaskType
	reply.NReduce = c.NReduce
	reply.NMap = c.NMap
	reply.Index = task.Index
	reply.FileName = task.FileName
	return nil
}

//change current coordinator stage from mapping to reduce, or finish
func (c *Coordinator) changeStage(reply *AskTaskReply) {
	if c.CurrentStage == "map" {

		log.Printf("All map tasks finished. Transit to reduce\n")
		c.CurrentStage = "reduce"

		//create reduce task and put it into Coordinator tasks collection
		for i := 0; i < c.NReduce; i++ {
			task := TaskArgs{
				TaskType: "reduce",
				Index:    i,
			}
			c.tasks[fmt.Sprintf("%s-%d", task.TaskType, task.Index)] = task
			c.channelTask <- task
		}

	} else if c.CurrentStage == "reduce" {
		//close channel
		log.Printf("All reduce tasks finished")
		close(c.channelTask)
		c.finishedAll = true
		reply.Done = true
	}
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
	//ret := false

	// Your code here.

	return c.finishedAll
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
		channelTask:  make(chan TaskArgs, int(math.Max(float64(len(files)), float64(nReduce)))), //channel capacity should be length of file or nReduce
		finishedAll:  false,
	}
	// Your code here.
	//loop through each input filename, put it into tasks map collection
	for i, file := range files {
		//store each file task args into Coordinator
		task := TaskArgs{
			Index:    i,
			TaskType: "map",
			FileName: file,
		}
		c.tasks[fmt.Sprintf("%s-%d", task.TaskType, task.Index)] = task
		c.channelTask <- task //send this task to channel

	}
	println("task length ", len(c.tasks))
	log.Println("Coordinator has started")
	c.server()

	return &c
}
