package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//get current worker's id
func getID() string {
	return strconv.Itoa(os.Getpid())
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var finishedTask string
	var finishedTaskIndex int
	log.Println("worker started")
	retry := 3
	//keep asking coordinator for task until finished
	for {
		args := AskTaskArgs{
			WId:               getID(),
			FinishedTask:      finishedTask,
			FinishedTaskIndex: finishedTaskIndex,
		}
		reply := AskTaskReply{}
		//this will call function in Coordinator.askTaskReply
		call("Coordinator.AskReply", &args, &reply)

		if reply.Done {
			log.Println("All task finished!")
			break
		}

		log.Printf("Received %s task %d from coordinator", reply.Type, reply.Index)

		switch reply.Type {
		case "map":
			MapTask(reply, mapf)
			println("mapping stage!!!\n")
			retry = 3
		case "reduce":
			ReduceTask(reply, reducef)
			println("Reduce stage!!!!!\n")
			retry = 3
		default:
			log.Println("error reply: would retry times: ", retry)
			if retry < 0 {
				return
			}
			retry--
		}
		finishedTask = reply.Type
		finishedTaskIndex = reply.Index
		time.Sleep(1000 * time.Millisecond)
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	log.Printf("Worker exit\n")
}

// MapTask handle task for Mapping
func MapTask(reply AskTaskReply, mapf func(string, string) []KeyValue) {
	//open replied file
	file, err := os.Open(reply.FileName)
	if err != nil {
		//%v could scan default format for certain value
		log.Fatalf("cannot open input file %v", reply.FileName)
	}
	//read entire file into content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read input file  %v", reply.FileName)
	}
	//use map function get intermediate result(slice of key value pair)
	kva := mapf(reply.FileName, string(content))
	//create a map to represent bucket(key=bucketIndex, value = a slice of KeyValue pair)
	bucketMap := make(map[int][]KeyValue)
	for _, keyValue := range kva {
		//according to key and NReduce to find which bucket the pair belongs to
		bucketIndex := ihash(keyValue.Key) % reply.NReduce
		bucketMap[bucketIndex] = append(bucketMap[bucketIndex], keyValue)
	}
	//store bucketMap into intermediate file
	//convention mr-X-Y, X=task index(map task number), Y = reduce index(reduce task number)
	tmpName := "mr-" + strconv.Itoa(reply.Index)
	for i := 0; i < reply.NReduce; i++ {
		tempFile, _ := os.Create(tmpName + "-" + strconv.Itoa(i))
		//there are NReduce number of buckets
		//loop through each buckets according to index
		for _, keyValue := range bucketMap[i] {
			//trick to write two variable into file
			_, err := fmt.Fprintf(tempFile, "%v,%v\n", keyValue.Key, keyValue.Value)
			if err != nil {
				log.Fatalf("cannot write temp file %v", tempFile.Name())
			}
		}
		err := tempFile.Close()
		if err != nil {
			log.Fatalf("cannot close file %v", tempFile.Name())
		}
	}

}

//ReduceTask handle task for reduce
func ReduceTask(reply AskTaskReply, reducef func(string, []string) string) {
	//create empty intermediate key value slice
	var intermediate []KeyValue
	var lines []string
	for mapIndex := 0; mapIndex < reply.NMap; mapIndex++ {
		//mr-X-Y, Y is current worker index
		fileName := "mr-" + strconv.Itoa(mapIndex) + "-" + strconv.Itoa(reply.Index)
		tempFile, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open this file %v", reply.FileName)
		}
		bytesFile, err := ioutil.ReadAll(tempFile)
		if err != nil {
			log.Fatalf("cannot read input file  %v", reply.FileName)
		}
		lines = append(lines, strings.Split(string(bytesFile), "\n")...)

	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		pairs := strings.Split(line, ",")
		intermediate = append(intermediate, KeyValue{pairs[0], pairs[1]})
	}

	//copy code from mrsequential
	sort.Sort(ByKey(intermediate))

	ofile, _ := os.Create("mr-out-" + strconv.Itoa(reply.Index))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//fmt.Printf("%v", values)
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return
		}

		i = j
	}
	err := ofile.Close()
	if err != nil {
		return
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	//call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
