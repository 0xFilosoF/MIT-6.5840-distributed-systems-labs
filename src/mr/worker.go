package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		r, ok := getTaskReply()
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		switch r.TaskType {
		case MAP:
			if err := doMapTask(r, mapf); err != nil {
				log.Printf("map task failed: %v", err)
			}
		case REDUCE:
			if err := doReduceTask(r, reducef); err != nil {
				log.Printf("reduce task failed: %v", err)
			}
		case NONE:
			time.Sleep(time.Second)
		}
	}
}

func doMapTask(reply TaskReply, mapf func(string, string) []KeyValue) error {
	filename := reply.File

	file, err := os.Open(filename)
	if err != nil {
		return err
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	file.Close()

	kva := mapf(filename, string(content))

	for i := 0; i < reply.NReduce; i++ {
		tmpFilename := fmt.Sprintf("mr-%d-%d", reply.TaskIndex, i)

		tmpFile, err := os.CreateTemp(".", tmpFilename)
		if err != nil {
			return err
		}

		enc := json.NewEncoder(tmpFile)

		for _, kv := range kva {
			hash := ihash(kv.Key) % reply.NReduce
			if hash == i {
				if err := enc.Encode(&kv); err != nil {
					return err
				}
			}
		}

		tmpFile.Close()

		os.Rename(tmpFile.Name(), tmpFilename)
	}

	return taskReport(reply)
}

func doReduceTask(reply TaskReply, reducef func(string, []string) string) error {
	fileIndex := reply.TaskIndex
	kva := []KeyValue{}

	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, fileIndex)
		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				return err
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", fileIndex)
	ofile, _ := os.CreateTemp(".", oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(), oname)

	return taskReport(reply)
}

func getTaskReply() (TaskReply, bool) {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)

	return reply, ok
}

func taskReport(r TaskReply) error {
	args := ReportTaskArgs{
		TaskType:  r.TaskType,
		TaskIndex: r.TaskIndex,
	}
	reply := ReportTaskReply{}
	ok := call("Coordinator.TaskReport", &args, &reply)

	if !ok {
		return errors.New("call failed!")
	}

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
