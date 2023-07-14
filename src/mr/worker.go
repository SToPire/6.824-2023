package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	register_req := RegisterRequest{}
	register_reply := RegisterReply{}
	ok := call("Coordinator.Register", &register_req, &register_reply)
	if !ok {
		fmt.Printf("Register call failed!\n")
	}
	worker_id := register_reply.Id
	n_reducer := register_reply.Nreducer

	intermediate_files := []*os.File{}
	encoders := []*json.Encoder{}
	for i := 0; i < n_reducer; i++ {
		filename := fmt.Sprintf("mr-%d-%d", worker_id, i)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		intermediate_files = append(intermediate_files, file)
		encoders = append(encoders, json.NewEncoder(file))
	}

	for {
		map_get_task_req := MapGetTaskRequest{Id: worker_id}
		map_get_task_reply := MapGetTaskReply{}
		if !call("Coordinator.MapTaskAssign", &map_get_task_req, &map_get_task_reply) {
			fmt.Printf("MapTaskAssign call failed!\n")
		}

		if map_get_task_reply.Filename == "" {
			if map_get_task_reply.Done {
				break
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
		} else {
			file, err := os.Open(map_get_task_reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", map_get_task_reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", map_get_task_reply.Filename)
			}
			file.Close()

			kva := mapf(map_get_task_reply.Filename, string(content))

			for _, kv := range kva {
				idx := ihash(kv.Key) % n_reducer
				err := encoders[idx].Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v", kv)
				}
			}

			map_done_req := MapTaskDoneRequest{Id: worker_id, Filename: map_get_task_reply.Filename}
			map_done_reply := MapTaskDoneReply{}
			if !call("Coordinator.MapTaskDone", &map_done_req, &map_done_reply) {
				fmt.Printf("MapTaskDone call failed!\n")
			}
		}
	}

	for _, file := range intermediate_files {
		file.Close()
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
