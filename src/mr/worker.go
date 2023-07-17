package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MapPhase(mapf func(string, string) []KeyValue, worker_id int, n_reducer int) {
	for {
		map_get_task_req := MapGetTaskRequest{Id: worker_id}
		map_get_task_reply := MapGetTaskReply{}
		if !call("Coordinator.MapGetTask", &map_get_task_req, &map_get_task_reply) {
			log.Fatalf("MapGetTask call failed!\n")
		}

		fileid := map_get_task_reply.FileId
		filename := map_get_task_reply.Filename

		if filename == "" {
			if map_get_task_reply.AllFileDone {
				break
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		intermediate_files := []*os.File{}
		encoders := []*json.Encoder{}
		for i := 0; i < n_reducer; i++ {
			file, err := ioutil.TempFile("./", "tmp-5840-map-")
			if err != nil {
				log.Fatalf("cannot create tempfile")
			}
			intermediate_files = append(intermediate_files, file)
			encoders = append(encoders, json.NewEncoder(file))
		}

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))

		for _, kv := range kva {
			idx := ihash(kv.Key) % n_reducer
			err := encoders[idx].Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v to tempfile %v", kv, intermediate_files[idx].Name())
			}
		}

		for i := 0; i < len(intermediate_files); i++ {
			os.Rename(intermediate_files[i].Name(), fmt.Sprintf("mr-%d-%d", fileid, i))
			intermediate_files[i].Close()
		}

		map_done_req := MapTaskDoneRequest{Id: worker_id, FileId: fileid}
		map_done_reply := MapTaskDoneReply{}
		if !call("Coordinator.MapTaskDone", &map_done_req, &map_done_reply) {
			log.Fatalf("MapTaskDone call failed!\n")
		}

	}
}

func ReducePhase(reducef func(string, []string) string, worker_id int) {
	for {
		reduce_get_task_req := ReduceGetTaskRequest{Id: worker_id}
		reduce_get_task_reply := ReduceGetTaskReply{}
		if !call("Coordinator.ReduceGetTask", &reduce_get_task_req, &reduce_get_task_reply) {
			log.Fatalf("ReduceGetTask call failed!\n")
		}

		if reduce_get_task_reply.ReducerId == -1 {
			if reduce_get_task_reply.AllReducerDone {
				break
			}

			time.Sleep(time.Duration(100) * time.Millisecond)
			continue
		}

		reducer_id := reduce_get_task_reply.ReducerId
		kva := []KeyValue{}

		for i := 0; i < reduce_get_task_reply.NFiles; i++ {
			filename := fmt.Sprintf("mr-%d-%d", i, reducer_id)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}

		sort.Sort(ByKey(kva))

		ofile, _ := ioutil.TempFile("./", "tmp-5840-reduce-")

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[i].Key == kva[j].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}

			output := reducef(kva[i].Key, values)

			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reducer_id))
		ofile.Close()

		reduce_done_req := ReduceTaskDoneRequest{Id: worker_id, ReducerId: reducer_id}
		reduce_done_reply := ReduceTaskDoneReply{}
		if !call("Coordinator.ReduceTaskDone", &reduce_done_req, &reduce_done_reply) {
			log.Fatalf("ReduceTaskDone call failed!\n")
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	register_req := RegisterRequest{}
	register_reply := RegisterReply{}
	if !call("Coordinator.Register", &register_req, &register_reply) {
		log.Fatalf("Register call failed!\n")
	}
	worker_id := register_reply.Id
	n_reducer := register_reply.Nreducer

	MapPhase(mapf, worker_id, n_reducer)
	ReducePhase(reducef, worker_id)

	grace_exit_request := GraceExitRequest{Id: worker_id}
	grace_exit_reply := GraceExitReply{}
	if !call("Coordinator.GraceExit", &grace_exit_request, &grace_exit_reply) {
		log.Fatalf("GraceExit call failed!\n")
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
