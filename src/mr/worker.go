package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	CoordAddr  string
	WorkerAddr string
	WorkerID   string
}

var config Config

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

const (
	INFO = iota
	WARNING
	FATAL
)

type WorkerServer struct {
	Mutex sync.Mutex
}

func log_message(msg string, level int) {
	switch level {
	case INFO:
		log.Printf("INFO: %s", msg)
	case WARNING:
		log.Printf("WARNING: %s", msg)
	case FATAL:
		log.Printf("FATAL: %s", msg)
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}, addr string) bool {
	c, err := rpc.DialHTTP("tcp", addr)

	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ws *WorkerServer) server() {

	rpc.Register(ws)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", config.WorkerAddr)

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (ws *WorkerServer) IntermediateFilesRequest(bucket *int, respsonse *[]string) error {
	ws.Mutex.Lock()
	defer ws.Mutex.Unlock()

	pattern := "mr-" + strconv.Itoa(*bucket) + "*"
	files, err := filepath.Glob(pattern)

	log.Printf("Intermediate Request for %s", pattern)
	if err != nil {
		log.Printf("Error finding files: %v", err)
	}

	// Check if any files match the pattern
	if len(files) == 0 {
		log.Printf("No files found matching the pattern")
		return nil
	}

	for _, file_name := range files {
		file, err := os.Open(file_name)
		if err != nil {
			log.Printf("Error opening file %s: %v", file_name, err)
			continue
		}

		content, err := io.ReadAll(file)
		if err != nil {
			log.Printf("Error reading file %s: %v", file_name, err)
		}
		*respsonse = append(*respsonse, string(content))
	}
	return nil
}

func map_task(response *TaskResponse, mapf func(string, string) []KeyValue) {

	file_name := response.TargetFiles[0]
	content := response.TargetFiles[1]

	word_list := mapf(file_name, string(content))
	intermediate := make([][]KeyValue, response.NReduce)

	// hash words into NReduce buckets
	for _, kv := range word_list {
		bucket := ihash(kv.Key) % response.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	// encode bucket to intermediate JSON files
	for bucket, bucket_list := range intermediate {
		oname := fmt.Sprintf("./mr-%d-%d.json", bucket, response.TaskID)
		ofile, err := os.Create(oname)
		if err != nil {
			log_message(oname+" create failed", WARNING)
			continue
		}

		encoder := json.NewEncoder(ofile)
		encoder.Encode(bucket_list)
		ofile.Close()
	}

	// Update status
	notify_request := TaskRequest{
		WorkerID: response.WorkerID,
		TaskID:   response.TaskID,
		Status:   FINISHED,
		TaskType: MAP,
	}
	notify_response := TaskResponse{}
	call("Coordinator.CompleteTask", &notify_request, &notify_response, config.CoordAddr)
}

func reduce_task(response *TaskResponse, reducef func(string, []string) string) {
	if len(response.TargetFiles) == 0 {
		return
	}

	// Load intermediate files
	intermediate := []KeyValue{}
	for i := 0; i < len(response.TargetFiles); i += 2 {

		var worker_intermediate []string
		res := call("WorkerServer.IntermediateFilesRequest", &response.TaskID, &worker_intermediate, response.TargetFiles[i+1])
		if !res {
			// notify coordinator if worker with intermediate files crashes
			notify_request := TaskRequest{
				TaskID:   response.TaskID,
				WorkerID: response.TargetFiles[i],
			}
			var temp int
			call("Coordinator.CrashNofity", &notify_request, &temp, config.CoordAddr)
			return
		}
		for _, file := range worker_intermediate {
			reader := strings.NewReader(file)
			decoder := json.NewDecoder(reader)

			var bucket_list []KeyValue
			decoder.Decode(&bucket_list)
			intermediate = append(intermediate, bucket_list...)
		}
	}

	// Sort intermediate key-value pairs by key
	sort.Sort(ByKey(intermediate))

	// Create output file
	oname := fmt.Sprintf("mr-out-%d", response.TaskID)
	ofile, _ := os.Create(oname)

	// Apply reduce function
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		word_count_list := []string{}
		for k := i; k < j; k++ {
			word_count_list = append(word_count_list, intermediate[k].Value)
		}
		word_count := reducef(intermediate[i].Key, word_count_list)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, word_count)
		i = j
	}

	// Close output file
	ofile.Close()

	// Update status
	notify_request := TaskRequest{
		WorkerID: response.WorkerID,
		TaskID:   response.TaskID,
		Status:   FINISHED,
		TaskType: REDUCE,
	}
	notify_response := TaskResponse{}
	call("Coordinator.CompleteTask", &notify_request, &notify_response, config.CoordAddr)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	configFile, err := os.Open("./config.json")
	if err != nil {
		log.Fatal(err)
	}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)
	configFile.Close()

	// init worker server
	worker_server := WorkerServer{
		Mutex: sync.Mutex{},
	}
	worker_server.server()

	prev_heartbeat := time.Now()
	var response_heartbeat int
	// Your worker implementation here.
	for {
		current_time := time.Now()
		// check heartbeat every 5 seconds
		if current_time.Sub(prev_heartbeat) > 5*time.Second {
			res_heartbeat := call("Coordinator.HeartBeat", &config.WorkerID, &response_heartbeat, config.CoordAddr)
			if !res_heartbeat {
				log.Fatal("Coordinator is down")
			} else {
				prev_heartbeat = current_time
			}
		}

		request := TaskRequest{
			WorkerID: config.WorkerID,
			TaskID:   -1,
			Status:   UNASSIGNED,
			TaskType: -1,
			Address:  config.WorkerAddr,
		}
		response := TaskResponse{}

		res := call("Coordinator.RequestTask", &request, &response, config.CoordAddr)
		if !res {
			log.Fatal("Coordinator is down")
		}

		// worker log assigned task,
		// note: except MAP_IN_PROGRESS(1) and REDUCE_IN_PROGRESS(2), the TaskID in other message is meaningless
		log.Printf("Coordinator reply Status %d, TaskID %d", response.Status, response.TaskID)

		switch response.Status {
		case EXIT:
			os.Exit(0)
		case MAP_IN_PROGRESS:
			map_task(&response, mapf)
		case REDUCE_IN_PROGRESS:
			reduce_task(&response, reducef)
		case WORKER_ID_EXIST:
			continue
		default:
			time.Sleep(1 * time.Second)
		}
		time.Sleep(1 * time.Second)
	}
}
