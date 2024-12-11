package mr

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkerStatus struct {
	Status    int
	Address   string
	TimeStamp time.Time
}

type Coordinator struct {
	// Your definitions here.
	Mutex      sync.Mutex
	InputFiles []string

	MapTasks    []TaskResponse
	ReduceTasks []TaskResponse

	Workers map[string]WorkerStatus

	MapRemaining    int
	MapRunning      int
	ReduceRemaining int
	ReduceRunning   int

	NReduce int
	TimeOut time.Duration
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {

	port := flag.Int("port", 1234, "Port to run the HTTP server on (default: 1234)")
	flag.Parse()
	addr := fmt.Sprintf(":%d", *port)

	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", addr)
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) UpdateWorker(request TaskRequest) {
	value, ok := c.Workers[request.WorkerID]
	if !ok {
		c.Workers[request.WorkerID] = WorkerStatus{
			Status:    WAIT,
			Address:   request.Address,
			TimeStamp: time.Now(),
		}
	} else {
		value.TimeStamp = time.Now()
		if value.Status == EXIT {
			value.Status = request.Status
		}
		c.Workers[request.WorkerID] = value
	}
}

func (c *Coordinator) HeartBeat(request *string, response *int) error {
	return nil
}

func (c *Coordinator) CompleteTask(request *TaskRequest, response *TaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.UpdateWorker(*request)

	// check if it is a valid task
	if request.TaskType == MAP && c.MapTasks[request.TaskID].Status != MAP_IN_PROGRESS {
		return nil
	} else if request.TaskType == REDUCE && c.ReduceTasks[request.TaskID].Status != REDUCE_IN_PROGRESS {
		return nil
	}

	//c.Workers[request.WorkerID].Status = FINISHED
	switch request.TaskType {
	case MAP:
		{
			c.MapTasks[request.TaskID].Status = FINISHED
			c.MapRemaining--
			c.MapRunning--

			log.Printf("Map Task %d finished", request.TaskID)
		}
	case REDUCE:
		{
			c.ReduceTasks[request.TaskID].Status = FINISHED
			c.ReduceRemaining--
			c.ReduceRunning--

			log.Printf("Reduce Task %d finished", request.TaskID)
		}
	}

	return nil
}

func (c *Coordinator) RequestTask(request *TaskRequest, response *TaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	c.UpdateWorker(*request)

	// assign a new map task
	if (c.MapRemaining - c.MapRunning) > 0 {
		for i := 0; i < len(c.MapTasks); i++ {
			if c.MapTasks[i].Status == UNASSIGNED {
				c.MapTasks[i].Status = MAP_IN_PROGRESS
				c.MapTasks[i].WorkerID = request.WorkerID
				c.MapTasks[i].TimeStamp = time.Now()
				*response = c.MapTasks[i]

				c.MapRunning++
				break
			}
		}
	} else if c.MapRemaining > 0 { // no map task to assign, still running
		response.Status = WAIT
	} else if (c.ReduceRemaining - c.ReduceRunning) > 0 {

		for i := 0; i < len(c.ReduceTasks); i++ { // assign a new reduce task
			if c.ReduceTasks[i].Status == UNASSIGNED {
				addrs := c.IntermediateAddr()
				if len(addrs) == 0 {
					return nil
				}
				c.ReduceTasks[i].Status = REDUCE_IN_PROGRESS
				c.ReduceTasks[i].WorkerID = request.WorkerID
				c.ReduceTasks[i].TimeStamp = time.Now()
				c.ReduceTasks[i].TargetFiles = addrs
				*response = c.ReduceTasks[i]

				c.ReduceRunning++
				break
			}
		}
	} else if c.ReduceRemaining > 0 { // no reduce task to assign, still running
		response.Status = WAIT
	} else {
		response.Status = EXIT
	}

	log.Printf("Response worker %s %d %d", request.WorkerID, response.Status, response.TaskID)
	return nil
}

func (c *Coordinator) CrashNofity(request *TaskRequest, response *int) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	crashed_id := request.WorkerID
	unfinished_reduce_task_id := request.TaskID
	if c.ReduceTasks[unfinished_reduce_task_id].Status == REDUCE_IN_PROGRESS {
		c.ReduceRunning--
	}
	c.ReduceTasks[unfinished_reduce_task_id].Status = UNASSIGNED

	log.Printf("Worker %s is crahsed", crashed_id)
	c.ResetCrashedTasks(crashed_id)
	value, ok := c.Workers[crashed_id]
	if ok {
		value.Status = EXIT
		c.Workers[crashed_id] = value
	}
	return nil
}

func (c *Coordinator) ResetDeadTasks() {
	var task *[]TaskResponse
	var status int
	var running *int
	if c.MapRemaining > 0 {
		task = &c.MapTasks
		status = MAP_IN_PROGRESS
		running = &c.MapRunning

	} else {
		task = &c.ReduceTasks
		status = REDUCE_IN_PROGRESS
		running = &c.ReduceRunning
	}

	endtime := time.Now()
	for i := 0; i < len(*task); i++ {
		if status == (*task)[i].Status && endtime.Sub((*task)[i].TimeStamp) > c.TimeOut {
			(*task)[i].Status = UNASSIGNED
			(*running)--

			if status == MAP_IN_PROGRESS {
				log.Printf("Map Task %d is reset", i)
			} else {
				log.Printf("Reduce Task %d is reset", i)
			}

		}
	}
}

func (c *Coordinator) ResetCrashedTasks(worker_id string) {
	// reset all map tasks completed by this worker
	if c.MapRemaining < len(c.MapTasks) {
		for i := 0; i < len(c.MapTasks); i++ {
			if worker_id == c.MapTasks[i].WorkerID {
				if c.MapTasks[i].Status == MAP_IN_PROGRESS {
					c.MapRunning--
				}
				c.MapTasks[i].Status = UNASSIGNED
				c.MapRemaining++
			}
		}
	}

	// reset all reduce tasks completed by this worker
	if c.ReduceRemaining < len(c.ReduceTasks) {
		for i := 0; i < len(c.ReduceTasks); i++ {
			if worker_id == c.ReduceTasks[i].WorkerID {
				if c.ReduceTasks[i].Status == REDUCE_IN_PROGRESS {
					c.ReduceRunning--
				}
				c.ReduceTasks[i].Status = UNASSIGNED
				c.ReduceRemaining++
			}
		}
	}
}

func (c *Coordinator) ResetDeadWorkers() {
	endtime := time.Now()
	for key, value := range c.Workers {
		if value.Status != EXIT && endtime.Sub(value.TimeStamp) > c.TimeOut {
			value.Status = EXIT
			c.Workers[key] = value

			c.ResetCrashedTasks(key)
			log.Printf("Worker %s is crahsed", key)
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// clear dead tasks every time tick
	c.ResetDeadTasks()
	c.ResetDeadWorkers()
	return c.MapRemaining == 0 && c.ReduceRemaining == 0
}

func (c *Coordinator) IntermediateAddr() []string {
	var addrs []string
	addr_map := map[string]string{}
	for i := 0; i < len(c.MapTasks); i++ {

		if c.MapTasks[i].Status != FINISHED {
			return addrs
		}
		id := c.MapTasks[i].WorkerID
		_, ok := addr_map[id]
		if !ok {
			value, ok := c.Workers[id]
			if ok {
				if value.Status == EXIT {
					return addrs
				} else {
					addr_map[id] = value.Address
				}
			}
		}
	}
	for k, v := range addr_map {
		addrs = append(addrs, k)
		addrs = append(addrs, v)
	}
	return addrs
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.

	timeout, _ := time.ParseDuration("10s")
	coordinator := Coordinator{
		Mutex:           sync.Mutex{},
		InputFiles:      files,
		MapTasks:        make([]TaskResponse, len(files)),
		ReduceTasks:     make([]TaskResponse, nReduce),
		Workers:         map[string]WorkerStatus{},
		MapRemaining:    len(files),
		MapRunning:      0,
		ReduceRemaining: nReduce,
		ReduceRunning:   0,
		NReduce:         nReduce,
		TimeOut:         timeout,
	}

	// init MapTasks
	for i := range coordinator.MapTasks {
		coordinator.MapTasks[i] = TaskResponse{
			TaskID:      i,
			Status:      UNASSIGNED,
			TargetFiles: []string{files[i]},
			NReduce:     coordinator.NReduce,
			// WorkerID and Timestamp will be generated when task is assigned
		}
		// add file content to the tasks
		content, err := os.ReadFile(files[i])
		if err != nil {
			coordinator.MapTasks[i].TargetFiles = append(coordinator.MapTasks[i].TargetFiles, "")
			continue
		}
		coordinator.MapTasks[i].TargetFiles = append(coordinator.MapTasks[i].TargetFiles, string(content))
	}

	// init ReduceTasks
	for i := range coordinator.ReduceTasks {
		coordinator.ReduceTasks[i] = TaskResponse{
			TaskID: i,
			Status: UNASSIGNED,
			//TargetFiles: IntermediateFiles(i, len(files)),
			NReduce: coordinator.NReduce,
			// WorkerID, Timestamp and TargetFiles will be generated when task is assigned
		}
	}

	coordinator.server()
	return &coordinator
}
