# Distributed Implementation (Bonus)

### Store intermediate files locally on worker

- On the mapping stage, every intermediate file is stored on the mapper which produces the file. We are not using any distributed file system like HDFS.

### Communicate via RPC

- All `coordinator <---> worker` and `worker <---> worker` communications are implemented via RPC. This requires all the nodes in the system run on a LAN or have their public IP addresses. 

- File sharing including input texts transfer from coordinator to worker and intermediate files transfer between workers are implemented using RPC(attaching file content to the RPC response). 

- Coordinator maintains a living workers set, so that a worker can learn where it can retrieve the intermediate files from.

### Crashes Tolerance

For crashes on mapping stage, we use the same method as the basic part which track the task processing time. If a timeout occurs, the Coordinator will re-assign the task to a living worker.

For crashes on reducing stage, we use a passive style to handle them. If a worker which stores intermediate files crashes, the whole system will wait until this worker restarts.

### Coordinator Failure

Workers will periodically send heartbeat message to the Coordinator. If the worker fails to connect the Coordinator, then it detects Coordinator failure and exits.