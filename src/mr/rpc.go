package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Add your RPC definitions here.

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	NONE
)

type TaskStatus int

const (
	IDLE TaskStatus = iota
	PROGRESS
	DONE
)

type TaskArgs struct {
	WorkerID int
}

type TaskReply struct {
	TaskType
	File      string
	TaskIndex int
	NMap      int
	NReduce   int
}

type ReportTaskArgs struct {
	TaskType
	TaskIndex int
}

type ReportTaskReply struct {
	OK bool
}
