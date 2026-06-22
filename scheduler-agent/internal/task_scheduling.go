package internal

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

type ReturnCode int

const (
	NormalRC ReturnCode = iota //
	ErrorRC
)

type TaskResultStatus int

const (
	Ready TaskResultStatus = iota // 0
	Skipped
	Canceled
	Processed
)

//Simple data model:
// Job Graph: set of steps one 1 to N to Task
// Task: single step has a runnable
// Run: single execution unit of Job Graph Job has 1 to N runs
// Result: job, task, run generic struct to store runs task and job results
// Schedule/Trigger: manual, GraphAsTask, time, loop/chain with offset, ... job could here I need to think

// RunConfig is used to pass job run configuration TODO
type RunConfig struct {
	Id string
}

// JobResult
type JobResult struct {
	JobId      string
	ReturnCode int
	Err        error
	Results    []Result
	StartAt    time.Time
	FinishedAt time.Time
	Canceled   bool
}

// Task Result
type Result struct {
	Id         string //  caller id
	ReturnCode int
	Err        error
	Duration   time.Duration
	FinishedAt time.Time
	Stdout     string
	Stderr     string
	Status     TaskResultStatus
}

// Task internal DAG processing unit uses channel to reflect dependency results
type Task struct {
	id         string
	childNodes []*Task
	runnable   Runnable
	inTaskMap  map[string]bool // this is a map that for each Id has bool value bool onSuccess or not
	// processing fields
	inBoxCh chan Result
	started atomic.Bool // flag if go routine is already started
}

// Basic Factory
func NewTask(id string, runnable Runnable) *Task {
	return &Task{
		id:         id,
		childNodes: make([]*Task, 0),
		runnable:   runnable,
		inTaskMap:  make(map[string]bool),
		inBoxCh:    make(chan Result),
	}
}

// Copies all fields, except childNodes, copies the pointer slice of pointers
func FromTask(task *Task) *Task {
	inmap := make(map[string]bool, len(task.inTaskMap))
	maps.Copy(inmap, task.inTaskMap)
	return &Task{
		id:         task.id,
		runnable:   task.runnable, // copy
		inTaskMap:  inmap,
		inBoxCh:    make(chan Result),
		childNodes: task.childNodes, // pointer copy
	}
}

// returns position of the child
func (t *Task) childIdx(task *Task) int {
	i := -1
	for idx, c := range t.childNodes {
		if c.id == task.id {
			i = idx
			break
		}
	}
	return i
}

func (t *Task) AddChild(task *Task) {
	// add child only if not exists
	if t.childIdx(task) < 0 {
		t.childNodes = append(t.childNodes, task)
	}

}

// Remove child
func (t *Task) RemoveChild(task *Task) {
	if task != nil {
		i := t.childIdx(task)
		l := len(t.childNodes)
		if i >= 0 {
			// overwrite with last kinf of swap
			t.childNodes[i] = t.childNodes[l-1]
			t.childNodes[l-1] = nil // empty task
			// NOTE: do not care about memory of nil pointer numbre of child nodes are not expceted to be high
			t.childNodes = t.childNodes[:l-1]
		}
	}
}

// add dependecy
func (t *Task) AddDependency(taskId string, proceedOnSuccess bool) {
	t.inTaskMap[taskId] = proceedOnSuccess
}

// removes dep.
func (t *Task) RemoveDependency(task *Task) {
	if task != nil {
		delete(t.inTaskMap, task.id)
	}
}

// IsRoot if no dependecies exists
func (t *Task) IsRoot() bool {
	return len(t.inTaskMap) == 0
}

// Process: creates a goroutine for runnable execution, on
func (t *Task) Process(ctx context.Context, resultChan chan<- Result) {
	// Process starts the go routine and after it finishes
	// start dependet process
	if t.started.CompareAndSwap(false, true) {
		go func() {
			// note do not close the channel they will be GCd
			expectedIds := FromStringBoolMap(t.inTaskMap) // local to execution
			results := make([]Result, 0, len(expectedIds))
			runResult := Result{
				Id:         t.id,
				ReturnCode: 0,
			}
			canceled := false
		forloop:
			for len(expectedIds) > 0 {
				// blocks until
				select {
				case res, ok := <-t.inBoxCh:
					if !ok {
						t.inBoxCh = nil
						continue
					}
					// check if alle results returned
					results = append(results, res)
					expectedIds.Remove(res.Id)
				case <-ctx.Done():
					canceled = true
					runResult.Status = Canceled
					break forloop // break early from the loop
				}
			}
			if !canceled {
				if t.checkResults(results) {
					runResult = t.runnable.Run(ctx, t.id) // this can be cancelled
					runResult.Status = Processed
				} else {
					runResult.Status = Skipped
				}
			}
			// write to channel we guarantee to return a result
			resultChan <- runResult
			// check children
			for _, cRun := range t.childNodes {
				// start lazy traverse the graph, nodes has an atomic bool if already visited started
				cRun.Process(ctx, resultChan)
				// child can be canceled
				// should not be a problem we GC no inbox are closed
				cRun.inBoxCh <- runResult
			}
		}()
	} else {
		// TODO impelement log logic
		fmt.Println("task alerady started")
	}

}

func (t *Task) checkResults(results []Result) bool {
	proceed := true
	for _, r := range results {
		if r.Status == Canceled || r.Status == Skipped {
			return false
		}
		proceedOnSuccess := t.inTaskMap[r.Id]
		if proceedOnSuccess && (r.ReturnCode != 0 || r.Err != nil) {
			return false
		}
		if !proceedOnSuccess && (r.ReturnCode == 0) {
			return false
		}
	}
	return proceed
}

// JobGraph  is a DAG of tasks to be executed.
// It supports adding tasks and dependencies, building the graph to ensure no cycles exist, and processing the tasks in order.
// Holds mutex, provides snapshot function for the execution
type JobGraph struct {
	id     string
	nodes  map[string]*Task
	cancel func()       // broadcast cancel from
	rwMux  sync.RWMutex // mutex
}

func NewJobGraph(id string) *JobGraph {
	return &JobGraph{
		id:     id,
		nodes:  map[string]*Task{},
		cancel: nil,
	}
}

// Add Create new Task
func (j *JobGraph) Add(taskId string, runnable Runnable) {
	j.rwMux.Lock()
	defer j.rwMux.Unlock()
	j.nodes[taskId] = NewTask(taskId, runnable)
}

// AddDependency creates egde (task dependecy from source -> sink)
// returns errors:
// if either if source or sink not eixsts
// if we build a cycle in DAG
// cycle check is done via DFS from sink to source
func (j *JobGraph) AddDependency(sourceId string, sinkId string, proceedOnSuccess bool) error {
	j.rwMux.Lock()
	defer j.rwMux.Unlock()
	if _, ok := j.nodes[sourceId]; !ok {
		return fmt.Errorf("sourceId %s not found", sourceId)
	}
	if _, ok := j.nodes[sinkId]; !ok {
		return fmt.Errorf("sinkId %s not found", sinkId)
	}
	// update task defs
	sourceTask := j.nodes[sourceId]
	sinkTask := j.nodes[sinkId]
	// add edge from source to sink
	sourceTask.AddChild(sinkTask)
	// now check backwards if DAG still holds
	if dagOk := CheckDfsDagNodes(sinkTask, sourceTask); dagOk {
		sourceTask.RemoveChild(sinkTask)
		return fmt.Errorf("Cannot add dependecy, creates cycle, %s, %s", sourceTask.id, sinkTask.id)
	}
	sinkTask.AddDependency(sourceTask.id, proceedOnSuccess)
	// check
	return nil
}

// Removes Task and all its edges
func (j *JobGraph) RemoveTask(taskId string) {
	j.rwMux.Lock()
	defer j.rwMux.Unlock()
	if s, ok := j.nodes[taskId]; ok {
		// currently full search
		for id, node := range j.nodes {
			if id == s.id {
				continue
			}
			node.RemoveChild(s)
			node.RemoveDependency(s)
		}
		delete(j.nodes, taskId)
	}
}

// Removes edge between DAG node Tasks
func (j *JobGraph) RemoveDependency(sourceId string, sinkId string) {
	j.rwMux.Lock()
	defer j.rwMux.Unlock()
	s, sok := j.nodes[sourceId]
	si, siok := j.nodes[sinkId]
	if sok {
		s.RemoveChild(si)
	}
	if siok {
		si.RemoveDependency(s)
	}
}

// Snapshot the graph for execution
func (j *JobGraph) Snaphot() map[string]*Task {
	j.rwMux.RLock()         //
	defer j.rwMux.RUnlock() // release
	// copy
	cNodes := make(map[string]*Task, len(j.nodes))
	for key, refTask := range j.nodes {
		// can it happen that I will receive a nil ref?
		if refTask == nil {
			continue
		}
		cNodes[key] = FromTask(refTask) // store ref
	}
	// now copy create new child nodes
	for _, cNode := range cNodes {
		cChilds := make([]*Task, 0, len(cNode.childNodes))
		for _, c := range cNode.childNodes { // from the copy
			if node, ok := cNodes[c.id]; ok {
				cChilds = append(cChilds, node)
			}
		}
		cNode.childNodes = cChilds
	}
	return cNodes
}

// --- Graph Executions ---

// JobGraphRun: is an instance of running the JobGraph defintion
type JobGraphRun struct {
	nodes   map[string]*Task
	graphId string
	config  RunConfig
	cancel  func() // broadcast cancel
}

// Factory fucntion
// the copy of job graph struct
func NewJobGraphRun(config RunConfig, graphId string, nodes map[string]*Task) *JobGraphRun {
	return &JobGraphRun{
		config:  config,
		nodes:   nodes,
		graphId: graphId,
	}
}

// Graph job runner: runs on a current snapshot/copy of the graph
// any changes to a main graph after the run are not reflected
func (j *JobGraphRun) Run(parentCtx context.Context, jobResultChan chan<- JobResult) error {
	go func() {
		startAt := time.Now() // wall clock
		jCtx, cancel := context.WithCancel(parentCtx)
		j.cancel = cancel
		taskResChan := make(chan Result, len(j.nodes))
		defer close(taskResChan)
		defer cancel()
		expectedResultIds := make(map[string]struct{})
		for id := range j.nodes {
			expectedResultIds[id] = struct{}{}
		}
		result := make([]Result, 0, len(j.nodes))
		// start
		for t := range j.getRoots() {
			t.Process(jCtx, taskResChan)
		}
		// receive
		var tErrCantProceed error
		var canceled bool
		returnCode := NormalRC
		// dfg traverse return results
		for len(expectedResultIds) > 0 {
			select {
			case res, ok := <-taskResChan: // fetch task results
				if !ok {
					continue
				}
				delete(expectedResultIds, res.Id)
				result = append(result, res)
			case <-jCtx.Done():
				canceled = true
			}
		}
		jobResultChan <- JobResult{
			JobId:      j.graphId,
			Err:        tErrCantProceed,
			ReturnCode: int(returnCode),
			Results:    result,
			StartAt:    startAt,
			FinishedAt: time.Now(),
			Canceled:   canceled,
		}
	}()
	return nil
}

func (j *JobGraphRun) Cancel() error {
	if j.cancel != nil {
		j.cancel()
		return nil
	}
	return fmt.Errorf("Graph is not ready and context is not set")
}

// internal function
func (j *JobGraphRun) getRoots() iter.Seq[*Task] {
	return func(yield func(*Task) bool) {
		for _, t := range j.nodes {
			if t.IsRoot() {
				if !yield(t) {
					return
				}
			}
		}
	}
}
