package internal

import (
	"context"
	"fmt"
	"time"
)

//Simple data model:
// Job Graph: set of steps one 1 to N to Task
// Task: single step has a runnable
// Run: single execution unit of Job Graph Job has 1 to N runs
// Result: job, task, run generic struct to store runs task and job results
// Schedule/Trigger: manual, GraphAsTask, time, loop/chain with offset, ... job could here I need to think

// RunConfig is used to pass job run configuration
// each scheduled or manual job triggered run can be then
// associated with the run
// one job graph can have 1 to N runs
// TODO this would be extedend
type RunConfig struct {
	Id string
}

// JobResult
type JobResult struct {
	JobId      string
	ReturnCode int
	Err        error
	Results    []Result
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
}

// TaskIdSet
type TaskIdSet map[string]struct{}

func NewTaskIdSet() TaskIdSet {
	return make(TaskIdSet)
}

func FromTaskIdMap(inTaskMap map[string]bool) TaskIdSet {
	set := NewTaskIdSet()
	for k := range inTaskMap {
		set.Add(k)
	}
	return set
}

func (s TaskIdSet) Add(element string) {
	s[element] = struct{}{}
}

func (s TaskIdSet) Contains(element string) bool {
	_, exists := s[element]
	return exists
}

func (s TaskIdSet) Remove(element string) {
	delete(s, element)
}

// Task internal DAG processing unit uses channel to reflect dependency results
type Task struct {
	id         string
	InBox      chan Result
	childNodes []*Task
	runnable   Runnable
	inTaskMap  map[string]bool // this is a map that for each Id has bool value
	// if true than checkResults function treats this on success to proceed
	// if false than checkResults function treats this on failure to proceed
	expectedIds TaskIdSet
}

func NewTask(id string, runnable Runnable) *Task {
	return &Task{
		id:          id,
		InBox:       make(chan Result),
		childNodes:  make([]*Task, 0),
		runnable:    runnable,
		inTaskMap:   make(map[string]bool),
		expectedIds: NewTaskIdSet(),
	}
}

func (t *Task) AddChild(task *Task) {
	t.childNodes = append(t.childNodes, task)
}

func (t *Task) AddDependency(taskId string, proceedOnSuccess bool) {
	t.expectedIds.Add(taskId)
	t.inTaskMap[taskId] = proceedOnSuccess
}

// TODO
// do we really need a errorChan maye be just wrapp result as non proceedError type
func (t *Task) Process(ctx context.Context, errorChan chan<- error, resultChan chan<- Result) {
	go func() {
		defer close(t.InBox)
		results := make([]Result, 0, len(t.expectedIds))
	forLoop:
		for len(t.expectedIds) > 0 {
			// blocks until
			select {
			case res := <-t.InBox:
				// check if alle results returned
				results = append(results, res)
				t.expectedIds.Remove(res.Id)
			case <-ctx.Done():
				break forLoop
			}
		}
		canProceed := t.checkResults(results)
		if canProceed {
			runResult := t.runnable.Run(ctx, t.id)
			resultChan <- runResult // used by job go routine to e.g. persist results
			for _, child := range t.childNodes {
				ch := child.InBox
				ch <- runResult
			}
		} else {
			errorChan <- fmt.Errorf("ends with error %v", results)
		}
	}()
}

func (t *Task) checkResults(results []Result) bool {
	proceed := true
	for _, r := range results {
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

type InEdge struct {
	proceedOnSuceess bool
	sinkId           string
}

// JobGraph  is a DAG of tasks to be executed.
// It supports adding tasks and dependencies, building the graph to ensure no cycles exist, and processing the tasks in order.
// TODO: pass a DB Interface to store the task results
// TODO: Add delete task method
type JobGraph struct {
	id       string
	nodes    map[string]*Task
	inEdges  map[string][]InEdge
	outEdges map[string][]string
	tasks    []*Task
	isReady  bool
}

func NewJobGraph(id string) *JobGraph {
	return &JobGraph{
		id:       id,
		nodes:    map[string]*Task{},
		outEdges: map[string][]string{}, // from source to list of sinks
		inEdges:  map[string][]InEdge{}, // reverse edges needed for dependecies
		tasks:    make([]*Task, 0),
		isReady:  false,
	}
}

// Add Create new Task
func (j *JobGraph) Add(taskId string, runnable Runnable) {
	j.nodes[taskId] = NewTask(taskId, runnable)
}

// AddDependency creates egde (task dependecy from source -> sink)
func (j *JobGraph) AddDependency(sourceId string, sinkId string, proceedOnSuccess bool) error {
	if _, ok := j.nodes[sourceId]; !ok {
		return fmt.Errorf("sourceId %s not found", sourceId)
	}
	if _, ok := j.nodes[sinkId]; !ok {
		return fmt.Errorf("sinkId %s not found", sinkId)
	}
	_, ok := j.inEdges[sinkId]
	if !ok {
		j.inEdges[sinkId] = make([]InEdge, 0)
	}
	j.inEdges[sinkId] = append(j.inEdges[sinkId], InEdge{sinkId: sourceId, proceedOnSuceess: proceedOnSuccess})
	_, ok = j.outEdges[sourceId]
	if !ok {
		j.outEdges[sourceId] = make([]string, 0)
	}
	j.outEdges[sourceId] = append(j.outEdges[sourceId], sinkId)
	// update task defs
	sourceTask := j.nodes[sourceId]
	sinkTask := j.nodes[sinkId]
	sourceTask.AddChild(sinkTask)
	sinkTask.AddDependency(sourceTask.id, proceedOnSuccess)
	return nil
}

func (j *JobGraph) BuildGraph() error {
	tOrderedNodes := make([]string, 0, len(j.nodes))
	// copy the graph
	graphOutCopy := map[string]map[string]struct{}{}
	graphInCopy := map[string]map[string]struct{}{}
	for k, v := range j.outEdges {
		m := map[string]struct{}{}
		for _, n := range v {
			m[n] = struct{}{}
		}
		graphOutCopy[k] = m
	}
	for k, v := range j.inEdges {
		m := map[string]struct{}{}
		for _, n := range v {
			m[n.sinkId] = struct{}{}
		}
		graphInCopy[k] = m
	}
	// find roots
	roots := j.getRoots()
	for len(roots) > 0 {
		for c := range roots {
			// remove from roots reflected after loop end
			delete(roots, c)
			tOrderedNodes = append(tOrderedNodes, c)
			// expand
			childNodes, ok := graphOutCopy[c]
			if ok {
				delete(graphOutCopy, c)
				for ch := range childNodes {
					// remove reverse edge too
					delete(graphInCopy[ch], c)
					inEdges, ok := graphInCopy[ch]
					if ok && len(inEdges) == 0 { // no in egdes
						delete(graphInCopy, ch)
						roots[ch] = struct{}{}
					}
				}
			}
		}
	}
	if len(graphOutCopy) > 0 {
		// TODO Logging and description the nodes in graph
		return fmt.Errorf("graph has at least one cycle")
	}
	//
	for _, n := range tOrderedNodes {
		j.tasks = append(j.tasks, j.nodes[n])
	}
	j.isReady = true
	return nil
}

func (j *JobGraph) getRoots() map[string]struct{} {
	roots := map[string]struct{}{}
	for c := range j.nodes {
		if _, ok := j.inEdges[c]; !ok {
			roots[c] = struct{}{}
		}
	}
	return roots
}

// Process is a main go routine for a job
// TODO: config management
// number of errors ,...
// persistence layer
// pass run config
func (j *JobGraph) Run(ctx context.Context, runConfig RunConfig, jobErrorChan chan<- error, jobResultChan chan<- JobResult) error {
	// build tasks
	if !j.isReady {
		return fmt.Errorf("Run build function graph is not ready")
	}
	go func() {
		taskErrChan := make(chan error)
		taskResChan := make(chan Result, len(j.tasks))
		defer close(taskErrChan)
		defer close(taskResChan)
		expectedResultIds := make(map[string]struct{})
		for _, t := range j.tasks {
			expectedResultIds[t.id] = struct{}{}
		}
		result := make([]Result, 0, len(j.tasks))
		// start
		for _, t := range j.tasks {
			t.Process(ctx, taskErrChan, taskResChan)
		}
		// receive
		var tErrCantProceed error
	forloop:
		for len(expectedResultIds) > 0 {
			select {
			case res := <-taskResChan:
				delete(expectedResultIds, res.Id)
				result = append(result, res)
			case tErrCantProceed = <-taskErrChan:
				// non proceed task error
				jobErrorChan <- tErrCantProceed
				break forloop
			}
		}
		// all tasks
		// TODO: persist result here (  is it a right place or should we persist on receive?)
		returnCode := 0
		if tErrCantProceed != nil {
			returnCode = 1
		}
		jobResultChan <- JobResult{
			JobId:      j.id,
			Err:        tErrCantProceed,
			ReturnCode: returnCode,
			Results:    result,
		}
	}()
	return nil
}
