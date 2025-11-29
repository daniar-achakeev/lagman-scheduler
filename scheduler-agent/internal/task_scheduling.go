package internal

import (
	"context"
	"fmt"
)

type InEdge struct {
	proceedOnSuceess bool
	sinkId           string
}

type JobGraph struct {
	id       string
	nodes    map[string]Runnable
	inEdges  map[string][]InEdge
	outEdges map[string][]string
}

func NewJobGraph(id string) *JobGraph {
	return &JobGraph{
		id:       id,
		nodes:    map[string]Runnable{},
		outEdges: map[string][]string{}, // from source to list of sinks
		inEdges:  map[string][]InEdge{}, // reverse edges needed for dependecies
	}
}

func (j *JobGraph) Add(taskId string, runnable Runnable) {
	j.nodes[taskId] = runnable
}

func (j *JobGraph) AddDependsOn(sourceId string, sinkId string, proceedOnSuccess bool) error {
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
	return nil
}

func (j *JobGraph) GetTopologicalOrder() ([]string, error) {
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
	roots := map[string]struct{}{}
	for c := range j.nodes {
		if _, ok := j.inEdges[c]; !ok {
			roots[c] = struct{}{}
		}
	}
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
		return nil, fmt.Errorf("graph has at least one cycle")
	}
	return tOrderedNodes, nil
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

func NewTaskL(id string, inTaskMap map[string]bool, run Runnable) *Task {
	return &Task{
		id:          id,
		InBox:       make(chan Result),
		childNodes:  []*Task{},
		runnable:    run,
		inTaskMap:   inTaskMap, // empty map
		expectedIds: FromTaskIdMap(inTaskMap),
	}
}

func NewTaskI(id string, inTaskMap map[string]bool, childNodes []*Task, run Runnable) *Task {
	return &Task{
		id:          id,
		InBox:       make(chan Result),
		childNodes:  childNodes,
		runnable:    run,
		inTaskMap:   inTaskMap,
		expectedIds: FromTaskIdMap(inTaskMap),
	}
}

func NewTaskR(id string, childNodes []*Task, run Runnable) *Task {
	return &Task{
		id:          id,
		InBox:       make(chan Result),
		childNodes:  childNodes,
		runnable:    run,
		inTaskMap:   map[string]bool{},
		expectedIds: NewTaskIdSet(),
	}
}

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
				t.expectedIds.Remove(res.TaskId)
			case <-ctx.Done():
				fmt.Println("context termination!")
				break forLoop
			}
		}
		canProceed := t.checkResults(results)
		if canProceed {
			runResult := t.runnable.Run(ctx, t.id)
			resultChan <- runResult
			for _, child := range t.childNodes {
				ch := child.InBox
				ch <- runResult
			}
			// TODO: store to DB
		} else {
			errorChan <- fmt.Errorf("ends with error %v", results)
		}
	}()
}

func (t *Task) checkResults(results []Result) bool {
	proceed := true
	for _, r := range results {
		proceedOnSuccess := t.inTaskMap[r.TaskId]
		if proceedOnSuccess && (r.ReturnCode != 0 || r.Err != nil) {
			return false
		}
		if !proceedOnSuccess && (r.ReturnCode == 0) {
			return false
		}
	}
	return proceed
}
