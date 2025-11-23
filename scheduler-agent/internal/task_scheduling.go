package internal

import (
	"context"
	"fmt"
)

type JobGraph struct {
	Id    string
	roots []*Task
}

func NewJobGraph(id string, roots []*Task) *JobGraph {
	return &JobGraph{
		Id:    id,
		roots: roots,
	}
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
