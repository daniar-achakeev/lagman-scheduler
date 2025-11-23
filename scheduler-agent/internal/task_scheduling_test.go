package internal

import (
	"context"
	"testing"
)

func TestSimpleGraph(t *testing.T) {
	leafTask := NewTaskL("leaf", map[string]bool{"root_1": true, "root_2": true}, &LocalBashRunnable{Command: "echo Hello World && sleep 4"})
	leafTaskSingle := NewTaskL("leaf_single", map[string]bool{"root_2": true}, &LocalBashRunnable{Command: "echo SingleTask && sleep 4"})
	root1 := NewTaskR("root_1", []*Task{leafTask}, &LocalBashRunnable{Command: "echo I am root 1 && sleep 10"})
	root2 := NewTaskR("root_2", []*Task{leafTask, leafTaskSingle}, &LocalBashRunnable{Command: "echo I am root 2 && sleep 5"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//var wg sync.WaitGroup
	errorChan := make(chan error)
	defer close(errorChan)
	resultChan := make(chan Result)
	defer close(resultChan)
	leafTask.Process(ctx, errorChan, resultChan)
	root1.Process(ctx, errorChan, resultChan)
	root2.Process(ctx, errorChan, resultChan)
	leafTaskSingle.Process(ctx, errorChan, resultChan)
	resultsIds := map[string]struct{}{
		"leaf":        {},
		"root_1":      {},
		"root_2":      {},
		"leaf_single": {},
	}
	results := make([]Result, 0, 4)
forLoop:
	for len(resultsIds) > 0 {
		select {
		case res := <-resultChan:
			delete(resultsIds, res.TaskId)
			results = append(results, res)
			t.Log("Result:", res)
		case err := <-errorChan:
			t.Fatalf(" error %v", err)
			break forLoop
		}
	}
	t.Logf("All results \n %v", results)
}
