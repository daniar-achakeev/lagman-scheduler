package internal

import (
	"context"
	"slices"
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

func TestJobGraphSortCycle(t *testing.T) {
	job := NewJobGraph("GraphCycle")
	job.Add("A", &LocalBashRunnable{Command: "echo A && sleep 1"})
	job.Add("B", &LocalBashRunnable{Command: "echo B && sleep 1"})
	job.Add("C", &LocalBashRunnable{Command: "echo C && sleep 1"})
	job.Add("P", &LocalBashRunnable{Command: "echo P && sleep 1"})
	job.Add("D", &LocalBashRunnable{Command: "echo D && sleep 1"})
	job.Add("W", &LocalBashRunnable{Command: "echo W && sleep 1"})
	err := job.AddDependsOn("A", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("B", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("C", "P", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("P", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("D", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	// cycle
	err = job.AddDependsOn("W", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = job.GetTopologicalOrder()
	if err == nil {
		t.Fatalf("there is a cycle in a graph %v", err)
	}
}

func TestJobGraphSort(t *testing.T) {
	job := NewJobGraph("GraphCycle")
	job.Add("A", &LocalBashRunnable{Command: "echo A && sleep 1"})
	job.Add("B", &LocalBashRunnable{Command: "echo B && sleep 1"})
	job.Add("C", &LocalBashRunnable{Command: "echo C && sleep 1"})
	job.Add("P", &LocalBashRunnable{Command: "echo P && sleep 1"})
	job.Add("D", &LocalBashRunnable{Command: "echo D && sleep 1"})
	job.Add("W", &LocalBashRunnable{Command: "echo W && sleep 1"})
	err := job.AddDependsOn("A", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("B", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("C", "P", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("P", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependsOn("D", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	orderedSeq, err := job.GetTopologicalOrder()
	t.Log(orderedSeq)
	if err != nil {
		t.Fatalf("there is a cycle in a graph but it should not %v", err)
	}
	expected := []string{"A", "B", "C", "D", "P", "W"}
	for _, e := range expected {
		ok := slices.Contains(orderedSeq, e)
		if !ok {
			t.Fatalf("node %s not in sequence", e)
		}
	}
}
