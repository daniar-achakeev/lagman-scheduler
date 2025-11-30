package internal

import (
	"slices"
	"strings"
	"testing"
)

func TestSimpleGraph(t *testing.T) {
	// r1 -> leaf
	// r2 -> leaf
	// r2 -> leafSingle
	leafTask := NewTask("leaf", NewLocalBashRunnable("echo leaf && sleep 1", 50))
	leafSingle := NewTask("leaf_single", NewLocalBashRunnable("echo leaf_single && sleep 2", 50))
	root1 := NewTask("root_1", NewLocalBashRunnable("echo root_1 && sleep 4", 50))
	root2 := NewTask("root_2", NewLocalBashRunnable("echo root_2 && sleep 1", 50))
	root1.AddChild(leafTask)
	root2.AddChild(leafTask)
	root2.AddChild(leafSingle)
	leafTask.AddDependency("root_1", true)
	leafTask.AddDependency("root_2", true)
	leafSingle.AddDependency("root_2", true)
	ctx := t.Context()
	//var wg sync.WaitGroup
	errorChan := make(chan error)
	defer close(errorChan)
	resultChan := make(chan Result)
	defer close(resultChan)
	leafTask.Process(ctx, errorChan, resultChan)
	root1.Process(ctx, errorChan, resultChan)
	root2.Process(ctx, errorChan, resultChan)
	leafSingle.Process(ctx, errorChan, resultChan)
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
			delete(resultsIds, res.Id)
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
	err := job.AddDependency("A", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("B", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("C", "P", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("P", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("D", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	// cycle
	err = job.AddDependency("W", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.BuildGraph()
	if err == nil || job.isReady {
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
	err := job.AddDependency("A", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("B", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("C", "P", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("P", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("D", "W", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.BuildGraph()
	t.Log(job.tasks)
	if err != nil {
		t.Fatalf("there is a cycle in a graph but it should not %v", err)
	}
	if !job.isReady {
		t.Fatalf("graph has no cycles, but it is not ready")
	}
	topOrderedSeq := make([]string, 0, len(job.tasks))
	for _, t := range job.tasks {
		topOrderedSeq = append(topOrderedSeq, t.id)
	}
	expected := []string{"A", "B", "C", "D", "P", "W"}
	for _, e := range expected {
		ok := slices.Contains(topOrderedSeq, e)
		if !ok {
			t.Fatalf("node %s not in sequence", e)
		}
	}
}

func TestJobGraphProcess(t *testing.T) {
	job := NewJobGraph("ProcessGraph")
	job.Add("A", NewLocalBashRunnable("echo A && sleep 1", 50))
	job.Add("B", NewLocalBashRunnable("echo B && sleep 2", 50))
	job.Add("C", NewLocalBashRunnable("echo C && sleep 1", 50))
	job.Add("P", NewLocalBashRunnable("echo P && sleep 1", 50))
	job.Add("D", NewLocalBashRunnable("echo D && sleep 3", 50))
	job.Add("W", NewLocalBashRunnable("echo W && sleep 1", 50))
	job.AddDependency("A", "D", true)
	job.AddDependency("B", "D", true)
	job.AddDependency("C", "P", true)
	job.AddDependency("P", "W", true)
	job.AddDependency("D", "W", true)
	if err := job.BuildGraph(); err != nil {
		t.Fatalf("there is a cycle in a graph but it should not %v", err)
	}
	jErrorChan := make(chan error)
	defer close(jErrorChan)
	jResultChan := make(chan JobResult)
	defer close(jResultChan)
	//
	ctx := t.Context()
	if err := job.Run(ctx, RunConfig{Id: "run_1"}, jErrorChan, jResultChan); err != nil {
		t.Logf("should be ready %v", err)
	}
	stdOutResultsExp := []string{"A", "C", "B", "P", "D", "W"}
	stdOutResults := make([]string, 0, len(stdOutResultsExp))
	select {
	case jres := <-jResultChan:
		// check result
		for _, r := range jres.Results {
			stdOutResults = append(stdOutResults, strings.TrimSpace(r.Stdout))
		}
		for _, e := range stdOutResultsExp {
			if ok := slices.Contains(stdOutResults, e); !ok {
				t.Fatalf("expected result not found %s", e)
			}
		}
		break
	case err := <-jErrorChan:
		t.Fatalf("error %v", err)
		break
	}
}
