package internal

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSimpleGraph(t *testing.T) {
	// r1 -> leaf
	// r2 -> leaf
	// r2 -> leafSingle
	leafTask := NewTask("l1", NewLocalBashRunnable("echo leaf && sleep 1", 50))
	leafSingle := NewTask("l2", NewLocalBashRunnable("echo leaf_single && sleep 2", 50))
	root1 := NewTask("r1", NewLocalBashRunnable("echo root_1 && sleep 4", 50))
	root2 := NewTask("r2", NewLocalBashRunnable("echo root_2 && sleep 1", 50))
	root1.AddChild(leafTask)
	root2.AddChild(leafTask)
	root2.AddChild(leafSingle)
	leafTask.AddDependency("r1", true)
	leafTask.AddDependency("r2", true)
	leafSingle.AddDependency("r2", true)
	ctx := t.Context()
	resultChan := make(chan Result)
	defer close(resultChan)
	// start leaf before roots test
	leafTask.Process(ctx, resultChan)
	leafSingle.Process(ctx, resultChan)
	// start roots
	t.Log("Start r1")
	root1.Process(ctx, resultChan)
	t.Log("Start r2")
	root2.Process(ctx, resultChan)
	resultsIds := map[string]struct{}{
		"l1": {},
		"r1": {},
		"r2": {},
		"l2": {},
	}
	results := make([]Result, 0, 4)
	for len(resultsIds) > 0 {
		res := <-resultChan
		delete(resultsIds, res.Id)
		results = append(results, res)
		t.Log("Result:", res)
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
	err = job.AddDependency("A", "B", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("B", "D", true)
	if err != nil {
		t.Fatal(err)
	}
	err = job.AddDependency("B", "C", true)
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
	if err == nil {
		t.Fatal("Cycle expected", err)
	}
	// cycle
	err = job.AddDependency("W", "A", true)
	if err == nil {
		t.Fatal("Cycle expected", err)
	}
}

func TestSnapshot(t *testing.T) {
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
	tasks := job.Snaphot()
	for key, task := range tasks {
		if jt, ok := job.nodes[key]; ok {
			// compare tasks
			tids := make([]string, 0)
			tidsSource := make([]string, 0)
			for _, ch := range task.childNodes {
				tids = append(tids, ch.id)
			}
			for _, ch := range jt.childNodes {
				tidsSource = append(tidsSource, ch.id)
			}
			if !slices.Equal(tids, tidsSource) {
				t.Fatal("child nodes were not copied")
			}
			taskRun, ok := task.runnable.(*LocalBashRunnable)
			if !ok {
				t.Fatal("false runnable ")
			}
			taskRunJ, ok := jt.runnable.(*LocalBashRunnable)
			if !ok {
				t.Fatal("false runnable ")
			}
			if taskRun != taskRunJ {
				t.Fatal("Runnables  are not copied")
			}
			// check inMaps
			for idt, suc := range task.inTaskMap {
				if sucjt, ok := jt.inTaskMap[idt]; ok {
					if suc != sucjt {
						t.Fatal("in task map false success elem", idt, suc, sucjt)
					}
				} else {
					t.Fatal("in task map not copied", task.id, idt)
				}
			}
		} else {
			t.Fatal("key and node is not copied", key) //
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
	// run job
	ctx := t.Context()
	//var wg sync.WaitGroup
	jResultChan := make(chan JobResult)
	defer close(jResultChan)
	jobRun := NewJobGraphRun(RunConfig{Id: "run_1"}, job.id, job.Snaphot())
	err := jobRun.Run(ctx, jResultChan)
	if err != nil {
		t.Fatal("Job start failed")
	}
	stdOutResultsExp := []string{"A", "C", "B", "P", "D", "W"}
	stdOutResults := make([]string, 0, len(stdOutResultsExp))
	jres := <-jResultChan
	// check result
	for _, r := range jres.Results {
		stdOutResults = append(stdOutResults, strings.TrimSpace(r.Stdout))
	}
	for _, e := range stdOutResultsExp {
		if ok := slices.Contains(stdOutResults, e); !ok {
			t.Fatalf("expected result not found %s", e)
		}
	}
}

func TestTaskCancelation(t *testing.T) {
	job := NewJobGraph("ProcessGraph")
	job.Add("A", NewLocalBashRunnable("echo A && sleep 1", 50))
	job.Add("B", NewLocalBashRunnable("echo B && sleep 5", 50))
	job.Add("C", NewLocalBashRunnable("echo C && sleep 1", 50))
	job.Add("D", NewLocalBashRunnable("echo D && sleep 1", 50))
	job.AddDependency("A", "C", true)
	job.AddDependency("A", "D", true)
	job.AddDependency("B", "D", true)
	job.AddDependency("B", "C", true)
	ctx := t.Context()
	jResultChan := make(chan JobResult)
	defer close(jResultChan)
	jobRun := NewJobGraphRun(RunConfig{Id: "run_1"}, job.id, job.Snaphot())
	err := jobRun.Run(ctx, jResultChan)
	if err != nil {
		t.Fatal("Job start failed")
	}
	time.Sleep(1 * time.Second)
	jobRun.Cancel()
	jres := <-jResultChan
	t.Log("JobRun", jobRun, "Result", jres)
	for _, res := range jres.Results {
		t.Log("ID", res.Id, "status", res.Status, "stdout", res.Stdout)
		if res.Id == "A" || res.Id == "B" {
			if res.Status != Processed {
				t.Fatal("wrong status")
			}
		}
		if res.Id == "C" || res.Id == "D" {
			if res.Status != Canceled {
				t.Fatal("wrong status")
			}
		}
	}

}
