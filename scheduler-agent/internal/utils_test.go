package internal

import (
	"fmt"
	"testing"
)

func TestStack(t *testing.T) {
	var stack Stack[int]
	if _, ok := stack.Peek(); ok {
		t.Errorf("Stack should be emtpty %v", ok)
	}
	if _, ok := stack.Pop(); ok {
		t.Errorf("Stack should be emtpty %v", ok)
	}
	// PUSH
	stack.Push(1)
	stack.Push(2)
	if pop, ok := stack.Pop(); ok {
		if pop != 2 {
			t.Errorf("stack should pop %d", pop)
		}
	} else {
		t.Errorf("Stack should contain elements %v", ok)
	}
	if peek, ok := stack.Peek(); ok {
		if peek != 1 {
			t.Errorf("stack should pop %d", peek)
		}
	} else {
		t.Errorf("Stack should contain elements %v", ok)
	}
	if pop, ok := stack.Pop(); ok {
		if pop != 1 {
			t.Errorf("stack should pop %d", pop)
		}
	} else {
		t.Errorf("Stack should contain elements %v", ok)
	}
	if _, ok := stack.Peek(); ok {
		t.Errorf("Stack should be emtpty %v", ok)
	}
	if _, ok := stack.Pop(); ok {
		t.Errorf("Stack should be emtpty %v", ok)
	}
	// PUSH again
	stack.Push(3)
	if peek, ok := stack.Peek(); ok {
		if peek != 3 {
			t.Errorf("stack should pop %d", peek)
		}
	} else {
		t.Errorf("Stack should contain elements %v", ok)
	}
	if pop, ok := stack.Pop(); ok {
		if pop != 3 {
			t.Errorf("stack should pop %d", pop)
		}
	} else {
		t.Errorf("Stack should contain elements %v", ok)
	}
}

func getNewTask(id string) *Task {
	return NewTask(id, NewLocalBashRunnable(fmt.Sprintf("echo %s && sleep 1", id), 50))
}

func TestSimpleDFSonDAG(t *testing.T) {
	a := getNewTask("a")
	b := getNewTask("b")
	c := getNewTask("c")
	p := getNewTask("p")
	d := getNewTask("d")
	w := getNewTask("w")
	// a->b, a->d
	a.AddChild(b)
	a.AddChild(d)
	// b -> d, b -> w
	b.AddChild(d)
	b.AddChild(w)
	// c -> p, c ->w
	c.AddChild(d)
	c.AddChild(w)
	// p-> w
	p.AddChild(w)
	// d ->w
	d.AddChild(w)
	// there is a path
	if !CheckDfsDagNodes(a, w) {
		t.Error("path should be there")
	}
	t.Log("\n---------New Check---------\n")
	// no path
	if CheckDfsDagNodes(a, p) {
		t.Error("path should be there")
	}

}

func TestSimpleDFSonDAGDiamondCycle(t *testing.T) {
	a := getNewTask("a")
	b := getNewTask("b")
	c := getNewTask("c")
	d := getNewTask("d")
	e := getNewTask("e")
	w := getNewTask("w")
	// a->b, a->c
	a.AddChild(b)
	a.AddChild(c)
	// b -> d, b -> e
	b.AddChild(c)
	b.AddChild(e)
	// c -> d
	c.AddChild(d)
	// d ->a
	d.AddChild(a)
	e.AddChild(a)
	// there is a path
	if !CheckDfsDagNodes(a, d) {
		t.Error("path should be there")
	}
	t.Log("\n---------New Check---------\n")
	// there is path
	if !CheckDfsDagNodes(b, a) {
		t.Error("path should be there")
	}
	t.Log("\n---------New Check---------\n")
	// there is path
	if CheckDfsDagNodes(a, w) {
		t.Error("path should be there")
	}

}
