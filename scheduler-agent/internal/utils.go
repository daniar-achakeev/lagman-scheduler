package internal

import "fmt"

// sorts topologically the graph
// returns nil as error and non empty topologically sorted tasks
// if there are cycles returns error and nil for the slice
func TopologicalSort(nodes map[string]*Task, inEdges map[string][]string, outEdges map[string][]string) ([]*Task, error) {
	tOrderedNodes := make([]string, 0, len(nodes))
	// copy the graph
	graphOutCopy := map[string]map[string]struct{}{}
	graphInCopy := map[string]map[string]struct{}{}
	for k, v := range outEdges {
		m := map[string]struct{}{}
		for _, n := range v {
			m[n] = struct{}{}
		}
		graphOutCopy[k] = m
	}
	for k, v := range inEdges {
		m := map[string]struct{}{}
		for _, n := range v {
			m[n] = struct{}{}
		}
		graphInCopy[k] = m
	}
	// find roots
	roots := map[string]struct{}{}
	for tid := range nodes {
		if _, ok := inEdges[tid]; !ok {
			roots[tid] = struct{}{}
		}
	}
	//
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
		// cycles
		return nil, fmt.Errorf("graph has at least one cycle")
	}
	tasks := make([]*Task, 0, len(tOrderedNodes))
	for _, n := range tOrderedNodes {
		tasks = append(tasks, nodes[n])
	}
	return tasks, nil
}

// simple DFS on DAG
// DAG is implicit via parent child rel.
func CheckDfsDagNodes(s *Task, v *Task) bool {
	//short circuite
	if s.id == v.id {
		return true
	}
	// set for visisted nods
	visisted := make(map[string]struct{})
	var stack Stack[*Task]
	stack.Push(s)
	visisted[s.id] = struct{}{}
	for len(stack) != 0 {
		if t, ok := stack.Pop(); ok {
			childNodes := t.childNodes
			// expand
			for _, c := range childNodes {
				if c.id == v.id {
					return true
				}
				if _, ok := visisted[c.id]; !ok {
					stack.Push(c)
					visisted[c.id] = struct{}{}
				}
			}
		}
	}
	return false
}

// --- Stack
type Stack[T any] []T

// push
func (s *Stack[T]) Push(e T) {
	*s = append(*s, e)
}

// get top element
func (s *Stack[T]) Pop() (T, bool) {
	topIdx := len(*s) - 1
	var zero T
	if topIdx >= 0 {
		e := (*s)[topIdx]
		(*s)[topIdx] = zero // free pointer still nil holds address 0
		*s = (*s)[:topIdx]  // overwrite the pointer
		return e, true
	}
	return zero, false
}

// peek
func (s *Stack[T]) Peek() (T, bool) {
	topIdx := len(*s) - 1
	if topIdx >= 0 {
		return (*s)[topIdx], true
	}
	var zero T
	return zero, false
}

// --- String set

// StringSet:  Helper set structure
type StringSet map[string]struct{}

func NewStringSet() StringSet {
	return make(StringSet)
}

func FromStringBoolMap(inTaskMap map[string]bool) StringSet {
	set := NewStringSet()
	for k := range inTaskMap {
		set.Add(k)
	}
	return set
}

func (s StringSet) Add(element string) {
	s[element] = struct{}{}
}

func (s StringSet) Contains(element string) bool {
	_, exists := s[element]
	return exists
}

func (s StringSet) Remove(element string) {
	delete(s, element)
}
