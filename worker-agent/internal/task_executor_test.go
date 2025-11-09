package internal

import (
	"context"
	"iter"
	"runtime"
	"slices"
	"testing"
	"time"
)

func TestLocalTaskExecutor(t *testing.T) {
	command := "echo Hello World && sleep 1"
	task := Task{
		TaskId:       "123",
		SchedulerId:  "456",
		CallbackIp:   "127.0.0.1",
		CallbackPort: 8080,
		Command:      command,
		DeadlineSec:  3,
	}
	if runtime.GOOS == "linux" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(task.DeadlineSec)*(time.Second))
		defer cancel()
		result := ExecuteCommand(ctx, task)
		if result.Error != nil {
			t.Errorf("Expected no error, got: %v", result.Error)
		}
		if result.ExitCode > 0 {
			t.Errorf("Expected exit code 0, got: %d", result.ExitCode)
		}
		if result.Stdout != "Hello World\n" {
			t.Errorf("Expected stdout 'Hello World\\n', got: %s", result.Stdout)
		}
	}
}

func TestLocalTaskExecutorDeadline(t *testing.T) {
	command := "echo Hello World && sleep 10"
	task := Task{
		TaskId:       "123",
		SchedulerId:  "456",
		CallbackIp:   "127.0.0.1",
		CallbackPort: 8080,
		Command:      command,
		DeadlineSec:  5,
	}
	if runtime.GOOS == "linux" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(task.DeadlineSec)*time.Second)
		defer cancel()
		result := ExecuteCommand(ctx, task)
		if result.Error == nil {
			// expect time deade line error
			t.Errorf("Expected timeout error, got nil")
		}
		if result.ExitCode != -1 {
			t.Errorf("Expected exit code -1 for timeout, got: %d", result.ExitCode)
		}
	}
}

// Mocked Sender
type MockResultSender struct {
	results []Result
}

func NewMockResultSender() *MockResultSender {
	iResults := make([]Result, 0)
	return &MockResultSender{
		results: iResults,
	}
}

func (m *MockResultSender) SubmitResult(ctx context.Context, results iter.Seq[Result]) error {
	for result := range results {
		m.results = append(m.results, result)
	}
	return nil
}

func (m *MockResultSender) SubmitSingleResult(ctx context.Context, result Result) error {
	m.results = append(m.results, result)
	return nil
}

// Mocked DB
type MockResultDB struct {
	results []Result
}

func NewMockResultDB() *MockResultDB {
	iResults := make([]Result, 0)
	return &MockResultDB{
		results: iResults,
	}
}

func (m *MockResultDB) Persist(ctx context.Context, result Result) error {
	m.results = append(m.results, result)
	return nil
}

func (m *MockResultDB) GetPendingResults(ctx context.Context) (iter.Seq[Result], error) {
	return slices.Values(m.results), nil
}

func (m *MockResultDB) DeleteResult(ctx context.Context, taskId string, schedulerId string) error {
	for i, result := range m.results {
		if result.Task.TaskId == taskId && result.Task.SchedulerId == schedulerId {
			m.results = append(m.results[:i], m.results[i+1:]...)
			return nil
		}
	}
	return nil
}

func getTestTasks() []Task {
	return []Task{
		{
			TaskId:       "1",
			SchedulerId:  "S1",
			CallbackIp:   "127.0.0.1",
			CallbackPort: 8080,
			Command:      "echo t1 && sleep 1",
			DeadlineSec:  5,
		},
		{
			TaskId:       "2",
			SchedulerId:  "S1",
			CallbackIp:   "127.0.0.1",
			CallbackPort: 8080,
			Command:      "echo t2 && sleep 2",
			DeadlineSec:  5,
		},
		{
			TaskId:       "3",
			SchedulerId:  "S1",
			CallbackIp:   "127.0.0.1",
			CallbackPort: 8080,
			Command:      "echo t3 && sleep 3",
			DeadlineSec:  5,
		},
	}
}

func TestAsynExecution(t *testing.T) {
	tasks := getTestTasks()
	mockDB := NewMockResultDB()
	if runtime.GOOS == "linux" {
		resultChan := make(chan Result)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, task := range tasks {
			ExecuteCommandAsync(task, resultChan)
		}
		submitTicker := time.NewTicker(1 * time.Second)
		defer submitTicker.Stop()

	outer:
		for {
			// get result from channel
			select {
			case result := <-resultChan:
				mockDB.Persist(ctx, result)
				t.Logf("Exit %s code %d \n", result.Stdout, result.ExitCode)
			case <-submitTicker.C:
				t.Log("submit pending")
			case <-ctx.Done():
				t.Log("Graceful shutdown")
				close(resultChan)
				break outer
			}
		}
		mockDB.GetPendingResults(ctx)
		cnt := 0
		resultTasks := make([]string, 0)
		expected := []string{"1", "2", "3"}
		for idx, result := range mockDB.results {
			resultTasks = append(resultTasks, result.Task.TaskId)
			cnt = idx
		}
		slices.Sort(resultTasks)
		if !slices.Equal(resultTasks, expected) {
			t.Fatalf("not all tasks finished %d", cnt)
		}

	}

}

func TestTaskManager(t *testing.T) {
	tasks := getTestTasks()
	mockDB := NewMockResultDB()
	mockSender := NewMockResultSender()
	if runtime.GOOS == "linux" {
		manager := NewTaskManager(mockDB, mockSender, 1*time.Second, 1)
		manager.Start(context.Background())
		for _, task := range tasks {
			manager.SubmitTask(task)
		}
		time.Sleep(5 * time.Second)
		manager.Stop()
		// they should be all sent
		iter, err := mockDB.GetPendingResults(context.Background())
		if err != nil {
			t.Fatal("DB error ")
			return
		}
		cnt := 0
		for range iter {
			cnt++
		}
		if cnt > 0 {
			t.Fatalf("not all tasks finished %d", cnt)
		}
		if len(mockSender.results) != len(tasks) {
			t.Fatalf("not all tasks sent %d", len(mockSender.results))
		}
		resultTasks := make([]string, 0)
		expected := []string{"1", "2", "3"}
		for _, r := range mockSender.results {
			resultTasks = append(resultTasks, r.Task.TaskId)
			t.Logf("Result %v", r)
		}
		slices.Sort(resultTasks)
		if !slices.Equal(resultTasks, expected) {
			t.Fatalf("not all tasks finished %d", cnt)
		}
	}

}
