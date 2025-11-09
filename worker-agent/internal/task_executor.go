package internal

import (
	"context"
	"fmt"
	"iter"
	"os/exec"
	"runtime"
	"time"
)

const (
	windows          = "windows"
	windowsCmd       = "cmd.exe"
	linuxCmd         = "/bin/bash"
	closeFlagLinux   = "-c"
	closeFlagWindows = "/C"
)

// Task model struct
type Task struct {
	TaskId       string
	SchedulerId  string
	CallbackIp   string
	CallbackPort int
	Command      string
	DeadlineSec  int64
}

// Result represents the result of a locally executed task.
type Result struct {
	Task       Task
	Stdout     string // currently stores 1000 chars
	Stderr     string // currently stores 1000 chars
	ExitCode   int
	Error      error
	Duration   time.Duration
	FinishedAt time.Time
}

// ResultSender interface
type ResultSender interface {
	SubmitResult(ctx context.Context, results iter.Seq[Result]) error
	SubmitSingleResult(ctx context.Context, result Result) error
}

// HeadWriter is used for stdtout and stderr
type HeadWriter struct {
	k      int
	buffer []byte
	offset int
}

func NewHeadWriter(k int) *HeadWriter {
	buf := make([]byte, k)
	return &HeadWriter{
		k:      k,
		buffer: buf,
		offset: 0,
	}
}

// implements Writer interface
func (hw *HeadWriter) Write(p []byte) (n int, err error) {
	if hw.offset >= hw.k {
		return 0, nil
	}
	n = copy(hw.buffer[hw.offset:], p)
	hw.offset += n
	return n, nil
}

func (hw *HeadWriter) String() string {
	return string(hw.buffer[:hw.offset])
}

// ExecuteCommand
func ExecuteCommand(ctx context.Context, task Task) Result {
	var cmd *exec.Cmd
	commandStr := task.Command
	startTime := time.Now()
	if runtime.GOOS == windows {
		// windows /C close flag
		cmd = exec.CommandContext(ctx, windowsCmd, closeFlagWindows, commandStr)
	} else {
		// linux/mac -c close flag
		cmd = exec.CommandContext(ctx, linuxCmd, closeFlagLinux, commandStr)
	}
	var stdout, stderr HeadWriter
	stdout = *NewHeadWriter(1000)
	stderr = *NewHeadWriter(1000)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	result := Result{}
	if err := cmd.Start(); err != nil {
		result.Error = fmt.Errorf("failed to start command: %w", err)
		return result
	}
	waitErr := cmd.Wait()
	if waitErr != nil {
		if exitError, ok := waitErr.(*exec.ExitError); ok {
			// Command failed with a non-zero exit code
			result.ExitCode = exitError.ExitCode()
			result.Error = fmt.Errorf("command exited with code %d", result.ExitCode)
		} else if ctx.Err() == context.DeadlineExceeded {
			// Command was killed due to timeout
			result.ExitCode = -1 // Use a custom code for timeout
			result.Error = fmt.Errorf("command timed out: %w", ctx.Err())
		} else {
			// Other execution errors (e.g., shell not found)
			result.Error = fmt.Errorf("command execution failed: %w", waitErr)
		}
	} else {
		// Command successful (Exit code 0)
		result.ExitCode = 0
	}
	// Capture the output regardless of success or failure
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	result.Task = task
	result.Duration = time.Since(startTime)
	result.FinishedAt = time.Now()
	return result
}

// ExecuteCommandAsync async wrapper
func ExecuteCommandAsync(task Task, resultChan chan<- Result) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(task.DeadlineSec)*time.Second)
		defer cancel()
		result := ExecuteCommand(ctx, task)
		resultChan <- result
	}()
}

// TaskManager provides result channel for async tasks
// persists tasks and sends every ... second result back to scheduler
type TaskManager struct {
	resultDb ResultDB
	// result sender
	resultSender ResultSender
	// every x seconds send results to scheduler
	submitInterval time.Duration
	// max number of results to send in one batch
	maxBatchSize int
	// channel for results from executed tasks
	resultChan chan Result
	// channel to stop the task manager
	stopChan chan struct{}
}

func NewTaskManager(resultDb ResultDB, resultSender ResultSender, submitInterval time.Duration, maxBatchSize int) *TaskManager {
	return &TaskManager{
		resultDb:       resultDb,
		resultSender:   resultSender,
		submitInterval: submitInterval,
		maxBatchSize:   maxBatchSize,
		resultChan:     make(chan Result),
		stopChan:       make(chan struct{}),
	}
}

func (tm *TaskManager) Start(ctx context.Context) {
	go tm.processResults(ctx)
}

func (tm *TaskManager) Stop() {
	close(tm.stopChan)
}

func (tm *TaskManager) SubmitTask(task Task) {
	ExecuteCommandAsync(task, tm.resultChan)
}

func (tm *TaskManager) processResults(ctx context.Context) {
	submitTicker := time.NewTicker(tm.submitInterval)
	defer submitTicker.Stop()

	for {
		select {
		case result := <-tm.resultChan:
			// Persist the result to the database
			err := tm.resultDb.Persist(ctx, result)
			if err != nil {
				// TODO: Log the error, perhaps retry persistence
				fmt.Printf("Error persisting result: %v\n", err)
			}
		case <-submitTicker.C:
			// Time to submit results to the scheduler
			err := tm.sendResultsToScheduler(ctx)
			if err != nil {
				// TODO log err retry on a next ticker
				fmt.Printf("Error submitting results: %v\n", err)
			}
		case <-tm.stopChan:
			// TODO add logger
			fmt.Println("Task Manager stopping...")
			// Optionally, send any remaining results before shutting down
			err := tm.sendResultsToScheduler(ctx)
			if err != nil {
				// TODO log err retry on a next ticker
				fmt.Printf("Error submitting results: %v\n", err)
			}
			return
		case <-ctx.Done():
			fmt.Println("Task Manager context cancelled, stopping...")
			err := tm.sendResultsToScheduler(ctx)
			if err != nil {
				// TODO log err retry on a next ticker
				fmt.Printf("Error submitting results: %v\n", err)
			}
			return
		}
	}
}

func (tm *TaskManager) sendResultsToScheduler(ctx context.Context) error {
	resultsIter, err := tm.resultDb.GetPendingResults(ctx)
	if err != nil {
		// add logging
		return fmt.Errorf("get pending results problem")
	}
	// chunc iter in batch sizes
	for task := range resultsIter {
		err := tm.resultSender.SubmitSingleResult(ctx, task)
		if err != nil {
			// log and skip remove
			fmt.Printf("Error submitting result: %v\n", err)
			continue
		}
		tm.resultDb.DeleteResult(ctx, task.Task.TaskId, task.Task.SchedulerId)
	}
	return nil
}
