package internal

import (
	"context"
	"fmt"
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

// Runnable context aware could be canceld due to timeouts or errors
type Runnable interface {
	Run(ctx context.Context, taskId string) Result
	DryRun(ctx context.Context, taskId string) bool
	// TODO Serialize to byte slice
	// TODO Deserialize from byte slice
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

type LocalBashRunnable struct {
	Command          string
	maxCharStdOutErr int
}

func NewLocalBashRunnable(command string, maxCharStdOutErr int) *LocalBashRunnable {
	return &LocalBashRunnable{
		Command:          command,
		maxCharStdOutErr: maxCharStdOutErr,
	}
}

func (l *LocalBashRunnable) DryRun(ctx context.Context, taskId string) bool {
	return true
}

func (l *LocalBashRunnable) Run(ctx context.Context, taskId string) Result {
	var cmd *exec.Cmd
	commandStr := l.Command
	startTime := time.Now()
	if runtime.GOOS == windows {
		// windows /C close flag
		cmd = exec.CommandContext(ctx, windowsCmd, closeFlagWindows, commandStr)
	} else {
		// linux/mac -c close flag
		cmd = exec.CommandContext(ctx, linuxCmd, closeFlagLinux, commandStr)
	}
	var stdout, stderr HeadWriter
	// TODO where to store stdout and stderr
	// where to log and so on
	stdout = *NewHeadWriter(l.maxCharStdOutErr)
	stderr = *NewHeadWriter(l.maxCharStdOutErr)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	result := Result{}
	if err := cmd.Start(); err != nil {
		result.Err = fmt.Errorf("failed to start command: %w", err)
		return result
	}
	waitErr := cmd.Wait()
	if waitErr != nil {
		if exitError, ok := waitErr.(*exec.ExitError); ok {
			// Command failed with a non-zero exit code
			result.ReturnCode = exitError.ExitCode()
			result.Err = fmt.Errorf("command exited with code %d", result.ReturnCode)
		} else if ctx.Err() == context.DeadlineExceeded {
			// Command was killed due to timeout
			result.ReturnCode = -1 // Use a custom code for timeout
			result.Err = fmt.Errorf("command timed out: %w", ctx.Err())
		} else {
			// Other execution errors (e.g., shell not found)
			result.Err = fmt.Errorf("command execution failed: %w", waitErr)
		}
	} else {
		// Command successful (Exit code 0)
		result.ReturnCode = 0
	}
	// Capture the output regardless of success or failure
	result.Id = taskId
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	result.Duration = time.Since(startTime)
	result.FinishedAt = time.Now()
	return result
}
