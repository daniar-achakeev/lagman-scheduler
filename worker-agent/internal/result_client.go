package internal

import (
	"context"
	"fmt"
	"time"

	jobpb "github.com/daniar-achakeev/scheduler/pkg-contracts/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRetries     = 5
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 5 * time.Second
)

// Submit result simple callback
// the context would be build and client uppon the config retrieved from a result queue
func SubmitResult(ctx context.Context, resultClient jobpb.ResultsClient, resultRequest *jobpb.SubmitResultRequest) error {
	delay := initialBackoff
	for retry := range maxRetries {
		_, err := resultClient.SubmitResult(ctx, resultRequest)
		if err == nil {
			// success
			return nil
		}
		st, ok := status.FromError(err)
		// If it's a non-gRPC error or a non-retryable gRPC error, fail immediately.
		if !ok || !isRetryable(st.Code()) {
			return fmt.Errorf("non-retryable error submitting result: %w", err)
		}
		// FIXME add logger instead of
		fmt.Printf("Attempt %d failed (Code: %s). Retrying in %v...\n", retry+1, st.Code(), delay)
		// Pause execution for the calculated delay
		select {
		case <-ctx.Done():
			return ctx.Err() // Stop retrying if the overall context is canceled
		case <-time.After(delay):
			// Continue loop after delay
		}
		delay *= 2
		if delay > maxBackoff {
			delay = maxBackoff
		}
	}
	return fmt.Errorf("failed to submit result after %d attempts", maxRetries)
}

// isRetryable checks for common transient network/server errors
func isRetryable(code codes.Code) bool {
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}
