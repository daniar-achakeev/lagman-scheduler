package internal

import (
	"context"
	"iter"
)

// interface to store results
type ResultDB interface {
	Persist(ctx context.Context, result Result) error
	// Returns non sent results
	GetPendingResults(ctx context.Context) (iter.Seq[Result], error)
	//Delete Result
	DeleteResult(ctx context.Context, taskId string, schedulerId string) error
}
