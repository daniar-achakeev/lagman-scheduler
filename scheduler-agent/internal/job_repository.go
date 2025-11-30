package internal

// JobRespository: used for manage the job state
type JobRespository interface {
	GetJobGraph(jobId string) (JobGraph, error)
	// TODO
}
