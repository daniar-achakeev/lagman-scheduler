package internal

// JobRespository: used for manage the job state
type JobRespository interface {
	GetNewJobId() (string, error)
	GetNewTaskId() (string, error)
	GetNewRunId() (string, error)
	GetJob(jobId string) (JobGraph, error)
	//GetTask(taskId string) ()
}
