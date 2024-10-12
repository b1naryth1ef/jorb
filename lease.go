package jorb

import (
	"encoding/json"

	"github.com/b1naryth1ef/jorb/db"
)

type JobLease struct {
	Job    *db.Job
	Client *Client
}

func NewJobLease(client *Client, job *db.Job) (*JobLease, error) {
	return &JobLease{
		Job:    job,
		Client: client,
	}, nil
}

func (j *JobLease) Data(targetPtr any) error {
	return json.Unmarshal(j.Job.Data, targetPtr)
}

func (j *JobLease) Close(state db.JobState) error {
	return j.Client.CompleteJob(j.Job.Id, state)
}
