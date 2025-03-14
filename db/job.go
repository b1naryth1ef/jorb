package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type Job struct {
	Id          string         `json:"id"`
	TypeName    string         `json:"type_name"`
	Priority    uint8          `json:"priority"`
	State       JobState       `json:"state"`
	Data        []byte         `json:"data"`
	DependsOn   pq.StringArray `json:"depends_on"`
	ScheduledAt *time.Time     `json:"scheduled_at"`
	StartedAt   *time.Time     `json:"started_at"`
	HeartbeatAt *time.Time     `json:"heartbeat_at"`
	FinishedAt  *time.Time     `json:"finished_at"`
}

type JobState = string

const (
	JobState_PENDING JobState = "pending"
	JobState_RUNNING          = "running"
	JobState_FAILURE          = "failure"
	JobState_SUCCESS          = "success"
)

func NewJob(typeName string, data any) *Job {
	dataJSON, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	return &Job{
		Id:        uuid.NewString(),
		TypeName:  typeName,
		State:     JobState_PENDING,
		Data:      dataJSON,
		DependsOn: pq.StringArray{},
	}
}

func GetJob(db *sqlx.DB, id string) (*Job, error) {
	var result Job

	row := db.QueryRowx("SELECT * FROM jobs WHERE id=$1", id)
	err := row.StructScan(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func GetRecentJobs(db *sqlx.DB) ([]Job, error) {
	rows, err := db.Queryx("SELECT * FROM jobs ORDER BY finished_at DESC, scheduled_at DESC LIMIT 1000")
	if err != nil {
		return nil, err
	}
	result := []Job{}
	for rows.Next() {
		var j Job
		err = rows.StructScan(&j)
		if err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	return result, nil
}

type execTgt interface {
	NamedExec(query string, arg interface{}) (sql.Result, error)
}

func createJob(tgt execTgt, job *Job) error {
	_, err := tgt.NamedExec(
		`INSERT INTO jobs
			(id, type_name, priority, state, data, depends_on, scheduled_at, heartbeat_at)
		VALUES
			(:id, :type_name, :priority, :state, :data, :depends_on, :scheduled_at, now())
		`,
		job,
	)
	return err
}

func CreateJob(db *sqlx.DB, job *Job) error {
	return createJob(db, job)
}

func CreateJobUnique(db *sqlx.DB, job *Job) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}

	// TODO: scheduled_at is not considered
	res, err := tx.NamedQuery("SELECT id FROM jobs WHERE type_name=:type_name AND priority=:priority AND state IN ('pending', 'running') AND jsonb_hash(data)=jsonb_hash(:data)", job)
	if err != nil {
		tx.Rollback()
		return err
	}
	for res.Next() {
		return tx.Rollback()
	}

	err = createJob(tx, job)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func CompleteJob(db *sqlx.DB, id string, state JobState) error {
	_, err := db.Exec(
		"UPDATE jobs SET state=$1, finished_at=now() WHERE id=$2",
		state,
		id,
	)
	if err != nil {
		return err
	}
	_, err = db.Exec(
		"UPDATE jobs SET depends_on=array_remove(depends_on, $1) WHERE $1=ANY(depends_on)",
		id,
	)
	return err
}

func UpdateJobHeartbeat(db *sqlx.DB, id string) error {
	_, err := db.Exec(
		"UPDATE jobs SET heartbeat_at=$1 WHERE id=$2 AND state=$3",
		time.Now(),
		id,
		JobState_RUNNING,
	)
	return err
}

func AcquirePendingJob(db *sqlx.DB, typeNames []string) (*Job, error) {
	arg := map[string]interface{}{
		"typeNames":   typeNames,
		"beforeState": JobState_PENDING,
		"afterState":  JobState_RUNNING,
	}
	query, args, err := sqlx.Named(
		`UPDATE jobs SET state=:afterState, started_at=now() WHERE id IN (
			SELECT id FROM jobs
			WHERE
				type_name IN (:typeNames)
				AND state=:beforeState
				AND (scheduled_at IS NULL OR scheduled_at < now())
				AND cardinality(depends_on) = 0
			ORDER BY priority DESC, scheduled_at ASC
			LIMIT 1
			FOR UPDATE
		) RETURNING *`,
		arg,
	)
	query, args, err = sqlx.In(query, args...)
	query = db.Rebind(query)

	res := db.QueryRowx(
		query, args...,
	)

	var job Job
	err = res.StructScan(&job)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &job, nil
}

func CancelStalledJobs(db *sqlx.DB) error {
	_, err := db.Exec(
		"UPDATE jobs SET state=$1, finished_at=now() WHERE state=$2 AND heartbeat_at IS NOT NULL and heartbeat_at < NOW() - INTERVAL '1 minutes'",
		JobState_FAILURE,
		JobState_RUNNING,
	)
	return err
}

func PruneJobs(db *sqlx.DB, olderThan time.Time) error {
	_, err := db.Exec(
		"DELETE FROM jobs WHERE finished_at < $1",
		olderThan,
	)
	return err
}

func GetJobStateStats(db *sqlx.DB) (map[JobState]uint64, error) {
	rows, err := db.Queryx("SELECT count(*) as cnt, state FROM jobs GROUP BY state")
	if err != nil {
		return nil, err
	}

	type Item struct {
		Cnt   uint64   `json:"cnt"`
		State JobState `json:"state"`
	}

	result := make(map[JobState]uint64)
	for rows.Next() {
		var count uint64
		var state JobState
		err = rows.Scan(&count, &state)
		if err != nil {
			return nil, err
		}

		result[state] = count
	}
	return result, nil
}
