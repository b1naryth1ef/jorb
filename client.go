package jorb

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/b1naryth1ef/jorb/db"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type notificationHubHandler struct {
	fn func(*pq.Notification)
}

type notificationHub struct {
	sync.RWMutex
	li       *pq.Listener
	handlers map[*notificationHubHandler]struct{}
}

func newNotificationHub(dsn string) (*notificationHub, error) {
	li := db.OpenListener(dsn)
	err := li.Listen("jobs_queue")
	if err != nil {
		return nil, err
	}
	err = li.Listen("jobs_results")
	if err != nil {
		return nil, err
	}
	return &notificationHub{
		li:       li,
		handlers: make(map[*notificationHubHandler]struct{}),
	}, nil
}

func (n *notificationHub) handle(fn func(*pq.Notification)) func() {
	n.Lock()
	defer n.Unlock()
	handler := &notificationHubHandler{fn: fn}
	n.handlers[handler] = struct{}{}

	return func() {
		n.Lock()
		defer n.Unlock()
		delete(n.handlers, handler)
	}
}

func (n *notificationHub) run() {
	for {
		notification := <-n.li.Notify
		n.RLock()
		for handler := range n.handlers {
			handler.fn(notification)
		}
		n.RUnlock()
	}
}

type Client struct {
	conn *sqlx.DB
	hub  *notificationHub
}

func NewClient(dsn string) (*Client, error) {
	conn := db.OpenDB(dsn)
	hub, err := newNotificationHub(dsn)
	if err != nil {
		return nil, err
	}
	go hub.run()
	return &Client{
		conn: conn,
		hub:  hub,
	}, nil
}

func TypeName(v any) string {
	typ := reflect.TypeOf(v).Elem()
	return fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
}

var ErrJobInvalidType = errors.New("job is not a pointer to a struct")

type DispatchOpts struct {
	Unique    bool
	Priority  uint8
	DependsOn []string
}

func (c *Client) Get(id string) (*db.Job, error) {
	return db.GetJob(c.conn, id)
}

func (c *Client) GetRecent() ([]db.Job, error) {
	return db.GetRecentJobs(c.conn)
}

func (c *Client) notify(queue, data any) error {
	_, err := c.conn.Exec("SELECT pg_notify($1, $2);", queue, data)
	return err
}

func (c *Client) Dispatch(jobData any, opts *DispatchOpts) (string, error) {
	jobType := reflect.TypeOf(jobData)
	if jobType.Kind() != reflect.Pointer || jobType.Elem().Kind() != reflect.Struct {
		log.Panicf("ERROR: job data is not a pointer to a struct, you fucked up! %v / %v", jobData, jobType)
		return "", ErrJobInvalidType
	}
	jobType = jobType.Elem()

	name := fmt.Sprintf("%s.%s", jobType.PkgPath(), jobType.Name())
	job := db.NewJob(name, jobData)

	if opts.DependsOn != nil {
		job.DependsOn = pq.StringArray(opts.DependsOn)
	}
	job.Priority = opts.Priority

	var err error
	if opts.Unique {
		err = db.CreateJobUnique(c.conn, job)
	} else {
		err = db.CreateJob(c.conn, job)
	}
	if err != nil {
		return "", err
	}

	err = c.notify("jobs_queue", name)
	if err != nil {
		return job.Id, err
	}

	return job.Id, nil
}

func (c *Client) WaitManyRaw(names ...string) (*JobLease, error) {
	wait := make(chan struct{}, 1)
	cleanup := c.hub.handle(func(n *pq.Notification) {
		if n.Channel == "jobs_queue" {
			for _, name := range names {
				if name == n.Extra {
					select {
					case wait <- struct{}{}:
					default:
					}
					return
				}
			}
		}
	})
	defer cleanup()

	for {
		job, err := db.AcquirePendingJob(c.conn, names)
		if err != nil {
			return nil, err
		}

		if job != nil {
			return NewJobLease(c, job)
		}

		select {
		case <-wait:
		case <-time.After(time.Second * 10):
		}
	}
}

func (c *Client) CompleteJob(jobId string, state db.JobState) error {
	err := db.CompleteJob(c.conn, jobId, state)
	if err != nil {
		return err
	}

	return c.notify("jobs_results", jobId)
}

func (c *Client) UpdateJobHeartbeat(jobId string) error {
	return db.UpdateJobHeartbeat(c.conn, jobId)
}

func (c *Client) WaitId(jobId string) (*db.Job, error) {
	wait := make(chan struct{}, 1)
	cleanup := c.hub.handle(func(n *pq.Notification) {
		if n.Channel == "jobs_results" && n.Extra == jobId {
			select {
			case wait <- struct{}{}:
			default:
			}
		}
	})
	defer cleanup()

	return c.Get(jobId)
}

func (c *Client) Wait(jobPtr any) (*JobLease, error) {
	jobType := reflect.TypeOf(jobPtr)

	if jobType.Kind() != reflect.Pointer || jobType.Elem().Kind() != reflect.Struct {
		return nil, ErrJobInvalidType
	}
	jobType = jobType.Elem()
	name := fmt.Sprintf("%s.%s", jobType.PkgPath(), jobType.Name())

	wait := make(chan struct{}, 1)
	cleanup := c.hub.handle(func(n *pq.Notification) {
		if n.Channel == "jobs_queue" && n.Extra == name {
			select {
			case wait <- struct{}{}:
			default:
			}
		}
	})
	defer cleanup()

	for {
		job, err := db.AcquirePendingJob(c.conn, []string{name})
		if err != nil {
			return nil, err
		}

		if job != nil {
			err = json.Unmarshal(job.Data, jobPtr)
			if err != nil {
				return nil, err
			}

			return NewJobLease(c, job)
		}

		select {
		case <-wait:
		case <-time.After(time.Second * 10):

		}
	}
}
