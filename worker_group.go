package jorb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/b1naryth1ef/jorb/db"
	"golang.org/x/sync/semaphore"
)

type JobRunnable interface {
	Run() error
}

var jobRunnableType = reflect.TypeOf((*JobRunnable)(nil)).Elem()

type WorkerGroup struct {
	sync.Mutex

	client   *Client
	handlers map[string]func(*JobLease) error
	sema     *semaphore.Weighted
}

func NewWorkerGroup(client *Client, concurrency int) *WorkerGroup {
	return &WorkerGroup{
		client:   client,
		handlers: make(map[string]func(*JobLease) error),
		sema:     semaphore.NewWeighted(int64(concurrency)),
	}
}

func (w *WorkerGroup) Handle(handler any) {
	w.Lock()
	defer w.Unlock()

	handlerValue := reflect.ValueOf(handler)
	handlerType := reflect.TypeOf(handler)
	if handlerType.Implements(jobRunnableType) {

		// handler == &T{}
		jobType := handlerType.Elem()
		fn := func(lease *JobLease) error {
			data := reflect.New(jobType)
			method := data.MethodByName("Run")

			err := json.Unmarshal(lease.Job.Data, data.Interface())
			if err != nil {
				return err
			}
			result := method.Call([]reflect.Value{})
			if result[0].IsNil() {
				return nil
			}
			return result[0].Interface().(error)
		}
		name := fmt.Sprintf("%s.%s", jobType.PkgPath(), jobType.Name())
		w.handlers[name] = fn
	} else if handlerType.Kind() == reflect.Func {
		// handler == func (T) error
		if handlerType.NumIn() != 1 {
			log.Panicf("numIn != 1 (%v)", handlerType.NumIn())
		}
		jobType := handlerType.In(0).Elem()

		fn := func(lease *JobLease) error {
			data := reflect.New(jobType)

			err := json.Unmarshal(lease.Job.Data, data.Interface())
			if err != nil {
				return err
			}

			result := handlerValue.Call([]reflect.Value{
				data,
			})
			if result[0].IsNil() {
				return nil
			}
			return result[0].Interface().(error)
		}

		name := fmt.Sprintf("%s.%s", jobType.PkgPath(), jobType.Name())
		w.handlers[name] = fn
	} else {
		log.Panicf("failure %v", handlerType.Kind())
	}

}

var ErrHandlerPanic = errors.New("job handler paniced")

func runSafe(fn func(*JobLease) error, lease *JobLease, result chan error) {
	defer func() {
		if r := recover(); r != nil {
			result <- ErrHandlerPanic
		}
	}()

	result <- fn(lease)
}

func (w *WorkerGroup) wait() {
	typeNames := make([]string, 0, len(w.handlers))
	for typeName := range w.handlers {
		typeNames = append(typeNames, typeName)
	}
	jobLease, err := w.client.WaitManyRaw(typeNames...)
	if err != nil {
		log.Panicf("error: %v", err)
	}

	w.Lock()
	fn := w.handlers[jobLease.Job.TypeName]
	w.Unlock()

	if fn == nil {
		err = jobLease.Close(db.JobState_FAILURE)
		if err != nil {
			log.Printf("Error: failed to close w/ failure: %v", err)
		}
		return
	}

	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-time.After(time.Second * 5):
				w.client.UpdateJobHeartbeat(jobLease.Job.Id)
			case <-done:
				return
			}
		}
	}()

	log.Printf("Started job %v (%v)", jobLease.Job.Id, jobLease.Job.TypeName)
	result := make(chan error, 1)
	runSafe(fn, jobLease, result)
	err = <-result
	close(done)
	log.Printf("Finished job %v (%v) err = %v", jobLease.Job.Id, jobLease.Job.TypeName, err)
	if err != nil {
		err = jobLease.Close(db.JobState_FAILURE)
	} else {
		err = jobLease.Close(db.JobState_SUCCESS)
	}

	if err != nil {
		log.Printf("Error: failed to finish job: %v", err)
	}
}

func (w *WorkerGroup) Run() error {
	for {
		err := w.sema.Acquire(context.Background(), 1)
		if err != nil {
			return err
		}
		go func() {
			defer w.sema.Release(1)
			w.wait()
		}()
	}
}
