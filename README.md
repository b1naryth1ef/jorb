# jorb

jorb is a lightweight background job queue for golang using postgres as a backing store

## example

```go
type SomeEvent struct {
	Data SomeData
	Nonce int
}

// basic dispatch
client, err := jorb.NewClient("postgres dsn")
client.Dispatch(&SomeEvent{Data: ..., Nonce: 1})

// basic wait for a job
var job SomeEvent
lease, err := client.Wait(&job)
lease.Close(db.JobState_SUCCESS)

// higher level interface for managing jobs concurrently
group := jorb.NewWorkerGroup(client, 4)
group.Handle(func (job *SomeEvent) error {
	return nil
})
group.Run()

// Register and handle events based on the `JobRunnable` interface
type AnotherEvent struct {}
func (a *AnotherEvent) Run() error {
	return nil
}
group.Handle(&AnotherEvent{})
```
