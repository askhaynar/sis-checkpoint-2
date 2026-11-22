# SIS – text analysis tasks (Checkpoint 1 + Checkpoint 2)

Individual project for the Golang Application Development course.

* **Checkpoint 1:** tasks, validation, in-memory storage, synchronous processing.
* **Checkpoint 2:** concurrent processing with a worker pool and a bounded queue.

## What the application does

A **task** is a unit of work: a piece of text that must be analysed.
Processing a task (`TextAnalyzer`) produces a `TextResult`:

* `WordCount` – number of words;
* `CharCount` – number of characters (runes, spaces included).

Example: `"Hello Go World"` -> 3 words, 14 characters.

**Validation:** input that is empty or contains only whitespace is rejected
(`ErrInvalidInput`).

## Project layout

| File              | Contents                                                                 |
|-------------------|--------------------------------------------------------------------------|
| `task.go`         | `Task`, `TaskStatus`, `TextResult`, validation, errors, `Processor`, `TextAnalyzer` |
| `service.go`      | `TaskRepository`, thread-safe `InMemoryTaskRepo`, `TaskService`          |
| `pool.go`         | `WorkerPool`: bounded queue + worker goroutines                          |
| `main.go`         | demo application                                                         |
| `service_test.go` | Checkpoint 1 tests, analyzer, repository and service tests               |
| `pool_test.go`    | worker pool and concurrency tests                                        |

## How tasks are created and submitted

```go
repo := NewInMemoryTaskRepo()
service := NewTaskService(repo)
pool, _ := NewWorkerPool(service, 3, 10) // 3 workers, queue capacity 10

task, err := service.CreateTask("Hello Go World") // validated, ID "task-1", Pending
err = pool.Submit(task.ID)                        // enters the queue, returns at once
pool.Shutdown()                                   // finish accepted tasks, stop workers
task, _ = service.GetTask(task.ID)                // Completed, Result is filled
```

`service.ProcessTask(id)` still processes a Pending task synchronously in the
calling goroutine (Checkpoint 1 behaviour). `GetTask` and `ListTasks` return
**copies**, so they can be called at any time, even while tasks are processed.

## Concurrency design

```
             Submit                bounded channel of task IDs (capacity 10)
caller(s) ----------> Pending->Queued   +----------------------+
                                        | id | id | id | ...    |
                                        +----------------------+
                                           |       |       |
                                           v       v       v
                                       Worker 1 Worker 2 Worker 3
                                           \       |       /
                                            v      v      v
                          TaskRepository (map + tasks, one RWMutex)
```

* **Workers:** the demo uses `DefaultWorkers = 3`; `NewWorkerPool` requires at
  least `MinWorkers = 2`. Workers are started once and are reused: each one runs
  `for id := range queue` until the queue is closed. No goroutine is created per
  task.
* **Queue:** a buffered `chan string` of task IDs, demo capacity
  `DefaultQueueSize = 10`. Only IDs travel through the channel; task data stays
  in the repository.
* **Full queue:** `Submit` never blocks. It returns `ErrQueueFull` and the task
  goes back to `Pending`, so it can be submitted again later.
* **Order:** tasks can finish in any order; nothing depends on it.

## Task lifecycle

```
Pending --Submit--> Queued --worker takes it--> Processing --+--> Completed
   ^                  |                                      |
   +--queue is full---+                                      +--> Failed
```

| From       | To         | When                                                |
|------------|------------|-----------------------------------------------------|
| Pending    | Queued     | `WorkerPool.Submit` accepted the task               |
| Queued     | Pending    | `Submit` found the queue full (rollback)            |
| Pending    | Processing | `TaskService.ProcessTask` (synchronous processing)  |
| Queued     | Processing | a worker took the task from the queue               |
| Processing | Completed  | processing succeeded, `Result` is stored            |
| Processing | Failed     | the processor returned an error or panicked         |

`Completed` and `Failed` are final (no retries). Submitting or processing a task
in any other state returns `ErrInvalidTransition`.

## How shared data is synchronized

* `InMemoryTaskRepo` has one `sync.RWMutex`. It protects the map, the order
  slice **and** the fields of every stored task (locking only the map would not
  protect the fields that workers modify). Readers take `RLock`, writers `Lock`.
* The repository never gives out internal pointers: `Save` stores a copy,
  `GetByID`, `Update` and `List` return copies.
* State changes use `repo.Update(id, func(t *Task) error)`: the status check and
  the change happen in **one critical section**, so two goroutines can never
  start the same task. If the function returns an error, nothing is changed.
* The processor runs **without** any lock, so slow tasks do not block readers or
  other workers.
* Task IDs come from `atomic.Uint64`, so they are unique under concurrency.
* `WorkerPool.mu` (`RWMutex`) protects the `closed` flag. `Submit` holds `RLock`
  while it sends to the queue (non-blocking `select`); `Shutdown` takes `Lock` to
  set the flag and close the channel. So sending on a closed channel is
  impossible. Lock order is always `WorkerPool.mu` -> repository lock and the
  repository never calls the pool, so there is no deadlock.
* A panic inside a processor is recovered and turned into `Failed`, so a worker
  never dies and a task never stays `Processing` forever.

## How workers are stopped

`WorkerPool.Shutdown()`:

1. takes the write lock, sets `closed = true` and closes the queue (the only
   place where it is closed, and only once);
2. waits on a `sync.WaitGroup` until all workers exit.

Workers keep reading until the closed channel is **drained**, so every accepted
task is processed – nothing is lost. `Submit` after `Shutdown` returns
`ErrPoolStopped`. Calling `Shutdown` again (or concurrently) is safe: it just
waits again.

## Run and test

```bash
go run .
gofmt -l .          # must print nothing
go vet ./...
go test ./...
go test -race ./...
```
