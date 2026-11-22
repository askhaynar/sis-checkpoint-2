package main

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 5 * time.Second

type processorFunc func(input string) (TextResult, error)

func (f processorFunc) Process(input string) (TextResult, error) { return f(input) }

type gate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGate(buffer int) *gate {
	return &gate{started: make(chan struct{}, buffer), release: make(chan struct{})}
}

func (g *gate) Process(input string) (TextResult, error) {
	g.started <- struct{}{}
	<-g.release
	return TextAnalyzer{}.Process(input)
}

func (g *gate) open() {
	g.once.Do(func() { close(g.release) })
}

func (g *gate) waitStarted(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-g.started:
		case <-time.After(testTimeout):
			t.Fatalf("timed out: only %d of %d tasks started processing", i, n)
		}
	}
}

func textWithWords(n int) string {
	return strings.Repeat("word ", n)
}

func newService(p Processor) *TaskService {
	return NewTaskServiceWithProcessor(NewInMemoryTaskRepo(), p)
}

func mustCreate(t *testing.T, s *TaskService, input string) Task {
	t.Helper()
	tk, err := s.CreateTask(input)
	if err != nil {
		t.Fatalf("CreateTask(%q) failed: %v", input, err)
	}
	return *tk
}

func mustGet(t *testing.T, s *TaskService, id string) Task {
	t.Helper()
	tk, err := s.GetTask(id)
	if err != nil {
		t.Fatalf("GetTask(%q) failed: %v", id, err)
	}
	return *tk
}

func submit(t *testing.T, p *WorkerPool, id string) {
	t.Helper()
	if err := p.Submit(id); err != nil {
		t.Fatalf("Submit(%q) failed: %v", id, err)
	}
}

func startPool(t *testing.T, s *TaskService, workers, queueSize int) *WorkerPool {
	t.Helper()
	pool, err := NewWorkerPool(s, workers, queueSize)
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}
	t.Cleanup(pool.Shutdown)
	return pool
}

func shutdownWithTimeout(t *testing.T, p *WorkerPool) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		p.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Shutdown did not return in time (deadlock or stuck worker)")
	}
}

func TestNewWorkerPool_InvalidConfig(t *testing.T) {
	service := newService(TextAnalyzer{})
	tests := []struct {
		name      string
		service   *TaskService
		workers   int
		queueSize int
	}{
		{name: "nil service", service: nil, workers: 2, queueSize: 1},
		{name: "one worker", service: service, workers: 1, queueSize: 1},
		{name: "zero workers", service: service, workers: 0, queueSize: 1},
		{name: "negative workers", service: service, workers: -3, queueSize: 1},
		{name: "zero queue", service: service, workers: 2, queueSize: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := NewWorkerPool(tt.service, tt.workers, tt.queueSize)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewWorkerPool error = %v, want ErrInvalidConfig", err)
			}
			if pool != nil {
				t.Error("NewWorkerPool returned a pool together with an error")
			}
		})
	}
}

func TestPool_Startup(t *testing.T) {
	s := newService(TextAnalyzer{})
	pool := startPool(t, s, 3, 5)

	if pool.Workers() != 3 {
		t.Errorf("Workers() = %d, want 3", pool.Workers())
	}
	if pool.QueueCapacity() != 5 {
		t.Errorf("QueueCapacity() = %d, want 5", pool.QueueCapacity())
	}

	tk := mustCreate(t, s, "hello pool")
	submit(t, pool, tk.ID)
	shutdownWithTimeout(t, pool)
	if got := mustGet(t, s, tk.ID); got.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", got.Status, StatusCompleted)
	}
}

func TestPool_ProcessesMultipleTasksWithCorrectResults(t *testing.T) {
	s := newService(TextAnalyzer{})
	pool := startPool(t, s, 3, 20)

	const n = 20
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		tk := mustCreate(t, s, textWithWords(i+1))
		ids[i] = tk.ID
		submit(t, pool, tk.ID)
	}
	shutdownWithTimeout(t, pool)

	for i, id := range ids {
		got := mustGet(t, s, id)
		if got.Status != StatusCompleted {
			t.Errorf("task %s: Status = %q, want %q", id, got.Status, StatusCompleted)
			continue
		}
		if got.Result.WordCount != i+1 {
			t.Errorf("task %s: WordCount = %d, want %d", id, got.Result.WordCount, i+1)
		}
	}
}

func TestPool_SubmitDoesNotWaitForProcessing(t *testing.T) {
	g := newGate(1)
	s := newService(g)
	pool := startPool(t, s, 2, 2)
	t.Cleanup(g.open)

	tk := mustCreate(t, s, "slow task")
	submit(t, pool, tk.ID)
	g.waitStarted(t, 1)

	got := mustGet(t, s, tk.ID)
	if got.Status != StatusProcessing {
		t.Errorf("Status = %q, want %q", got.Status, StatusProcessing)
	}
	if got.Result != (TextResult{}) {
		t.Errorf("Result = %+v, want an empty result while processing", got.Result)
	}

	g.open()
	shutdownWithTimeout(t, pool)
	if got := mustGet(t, s, tk.ID); got.Status != StatusCompleted || got.Result.WordCount != 2 {
		t.Errorf("after shutdown got %+v, want a Completed task with 2 words", got)
	}
}

func TestPool_ProcessesTasksConcurrently(t *testing.T) {
	g := newGate(2)
	s := newService(g)
	pool := startPool(t, s, 2, 2)
	t.Cleanup(g.open)

	a := mustCreate(t, s, "first task")
	b := mustCreate(t, s, "second task")
	submit(t, pool, a.ID)
	submit(t, pool, b.ID)

	g.waitStarted(t, 2)

	for _, id := range []string{a.ID, b.ID} {
		if got := mustGet(t, s, id); got.Status != StatusProcessing {
			t.Errorf("task %s: Status = %q, want %q", id, got.Status, StatusProcessing)
		}
	}

	g.open()
	shutdownWithTimeout(t, pool)
	for _, id := range []string{a.ID, b.ID} {
		if got := mustGet(t, s, id); got.Status != StatusCompleted {
			t.Errorf("task %s: Status = %q, want %q", id, got.Status, StatusCompleted)
		}
	}
}

func TestPool_NeverExceedsWorkerLimit(t *testing.T) {
	const workers = 3
	const tasks = 8

	var current, maxSeen atomic.Int32
	started := make(chan struct{}, tasks)
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }

	s := newService(processorFunc(func(input string) (TextResult, error) {
		n := current.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		current.Add(-1)
		return TextAnalyzer{}.Process(input)
	}))
	pool := startPool(t, s, workers, tasks)
	t.Cleanup(open)

	ids := make([]string, tasks)
	for i := range ids {
		ids[i] = mustCreate(t, s, textWithWords(i+1)).ID
		submit(t, pool, ids[i])
	}

	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-time.After(testTimeout):
			t.Fatalf("only %d of %d workers started", i, workers)
		}
	}
	open()
	shutdownWithTimeout(t, pool)

	if got := maxSeen.Load(); got != workers {
		t.Errorf("max concurrent tasks = %d, want exactly %d (the number of workers)", got, workers)
	}
	for i, id := range ids {
		if got := mustGet(t, s, id); got.Status != StatusCompleted || got.Result.WordCount != i+1 {
			t.Errorf("task %s: got %+v, want Completed with %d words", id, got, i+1)
		}
	}
}

func TestPool_FullQueueIsRejected(t *testing.T) {
	g := newGate(8)
	s := newService(g)
	pool := startPool(t, s, 2, 2)
	t.Cleanup(g.open)

	tasks := make([]Task, 5)
	for i := range tasks {
		tasks[i] = mustCreate(t, s, textWithWords(i+1))
	}

	submit(t, pool, tasks[0].ID)
	submit(t, pool, tasks[1].ID)
	g.waitStarted(t, 2)
	submit(t, pool, tasks[2].ID)
	submit(t, pool, tasks[3].ID)

	err := pool.Submit(tasks[4].ID)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Submit on a full queue: error = %v, want ErrQueueFull", err)
	}
	if got := mustGet(t, s, tasks[4].ID); got.Status != StatusPending {
		t.Errorf("rejected task: Status = %q, want %q", got.Status, StatusPending)
	}
	for _, i := range []int{2, 3} {
		if got := mustGet(t, s, tasks[i].ID); got.Status != StatusQueued {
			t.Errorf("task %s: Status = %q, want %q", tasks[i].ID, got.Status, StatusQueued)
		}
	}

	g.open()
	deadline := time.Now().Add(testTimeout)
	for {
		err := pool.Submit(tasks[4].ID)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrQueueFull) || time.Now().After(deadline) {
			t.Fatalf("resubmitting the rejected task: error = %v", err)
		}
		runtime.Gosched()
	}

	shutdownWithTimeout(t, pool)
	for i, tk := range tasks {
		got := mustGet(t, s, tk.ID)
		if got.Status != StatusCompleted || got.Result.WordCount != i+1 {
			t.Errorf("task %s: got %+v, want Completed with %d words", tk.ID, got, i+1)
		}
	}
}

func TestPool_InvalidSubmissions(t *testing.T) {
	g := newGate(4)
	s := newService(g)
	pool := startPool(t, s, 2, 4)
	t.Cleanup(g.open)

	t.Run("unknown task", func(t *testing.T) {
		if err := pool.Submit("task-999"); !errors.Is(err, ErrTaskNotFound) {
			t.Fatalf("error = %v, want ErrTaskNotFound", err)
		}
	})

	t.Run("duplicate submission", func(t *testing.T) {
		tk := mustCreate(t, s, "submit me once")
		submit(t, pool, tk.ID)
		g.waitStarted(t, 1)

		if err := pool.Submit(tk.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("error = %v, want ErrInvalidTransition", err)
		}
		if got := mustGet(t, s, tk.ID); got.Status != StatusProcessing {
			t.Errorf("Status = %q, want %q (a rejected submit must not change it)", got.Status, StatusProcessing)
		}
	})

	t.Run("completed task", func(t *testing.T) {
		other := newService(TextAnalyzer{})
		tk := mustCreate(t, other, "already done")
		if _, err := other.ProcessTask(tk.ID); err != nil {
			t.Fatalf("ProcessTask failed: %v", err)
		}
		otherPool := startPool(t, other, 2, 2)
		if err := otherPool.Submit(tk.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("error = %v, want ErrInvalidTransition", err)
		}
	})

	t.Run("failed task", func(t *testing.T) {
		failing := newService(processorFunc(func(string) (TextResult, error) {
			return TextResult{}, errors.New("boom")
		}))
		tk := mustCreate(t, failing, "already failed")
		if _, err := failing.ProcessTask(tk.ID); err == nil {
			t.Fatal("expected ProcessTask to fail")
		}
		failedPool := startPool(t, failing, 2, 2)
		if err := failedPool.Submit(tk.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("error = %v, want ErrInvalidTransition", err)
		}
	})

	g.open()
	shutdownWithTimeout(t, pool)
}

func TestPool_ConcurrentDuplicateSubmit(t *testing.T) {
	var calls atomic.Int32
	s := newService(processorFunc(func(input string) (TextResult, error) {
		calls.Add(1)
		return TextAnalyzer{}.Process(input)
	}))
	pool := startPool(t, s, 4, 50)
	tk := mustCreate(t, s, "only once")

	const goroutines = 30
	var accepted, rejected, unexpected atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			switch err := pool.Submit(tk.ID); {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrInvalidTransition):
				rejected.Add(1)
			default:
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	shutdownWithTimeout(t, pool)

	if accepted.Load() != 1 || rejected.Load() != goroutines-1 || unexpected.Load() != 0 {
		t.Errorf("accepted=%d rejected=%d unexpected=%d, want 1, %d, 0",
			accepted.Load(), rejected.Load(), unexpected.Load(), goroutines-1)
	}
	if calls.Load() != 1 {
		t.Errorf("the task was processed %d times, want exactly once", calls.Load())
	}
	if got := mustGet(t, s, tk.ID); got.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", got.Status, StatusCompleted)
	}
}

func TestPool_ConcurrentRetrievalWhileProcessing(t *testing.T) {
	g := newGate(8)
	s := newService(g)
	pool := startPool(t, s, 3, 10)
	t.Cleanup(g.open)

	const n = 3
	tasks := make([]Task, n)
	for i := range tasks {
		tasks[i] = mustCreate(t, s, textWithWords(i+1))
		submit(t, pool, tasks[i].ID)
	}
	g.waitStarted(t, n)

	var wg sync.WaitGroup
	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				for _, tk := range tasks {
					got, err := s.GetTask(tk.ID)
					if err != nil {
						t.Errorf("GetTask(%s) failed: %v", tk.ID, err)
						return
					}
					if got.Status != StatusProcessing || got.Result != (TextResult{}) || got.Input != tk.Input {
						t.Errorf("GetTask(%s) = %+v, want an untouched Processing task", tk.ID, got)
						return
					}
				}
				if got := len(s.ListTasks()); got != n {
					t.Errorf("ListTasks returned %d tasks, want %d", got, n)
					return
				}
			}
		}()
	}
	wg.Wait()

	g.open()
	shutdownWithTimeout(t, pool)
	for i, tk := range tasks {
		got := mustGet(t, s, tk.ID)
		if got.Status != StatusCompleted || got.Result.WordCount != i+1 {
			t.Errorf("task %s: got %+v, want Completed with %d words", tk.ID, got, i+1)
		}
	}
}

func TestPool_FailedProcessing(t *testing.T) {
	s := newService(processorFunc(func(string) (TextResult, error) {
		return TextResult{}, errors.New("boom")
	}))
	pool := startPool(t, s, 2, 4)
	tk := mustCreate(t, s, "will fail")
	submit(t, pool, tk.ID)
	shutdownWithTimeout(t, pool)

	got := mustGet(t, s, tk.ID)
	if got.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", got.Status, StatusFailed)
	}
	if got.Result != (TextResult{}) {
		t.Errorf("Result = %+v, a failed task must not have a result", got.Result)
	}
	if !strings.Contains(got.Error, "boom") {
		t.Errorf("Error = %q, want it to contain %q", got.Error, "boom")
	}
}

func TestPool_ProcessorPanicDoesNotKillWorkers(t *testing.T) {
	s := newService(processorFunc(func(input string) (TextResult, error) {
		if input == "explode" {
			panic("kaboom")
		}
		return TextAnalyzer{}.Process(input)
	}))
	pool := startPool(t, s, 2, 10)

	var bad, good []string
	for i := 0; i < 6; i++ {
		if i%3 == 0 {
			bad = append(bad, mustCreate(t, s, "explode").ID)
		} else {
			good = append(good, mustCreate(t, s, textWithWords(i)).ID)
		}
	}
	for _, id := range append(append([]string{}, bad...), good...) {
		submit(t, pool, id)
	}
	shutdownWithTimeout(t, pool)

	for _, id := range bad {
		got := mustGet(t, s, id)
		if got.Status != StatusFailed || !strings.Contains(got.Error, "kaboom") {
			t.Errorf("task %s: got %+v, want Failed with the panic message", id, got)
		}
	}
	for _, id := range good {
		if got := mustGet(t, s, id); got.Status != StatusCompleted {
			t.Errorf("task %s: Status = %q, want %q", id, got.Status, StatusCompleted)
		}
	}
}

func TestPool_ShutdownFinishesAcceptedTasks(t *testing.T) {
	g := newGate(8)
	s := newService(g)
	pool := startPool(t, s, 2, 4)
	t.Cleanup(g.open)

	accepted := make([]Task, 6)
	for i := range accepted {
		accepted[i] = mustCreate(t, s, textWithWords(i+1))
	}
	submit(t, pool, accepted[0].ID)
	submit(t, pool, accepted[1].ID)
	g.waitStarted(t, 2)
	for _, tk := range accepted[2:] {
		submit(t, pool, tk.ID)
	}
	late := mustCreate(t, s, "too late")

	done := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(done)
	}()

	deadline := time.Now().Add(testTimeout)
	for {
		err := pool.Submit(late.ID)
		if errors.Is(err, ErrPoolStopped) {
			break
		}
		if !errors.Is(err, ErrQueueFull) || time.Now().After(deadline) {
			t.Fatalf("waiting for shutdown: error = %v", err)
		}
		runtime.Gosched()
	}

	select {
	case <-done:
		t.Fatal("Shutdown returned before the accepted tasks were finished")
	default:
	}

	g.open()
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Shutdown did not return after the tasks were released")
	}

	for i, tk := range accepted {
		got := mustGet(t, s, tk.ID)
		if got.Status != StatusCompleted || got.Result.WordCount != i+1 {
			t.Errorf("accepted task %s was lost: got %+v", tk.ID, got)
		}
	}
	if got := mustGet(t, s, late.ID); got.Status != StatusPending {
		t.Errorf("late task: Status = %q, want %q", got.Status, StatusPending)
	}
}

func TestPool_RejectsSubmissionsAfterShutdown(t *testing.T) {
	s := newService(TextAnalyzer{})
	pool := startPool(t, s, 2, 2)
	tk := mustCreate(t, s, "after shutdown")

	shutdownWithTimeout(t, pool)

	if err := pool.Submit(tk.ID); !errors.Is(err, ErrPoolStopped) {
		t.Fatalf("Submit after Shutdown: error = %v, want ErrPoolStopped", err)
	}
	if got := mustGet(t, s, tk.ID); got.Status != StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, StatusPending)
	}
}

func TestPool_ShutdownTwiceIsSafe(t *testing.T) {
	pool := startPool(t, newService(TextAnalyzer{}), 2, 2)

	shutdownWithTimeout(t, pool)
	shutdownWithTimeout(t, pool)
}

func TestPool_ConcurrentSubmitAndShutdown(t *testing.T) {
	s := newService(TextAnalyzer{})
	pool := startPool(t, s, 3, 5)

	const n = 100
	ids := make([]string, n)
	for i := range ids {
		ids[i] = mustCreate(t, s, textWithWords(i+1)).ID
	}

	errs := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = pool.Submit(ids[i])
		}()
	}
	for k := 0; k < 3; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			pool.Shutdown()
		}()
	}
	close(start)
	wg.Wait()

	for i, id := range ids {
		got := mustGet(t, s, id)
		switch {
		case errs[i] == nil:
			if got.Status != StatusCompleted || got.Result.WordCount != i+1 {
				t.Errorf("accepted task %s was lost or wrong: %+v", id, got)
			}
		case errors.Is(errs[i], ErrQueueFull), errors.Is(errs[i], ErrPoolStopped):
			if got.Status != StatusPending {
				t.Errorf("rejected task %s: Status = %q, want %q", id, got.Status, StatusPending)
			}
		default:
			t.Errorf("task %s: unexpected Submit error: %v", id, errs[i])
		}
	}
}

func TestPool_WorkersTerminateAfterShutdown(t *testing.T) {
	before := runtime.NumGoroutine()

	s := newService(TextAnalyzer{})
	pool, err := NewWorkerPool(s, 8, 4)
	if err != nil {
		t.Fatalf("NewWorkerPool failed: %v", err)
	}
	tk := mustCreate(t, s, "leak check")
	submit(t, pool, tk.ID)
	shutdownWithTimeout(t, pool)

	deadline := time.Now().Add(testTimeout)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines leaked: %d before, %d after Shutdown", before, runtime.NumGoroutine())
		}
		runtime.Gosched()
	}
}
