package main

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestTaskWorkflow(t *testing.T) {
	repo := NewInMemoryTaskRepo()
	service := NewTaskService(repo)

	t.Run("Successful Task Flow", func(t *testing.T) {
		task, err := service.CreateTask("Golang is fast")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if task.Status != StatusPending {
			t.Errorf("expected Pending, got %s", task.Status)
		}

		processed, err := service.ProcessTask(task.ID)
		if err != nil {
			t.Fatalf("unexpected processing error: %v", err)
		}

		if processed.Status != StatusCompleted {
			t.Errorf("expected Completed, got %s", processed.Status)
		}

		if processed.Result.WordCount != 3 {
			t.Errorf("expected 3 words, got %d", processed.Result.WordCount)
		}
	})

	t.Run("Invalid Inputs Validation", func(t *testing.T) {
		tests := []struct {
			name  string
			input string
		}{
			{"Empty string", ""},
			{"Only spaces", "   "},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := service.CreateTask(tt.input)
				if err == nil {
					t.Error("expected validation error, got nil")
				}
			})
		}
	})

	t.Run("Get Non-existent Task", func(t *testing.T) {
		_, err := repo.GetByID("unknown-id")
		if err == nil {
			t.Error("expected error for missing task, got nil")
		}
	})

	t.Run("Prevent Re-processing", func(t *testing.T) {
		task, _ := service.CreateTask("Sample input")
		_, _ = service.ProcessTask(task.ID)

		_, err := service.ProcessTask(task.ID)
		if err == nil {
			t.Error("expected error when re-processing completed task, got nil")
		}
	})
}

func TestTextAnalyzer_Process(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  TextResult
	}{
		{name: "simple sentence", input: "Hello Go world", want: TextResult{WordCount: 3, CharCount: 14}},
		{name: "extra spaces", input: "  a  b ", want: TextResult{WordCount: 2, CharCount: 7}},
		{name: "unicode counts runes, not bytes", input: "Привет мир", want: TextResult{WordCount: 2, CharCount: 10}},
		{name: "several lines", input: "one two\nthree", want: TextResult{WordCount: 3, CharCount: 13}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TextAnalyzer{}.Process(tt.input)
			if err != nil {
				t.Fatalf("Process(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("Process(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestTextAnalyzer_Process_InvalidInput(t *testing.T) {
	for _, input := range []string{"", "   ", "\n\t"} {
		if _, err := (TextAnalyzer{}).Process(input); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("Process(%q) error = %v, want ErrInvalidInput", input, err)
		}
	}
}

func TestRepo_SaveAndGetReturnCopies(t *testing.T) {
	repo := NewInMemoryTaskRepo()
	original := &Task{ID: "task-1", Input: "text", Status: StatusPending}
	if err := repo.Save(original); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	original.Status = StatusFailed
	got, err := repo.GetByID("task-1")
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if got.Status != StatusPending {
		t.Errorf("Status = %q, want %q (Save must store a copy)", got.Status, StatusPending)
	}

	got.Status = StatusCompleted
	again, _ := repo.GetByID("task-1")
	if again.Status != StatusPending {
		t.Errorf("Status = %q, want %q (GetByID must return a copy)", again.Status, StatusPending)
	}
}

func TestRepo_SaveNil(t *testing.T) {
	if err := NewInMemoryTaskRepo().Save(nil); err == nil {
		t.Fatal("expected an error when saving nil")
	}
}

func TestRepo_GetByID_NotFound(t *testing.T) {
	_, err := NewInMemoryTaskRepo().GetByID("nope")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("error = %v, want ErrTaskNotFound", err)
	}
}

func TestRepo_Update(t *testing.T) {
	repo := NewInMemoryTaskRepo()
	_ = repo.Save(&Task{ID: "task-1", Input: "text", Status: StatusPending})

	t.Run("applies the change and returns a copy", func(t *testing.T) {
		got, err := repo.Update("task-1", func(tk *Task) error {
			tk.Status = StatusQueued
			return nil
		})
		if err != nil || got.Status != StatusQueued {
			t.Fatalf("Update = %+v, %v; want Queued task", got, err)
		}
		stored, _ := repo.GetByID("task-1")
		if stored.Status != StatusQueued {
			t.Errorf("stored Status = %q, want %q", stored.Status, StatusQueued)
		}
	})

	t.Run("a failing update changes nothing", func(t *testing.T) {
		boom := errors.New("boom")
		_, err := repo.Update("task-1", func(tk *Task) error {
			tk.Status = StatusFailed
			return boom
		})
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want boom", err)
		}
		stored, _ := repo.GetByID("task-1")
		if stored.Status != StatusQueued {
			t.Errorf("Status = %q, want %q (failed Update must not change the task)", stored.Status, StatusQueued)
		}
	})

	t.Run("unknown task", func(t *testing.T) {
		_, err := repo.Update("nope", func(*Task) error { return nil })
		if !errors.Is(err, ErrTaskNotFound) {
			t.Fatalf("error = %v, want ErrTaskNotFound", err)
		}
	})
}

func TestRepo_List_KeepsOrder(t *testing.T) {
	repo := NewInMemoryTaskRepo()
	for _, id := range []string{"task-1", "task-2", "task-3"} {
		_ = repo.Save(&Task{ID: id, Input: "x", Status: StatusPending})
	}
	_ = repo.Save(&Task{ID: "task-2", Input: "x", Status: StatusCompleted})

	list := repo.List()
	want := []string{"task-1", "task-2", "task-3"}
	if len(list) != len(want) {
		t.Fatalf("List returned %d tasks, want %d", len(list), len(want))
	}
	for i, id := range want {
		if list[i].ID != id {
			t.Errorf("List()[%d].ID = %s, want %s", i, list[i].ID, id)
		}
	}
}

func TestRepo_ConcurrentUpdatesAreNotLost(t *testing.T) {
	repo := NewInMemoryTaskRepo()
	_ = repo.Save(&Task{ID: "task-1", Input: "", Status: StatusPending})

	const goroutines = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.Update("task-1", func(tk *Task) error {
				tk.Input += "x"
				return nil
			})
			if err != nil {
				t.Errorf("Update failed: %v", err)
			}
		}()
	}
	wg.Wait()

	got, _ := repo.GetByID("task-1")
	if got.Input != strings.Repeat("x", goroutines) {
		t.Errorf("Input has %d characters, want %d (updates were lost)", len(got.Input), goroutines)
	}
}

func TestRepo_ConcurrentAccess(t *testing.T) {
	repo := NewInMemoryTaskRepo()
	const goroutines = 20

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := "task-" + strings.Repeat("a", g+1)
				_ = repo.Save(&Task{ID: id, Input: "x", Status: StatusPending})
				if _, err := repo.GetByID(id); err != nil {
					t.Errorf("GetByID(%s) failed: %v", id, err)
					return
				}
				repo.List()
			}
		}()
	}
	wg.Wait()

	if n := len(repo.List()); n != goroutines {
		t.Errorf("List returned %d tasks, want %d", n, goroutines)
	}
}

func TestService_CreateTask_InvalidInputWrapsSentinel(t *testing.T) {
	s := newService(TextAnalyzer{})
	_, err := s.CreateTask("   ")
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v, want ErrInvalidInput", err)
	}
	if n := len(s.ListTasks()); n != 0 {
		t.Errorf("a rejected task was stored: %d tasks", n)
	}
}

func TestService_CreateTask_UniqueIDsFromManyGoroutines(t *testing.T) {
	s := newService(TextAnalyzer{})
	const goroutines = 20
	const perGoroutine = 50

	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := make(map[string]bool)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				tk, err := s.CreateTask("some text")
				if err != nil {
					t.Errorf("CreateTask failed: %v", err)
					return
				}
				mu.Lock()
				if ids[tk.ID] {
					t.Errorf("duplicate ID %s", tk.ID)
				}
				ids[tk.ID] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(ids) != goroutines*perGoroutine {
		t.Errorf("got %d unique IDs, want %d", len(ids), goroutines*perGoroutine)
	}
	if n := len(s.ListTasks()); n != goroutines*perGoroutine {
		t.Errorf("ListTasks returned %d tasks, want %d", n, goroutines*perGoroutine)
	}
}

func TestService_GetTask(t *testing.T) {
	s := newService(TextAnalyzer{})
	created := mustCreate(t, s, "find me")

	got, err := s.GetTask(created.ID)
	if err != nil || got.Input != "find me" {
		t.Fatalf("GetTask = %+v, %v; want the created task", got, err)
	}
	if _, err := s.GetTask("task-999"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("GetTask(unknown) error = %v, want ErrTaskNotFound", err)
	}
}

func TestService_ProcessTask_ResultAndIndependence(t *testing.T) {
	s := newService(TextAnalyzer{})
	a := mustCreate(t, s, "alpha")
	b := mustCreate(t, s, "beta gamma delta")

	if _, err := s.ProcessTask(b.ID); err != nil {
		t.Fatalf("ProcessTask(b) failed: %v", err)
	}

	gotA := mustGet(t, s, a.ID)
	if gotA.Status != StatusPending || gotA.Result != (TextResult{}) {
		t.Errorf("task a changed although it was not processed: %+v", gotA)
	}
	gotB := mustGet(t, s, b.ID)
	if gotB.Status != StatusCompleted || gotB.Result != (TextResult{WordCount: 3, CharCount: 16}) {
		t.Errorf("task b = %+v, want Completed with 3 words and 16 chars", gotB)
	}
}

func TestService_ProcessTask_InvalidOperations(t *testing.T) {
	t.Run("unknown task", func(t *testing.T) {
		s := newService(TextAnalyzer{})
		if _, err := s.ProcessTask("task-999"); !errors.Is(err, ErrTaskNotFound) {
			t.Fatalf("error = %v, want ErrTaskNotFound", err)
		}
	})

	t.Run("already completed task", func(t *testing.T) {
		s := newService(TextAnalyzer{})
		tk := mustCreate(t, s, "once only")
		if _, err := s.ProcessTask(tk.ID); err != nil {
			t.Fatalf("first ProcessTask failed: %v", err)
		}
		before := mustGet(t, s, tk.ID)
		if _, err := s.ProcessTask(tk.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("second ProcessTask error = %v, want ErrInvalidTransition", err)
		}
		if after := mustGet(t, s, tk.ID); after != before {
			t.Errorf("a rejected operation changed the task: before %+v, after %+v", before, after)
		}
	})

	t.Run("queued task cannot be processed synchronously", func(t *testing.T) {
		s := newService(TextAnalyzer{})
		tk := mustCreate(t, s, "queued")
		if err := s.enqueue(tk.ID); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
		if _, err := s.ProcessTask(tk.ID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("error = %v, want ErrInvalidTransition", err)
		}
	})
}

func TestService_ProcessTask_Failure(t *testing.T) {
	boom := errors.New("boom")
	s := newService(processorFunc(func(string) (TextResult, error) { return TextResult{}, boom }))
	tk := mustCreate(t, s, "will fail")

	if _, err := s.ProcessTask(tk.ID); !errors.Is(err, boom) {
		t.Fatalf("ProcessTask error = %v, want boom", err)
	}
	got := mustGet(t, s, tk.ID)
	if got.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", got.Status, StatusFailed)
	}
	if got.Result != (TextResult{}) {
		t.Errorf("Result = %+v, a failed task must have no result", got.Result)
	}
	if !strings.Contains(got.Error, "boom") {
		t.Errorf("Error = %q, want it to contain %q", got.Error, "boom")
	}
	if _, err := s.ProcessTask(tk.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("reprocessing a failed task: error = %v, want ErrInvalidTransition", err)
	}
}

func TestService_ProcessTask_PanicBecomesFailure(t *testing.T) {
	s := newService(processorFunc(func(string) (TextResult, error) { panic("kaboom") }))
	tk := mustCreate(t, s, "panic text")

	if _, err := s.ProcessTask(tk.ID); err == nil {
		t.Fatal("expected an error from a panicking processor")
	}
	got := mustGet(t, s, tk.ID)
	if got.Status != StatusFailed || !strings.Contains(got.Error, "kaboom") {
		t.Errorf("got %+v, want a Failed task with the panic message", got)
	}
}

func TestService_QueueTransitions(t *testing.T) {
	s := newService(TextAnalyzer{})
	tk := mustCreate(t, s, "transitions")

	if err := s.unqueue(tk.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("unqueue of a Pending task: error = %v, want ErrInvalidTransition", err)
	}
	if err := s.enqueue(tk.ID); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if err := s.enqueue(tk.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("second enqueue: error = %v, want ErrInvalidTransition", err)
	}
	if err := s.unqueue(tk.ID); err != nil {
		t.Fatalf("unqueue failed: %v", err)
	}
	if got := mustGet(t, s, tk.ID); got.Status != StatusPending {
		t.Errorf("Status = %q, want %q", got.Status, StatusPending)
	}
}
