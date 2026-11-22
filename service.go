package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type TaskRepository interface {
	Save(task *Task) error

	GetByID(id string) (*Task, error)

	Update(id string, update func(t *Task) error) (*Task, error)

	List() []*Task
}

type InMemoryTaskRepo struct {
	mu    sync.RWMutex
	tasks map[string]*Task
	order []string
}

func NewInMemoryTaskRepo() *InMemoryTaskRepo {
	return &InMemoryTaskRepo{
		tasks: make(map[string]*Task),
	}
}

func (r *InMemoryTaskRepo) Save(task *Task) error {
	if task == nil {
		return errors.New("task cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[task.ID]; !exists {
		r.order = append(r.order, task.ID)
	}
	r.tasks[task.ID] = task.Clone()
	return nil
}

func (r *InMemoryTaskRepo) GetByID(id string) (*Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	task, exists := r.tasks[id]
	if !exists {
		return nil, notFound(id)
	}
	return task.Clone(), nil
}

func (r *InMemoryTaskRepo) Update(id string, update func(t *Task) error) (*Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	stored, exists := r.tasks[id]
	if !exists {
		return nil, notFound(id)
	}

	updated := stored.Clone()
	if err := update(updated); err != nil {
		return nil, err
	}
	r.tasks[id] = updated
	return updated.Clone(), nil
}

func (r *InMemoryTaskRepo) List() []*Task {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*Task, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, r.tasks[id].Clone())
	}
	return out
}

func notFound(id string) error {
	return fmt.Errorf("task with ID %s not found: %w", id, ErrTaskNotFound)
}

type TaskService struct {
	repo      TaskRepository
	processor Processor
	idSeq     atomic.Uint64
}

func NewTaskService(repo TaskRepository) *TaskService {
	return NewTaskServiceWithProcessor(repo, TextAnalyzer{})
}

func NewTaskServiceWithProcessor(repo TaskRepository, p Processor) *TaskService {
	return &TaskService{repo: repo, processor: p}
}

func (s *TaskService) CreateTask(input string) (*Task, error) {
	task := &Task{
		Input:  input,
		Status: StatusPending,
	}

	if err := task.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	task.ID = fmt.Sprintf("task-%d", s.idSeq.Add(1))

	if err := s.repo.Save(task); err != nil {
		return nil, err
	}

	return task, nil
}

func (s *TaskService) GetTask(id string) (*Task, error) {
	return s.repo.GetByID(id)
}

func (s *TaskService) ListTasks() []*Task {
	return s.repo.List()
}

func (s *TaskService) ProcessTask(id string) (*Task, error) {
	return s.process(id, StatusPending)
}

func (s *TaskService) process(id string, from TaskStatus) (*Task, error) {
	var input string
	_, err := s.repo.Update(id, func(t *Task) error {
		if t.Status != from {
			return invalidTransition(t, StatusProcessing)
		}
		t.Status = StatusProcessing
		input = t.Input
		return nil
	})
	if err != nil {
		return nil, err
	}

	result, perr := safeProcess(s.processor, input)

	task, err := s.repo.Update(id, func(t *Task) error {
		if t.Status != StatusProcessing {
			return invalidTransition(t, StatusCompleted)
		}
		if perr != nil {
			t.Status = StatusFailed
			t.Result = TextResult{}
			t.Error = perr.Error()
			return nil
		}
		t.Result = result
		t.Error = ""
		t.Status = StatusCompleted
		return nil
	})
	if err != nil {
		return nil, err
	}
	if perr != nil {
		return nil, fmt.Errorf("process task %s: %w", id, perr)
	}
	return task, nil
}

func (s *TaskService) enqueue(id string) error {
	_, err := s.repo.Update(id, func(t *Task) error {
		if t.Status != StatusPending {
			return invalidTransition(t, StatusQueued)
		}
		t.Status = StatusQueued
		return nil
	})
	return err
}

func (s *TaskService) unqueue(id string) error {
	_, err := s.repo.Update(id, func(t *Task) error {
		if t.Status != StatusQueued {
			return invalidTransition(t, StatusPending)
		}
		t.Status = StatusPending
		return nil
	})
	return err
}

func (s *TaskService) processQueued(id string) error {
	_, err := s.process(id, StatusQueued)
	return err
}
