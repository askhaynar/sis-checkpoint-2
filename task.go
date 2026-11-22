package main

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	ErrInvalidInput = errors.New("invalid input")

	ErrTaskNotFound = errors.New("task not found")

	ErrInvalidTransition = errors.New("invalid state transition")

	ErrQueueFull = errors.New("task queue is full")

	ErrPoolStopped = errors.New("worker pool is stopped")

	ErrInvalidConfig = errors.New("invalid worker pool configuration")
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "Pending"
	StatusQueued     TaskStatus = "Queued"
	StatusProcessing TaskStatus = "Processing"
	StatusCompleted  TaskStatus = "Completed"
	StatusFailed     TaskStatus = "Failed"
)

type TextResult struct {
	WordCount int `json:"word_count"`
	CharCount int `json:"char_count"`
}

type Task struct {
	ID     string     `json:"id"`
	Input  string     `json:"input"`
	Status TaskStatus `json:"status"`
	Result TextResult `json:"result"`
	Error  string     `json:"error,omitempty"`
}

func (t *Task) Validate() error {
	if strings.TrimSpace(t.Input) == "" {
		return fmt.Errorf("%w: input text cannot be empty or contain only spaces", ErrInvalidInput)
	}
	return nil
}

func (t *Task) Clone() *Task {
	c := *t
	return &c
}

func invalidTransition(t *Task, to TaskStatus) error {
	return fmt.Errorf("%w: task %s is %s, cannot move to %s", ErrInvalidTransition, t.ID, t.Status, to)
}

type Processor interface {
	Process(input string) (TextResult, error)
}

type TextAnalyzer struct{}

func (TextAnalyzer) Process(input string) (TextResult, error) {
	if strings.TrimSpace(input) == "" {
		return TextResult{}, fmt.Errorf("%w: input text cannot be empty or contain only spaces", ErrInvalidInput)
	}
	return TextResult{
		WordCount: len(strings.Fields(input)),
		CharCount: utf8.RuneCountInString(input),
	}, nil
}

func safeProcess(p Processor, input string) (res TextResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			res = TextResult{}
			err = fmt.Errorf("processor panicked: %v", r)
		}
	}()
	return p.Process(input)
}
