package main

import (
	"fmt"
	"sync"
)

const (
	MinWorkers = 2

	DefaultWorkers = 3

	DefaultQueueSize = 10
)

type WorkerPool struct {
	service *TaskService
	queue   chan string
	workers int

	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

func NewWorkerPool(service *TaskService, workers, queueSize int) (*WorkerPool, error) {
	switch {
	case service == nil:
		return nil, fmt.Errorf("%w: service is nil", ErrInvalidConfig)
	case workers < MinWorkers:
		return nil, fmt.Errorf("%w: need at least %d workers, got %d", ErrInvalidConfig, MinWorkers, workers)
	case queueSize < 1:
		return nil, fmt.Errorf("%w: queue size must be positive, got %d", ErrInvalidConfig, queueSize)
	}

	p := &WorkerPool{service: service, queue: make(chan string, queueSize), workers: workers}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p, nil
}

func (p *WorkerPool) Workers() int { return p.workers }

func (p *WorkerPool) QueueCapacity() int { return cap(p.queue) }

func (p *WorkerPool) Submit(id string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrPoolStopped
	}

	if err := p.service.enqueue(id); err != nil {
		return err
	}

	select {
	case p.queue <- id:
		return nil
	default:
		if err := p.service.unqueue(id); err != nil {
			return err
		}
		return ErrQueueFull
	}
}

func (p *WorkerPool) Shutdown() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()

	p.wg.Wait()
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for id := range p.queue {
		_ = p.service.processQueued(id)
	}
}
