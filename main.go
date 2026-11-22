package main

import (
	"errors"
	"fmt"
	"log"
)

func main() {
	repo := NewInMemoryTaskRepo()
	service := NewTaskService(repo)

	fmt.Println("--- Creating Task ---")
	task, err := service.CreateTask("Hello Go World")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Created Task: ID=%s, Status=%s\n", task.ID, task.Status)

	fmt.Println("--- Processing Task ---")
	processedTask, err := service.ProcessTask(task.ID)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Printf("Processed Task Result: Status=%s, Words=%d, Chars=%d\n",
		processedTask.Status, processedTask.Result.WordCount, processedTask.Result.CharCount)

	fmt.Println("--- Concurrent processing with a worker pool ---")
	pool, err := NewWorkerPool(service, DefaultWorkers, DefaultQueueSize)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Started %d workers, queue capacity %d\n", pool.Workers(), pool.QueueCapacity())

	inputs := []string{
		"the cat and the hat",
		"Golang is fast",
		"   ",
		"Привет, мир!",
		"one two three four five six",
	}
	for _, input := range inputs {
		t, err := service.CreateTask(input)
		if err != nil {
			fmt.Printf("Rejected %q: %v\n", input, err)
			continue
		}
		if err := pool.Submit(t.ID); err != nil {
			if errors.Is(err, ErrQueueFull) {
				fmt.Printf("%s: queue is full, try again later\n", t.ID)
				continue
			}
			fmt.Printf("Submit %s failed: %v\n", t.ID, err)
		}
	}

	pool.Shutdown()

	for _, t := range service.ListTasks() {
		fmt.Printf("%s [%s] words=%d chars=%d input=%q\n",
			t.ID, t.Status, t.Result.WordCount, t.Result.CharCount, t.Input)
	}
}
