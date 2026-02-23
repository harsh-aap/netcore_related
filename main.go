package main

import (
	"context"
	"fmt"
	"log"
	"sync"
)

func main() {
	ctx := context.Background()

	// Aggregator waitgroup
	aggWg := sync.WaitGroup{}
	aggWg.Add(2)

	// Start aggregators
	go func() { createAggregator(ctx); aggWg.Done() }()
	go func() { updateAggregator(ctx); aggWg.Done() }()
	log.Println("🚀 Aggregators started")

	// Start workers
	jobs := make(chan Contact, 100)
	wg := sync.WaitGroup{}
	StartWorkers(ctx, jobs, &wg)

	// Feed CSV
	err := ReadCSV(jobs)
	if err != nil {
		log.Fatal("CSV Error:", err)
	}

	// Wait for workers to finish
	close(jobs)
	wg.Wait()
	log.Println("✅ All searches done...")

	// Close queues and wait for aggregators to flush
	close(CreateQueue)
	close(UpdateQueue)
	aggWg.Wait() // ← this is the key line

	fmt.Println("✅ All contacts processed.")
}
