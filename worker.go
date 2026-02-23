package main

import (
	"context"
	"log"
	"sync"
)

func StartWorkers(ctx context.Context, jobs <-chan Contact, wg *sync.WaitGroup) {
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go worker(ctx, i, jobs, wg)
	}
	log.Printf("🚀 Started %d workers", WorkerCount)
}

func worker(ctx context.Context, id int, jobs <-chan Contact, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("👷 Worker %d started", id)

	for contact := range jobs {
		log.Printf("[Worker %d] Processing contact: %s", id, contact.Phone)
		ProcessContact(ctx, contact)
	}

	log.Printf("👷 Worker %d finished", id)
}
