package main

import (
	"context"
	"log"
	"time"
)

// ─────────────────────────────────────────────
//  Pending update carries both IDs and contact
// ─────────────────────────────────────────────

type PendingUpdate struct {
	ContactID int
	Contact Contact
}

// ─────────────────────────────────────────────
//  Aggregator channels (package-level singletons)
// ─────────────────────────────────────────────
var (
	CreateQueue = make(chan Contact, 500)
	UpdateQueue = make(chan PendingUpdate, 500)
	FlushTimeout = 60 * time.Second
)

// ─────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────
const (
	BatchSize = 2
)

// ─────────────────────────────────────────────
//  StartAggregators — call once from main()
// ─────────────────────────────────────────────
func StartAggregators(ctx context.Context) {
	go createAggregator(ctx)
	go updateAggregator(ctx)
	log.Println("🚀 Aggregators started (batch size:", BatchSize, "| flush timeout:", FlushTimeout, ")")
}

// ─────────────────────────────────────────────
//  Create aggregator
// ─────────────────────────────────────────────
func createAggregator(ctx context.Context) {
	var batch []Contact
	ticker := time.NewTicker(FlushTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		log.Printf("📦 [CreateAggregator] Flushing %d contacts", len(batch))
		if err := BulkCreateAPI(ctx, batch); err != nil {
			log.Println("❌ [CreateAggregator] Bulk create error:", err)
		}
		batch = batch[:0] // reset slice, keep capacity
	}

	for {
		select {
		case contact, ok := <-CreateQueue:
			if !ok {
				// Channel closed — flush whatever remains and exit
				flush()
				log.Println("✅ [CreateAggregator] Done")
				return
			}
			batch = append(batch, contact)
			if len(batch) >= BatchSize {
				flush()
				ticker.Reset(FlushTimeout)
			}

		case <-ticker.C:
			flush()

		case <-ctx.Done():
			flush()
			log.Println("⚠ [CreateAggregator] Context cancelled")
			return
		}
	}
}

// ─────────────────────────────────────────────
//  Update aggregator
// ─────────────────────────────────────────────
func updateAggregator(ctx context.Context) {
	var batch []PendingUpdate
	ticker := time.NewTicker(FlushTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		log.Printf("📦 [UpdateAggregator] Flushing %d contacts", len(batch))
		if err := BulkUpdateAPI(ctx, batch); err != nil {
			log.Println("❌ [UpdateAggregator] Bulk update error:", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case item, ok := <-UpdateQueue:
			if !ok {
				flush()
				log.Println("✅ [UpdateAggregator] Done")
				return
			}
			batch = append(batch, item)
			if len(batch) >= BatchSize {
				flush()
				ticker.Reset(FlushTimeout)
			}

		case <-ticker.C:
			flush()

		case <-ctx.Done():
			flush()
			log.Println("⚠ [UpdateAggregator] Context cancelled")
			return
		}
	}
}

func buildAttributes(c Contact) map[string]string {
	attrs := make(map[string]string)
	if c.FirstName != "" {
		attrs["FIRST_NAME"] = c.FirstName
	}
	if c.LastName != "" {
		attrs["LAST_NAME"] = c.LastName
	}
	if c.Rashi != "" {
		attrs["RASHI"] = c.Rashi
	}
	if c.Age != "" {
		attrs["AGE"] = c.Age
	}
	return attrs
}

