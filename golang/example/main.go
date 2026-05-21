package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"hariangr.my.id/kyukyumq"
)

// EmailPayload represents your custom job argument fields
type EmailPayload struct {
	Recipient string `json:"recipient"`
	Body      string `json:"body"`
}

// GlobalWorker implements the kyukyumq.BatchHandler signature
type GlobalWorker struct{}

func (w *GlobalWorker) ProcessBatch(ctx context.Context, tasks []kyukyumq.Task) []error {
	errs := make([]error, len(tasks))

	for i, task := range tasks {
		switch task.Type {
		case "email:welcome":
			var p EmailPayload
			if err := json.Unmarshal(task.Payload, &p); err != nil {
				errs[i] = err
				continue
			}
			fmt.Printf("[WORKER] Sending welcome email to %s\n", p.Recipient)
			// Simulate network latency or heavy computational workload
			time.Sleep(100 * time.Millisecond)
			errs[i] = nil
			errs[i] = errors.New("abc xxx")

		default:
			errs[i] = fmt.Errorf("unregistered task identifier payload structure: %s", task.Type)
		}
	}

	return errs
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Establish database connection pool
	pool, err := pgxpool.New(ctx, "postgres://postgres:password@postgres:3066/qqmqtest?sslmode=disable&search_path=pgmq")
	if err != nil {
		log.Fatalf("failed to open database handle: %v", err)
	}
	defer pool.Close()

	// 2. Setup Task Behavior Definitions
	registry := kyukyumq.Registry{
		"email:welcome": kyukyumq.TaskConfig{
			MaxRetries:       3,
			ExecutionTimeout: 30 * time.Second,
		},
	}

	// 3. Initialize Enqueue Client
	client := kyukyumq.NewClient(pool)

	// Inject a quick mock task payload sequence
	_, err = client.Enqueue(ctx, "global_jobs", "email:welcome", EmailPayload{
		Recipient: "hari@example.com",
		Body:      "Welcome to Nikay applications ecosystem!",
	})
	if err != nil {
		log.Fatalf("failed to enqueue task: %v", err)
	}

	// return

	// 4. Initialize and Run the Batch Server Loop
	worker := &GlobalWorker{}
	server := kyukyumq.NewBatchServer(
		pool,
		"global_jobs", // Queue name match
		15,            // Max Batch items pulled concurrently
		30,            // Base initial Lock Visibility Timeout in seconds
		registry,
		worker,
	)

	// Create a channel to catch when the server is fully done shutting down
	serverDone := make(chan struct{})

	fmt.Println("[SYS] Starting kyukyumq batch consumer loops...")
	go func() {
		defer close(serverDone) // Signal that the server has fully stopped
		if err := server.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("Fatal execution trace inside execution framework loop: %v", err)
		}
	}()

	// Graceful Shutdown orchestration block
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop // Wait for Ctrl-C
	fmt.Println("[SYS] Signal received! Halting running workers and cleaning connections gracefully...")

	// 5. CRITICAL: Trigger context cancellation immediately to notify the server loops to stop
	cancel()

	// 6. Wait for the server goroutine to completely finish processing or cleaning up
	<-serverDone

	fmt.Println("[SYS] Shutdown complete. Exiting.")
}
