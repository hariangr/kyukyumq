package kyukyumq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	kyukyumq "hariangr.my.id/kyukyumq"
)

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image: "ghcr.io/pgmq/pg18-pgmq:v1.10.0",
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "test",
		},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		container.Terminate(ctx)
		t.Fatal(err)
	}

	connStr := fmt.Sprintf("postgres://test:test@%s:%s/test?sslmode=disable&search_path=pgmq,public", host, port.Port())

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		container.Terminate(ctx)
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;`); err != nil {
		pool.Close()
		container.Terminate(ctx)
		t.Fatalf("create extension pgmq: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS queue_control (
			queue_name TEXT PRIMARY KEY,
			is_paused BOOLEAN NOT NULL DEFAULT FALSE
		);
	`); err != nil {
		pool.Close()
		container.Terminate(ctx)
		t.Fatalf("create queue_control: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION notify_queue_control_change()
		RETURNS TRIGGER AS $$
		BEGIN
			PERFORM pg_notify('queue_control_channel', OLD.queue_name || ':' || CASE WHEN NEW.is_paused THEN 'paused' ELSE 'resumed' END);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		pool.Close()
		container.Terminate(ctx)
		t.Fatalf("create trigger function: %v", err)
	}

	pool.Exec(ctx, `DROP TRIGGER IF EXISTS queue_control_trigger ON queue_control;`)
	if _, err := pool.Exec(ctx, `
		CREATE TRIGGER queue_control_trigger
		AFTER UPDATE ON queue_control
		FOR EACH ROW
		EXECUTE FUNCTION notify_queue_control_change();
	`); err != nil {
		pool.Close()
		container.Terminate(ctx)
		t.Fatalf("create trigger: %v", err)
	}

	// Ensure the queue_control row exists for our test queue
	if _, err := pool.Exec(ctx, `
		INSERT INTO queue_control (queue_name, is_paused)
		VALUES ($1, false)
		ON CONFLICT (queue_name) DO NOTHING;
	`, testQueue); err != nil {
		pool.Close()
		container.Terminate(ctx)
		t.Fatalf("seed queue_control: %v", err)
	}

	// Create the PGMQ queue explicitly (send may auto-create in some versions)
	if _, err := pool.Exec(ctx, `SELECT pgmq.create($1);`, testQueue); err != nil {
		pool.Close()
		container.Terminate(ctx)
		t.Fatalf("create pgmq queue: %v", err)
	}

	cleanup := func() {
		pool.Close()
		container.Terminate(ctx)
	}

	return pool, cleanup
}

const testQueue = "test_queue"

// trackHandler records processed tasks and returns errors based on configuration.
type trackHandler struct {
	mu        sync.Mutex
	processed []kyukyumq.Task
	failTypes map[string]error
}

func (h *trackHandler) ProcessBatch(_ context.Context, tasks []kyukyumq.Task) []error {
	h.mu.Lock()
	h.processed = append(h.processed, tasks...)
	h.mu.Unlock()

	errs := make([]error, len(tasks))
	for i, t := range tasks {
		if err, ok := h.failTypes[t.Type]; ok {
			errs[i] = err
		}
	}
	return errs
}

func (h *trackHandler) Processed() []kyukyumq.Task {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]kyukyumq.Task, len(h.processed))
	copy(out, h.processed)
	return out
}

// counterHandler tracks call count and can inject a delay.
type counterHandler struct {
	mu      sync.Mutex
	count   int
	fail    bool
	delay   time.Duration
}

func (h *counterHandler) ProcessBatch(ctx context.Context, tasks []kyukyumq.Task) []error {
	h.mu.Lock()
	h.count++
	c := h.count
	shouldFail := h.fail
	h.mu.Unlock()

	if h.delay > 0 {
		select {
		case <-time.After(h.delay):
		case <-ctx.Done():
		}
	}

	if shouldFail {
		errs := make([]error, len(tasks))
		for i := range errs {
			errs[i] = fmt.Errorf("injected failure on call %d", c)
		}
		return errs
	}
	return nil
}

func (h *counterHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

func TestEnqueueAndConsume(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	client := kyukyumq.NewClient(pool)
	handler := &trackHandler{}

	ids := make([]int64, 3)
	for i := range 3 {
		id, err := client.Enqueue(ctx, testQueue, "test:type", map[string]any{"seq": i})
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		ids[i] = id
	}

	reg := kyukyumq.Registry{
		"test:type": {MaxRetries: 1, ExecutionTimeout: 5 * time.Second},
	}
	server := kyukyumq.NewBatchServer(pool, testQueue, 10, 30, reg, handler)

	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	server.Start(ctx)

	processed := handler.Processed()
	if len(processed) != 3 {
		t.Fatalf("expected 3 processed tasks, got %d", len(processed))
	}

	// Verify all messages were deleted from the queue (use fresh context)
	checkCtx := context.Background()
	var remaining int
	err := pool.QueryRow(checkCtx, `SELECT count(*) FROM pgmq.q_test_queue`).Scan(&remaining)
	if err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected 0 remaining messages, got %d", remaining)
	}
}

func TestMaxRetriesAndArchive(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	client := kyukyumq.NewClient(pool)
	handler := &counterHandler{fail: true}

	_, err := client.Enqueue(ctx, testQueue, "fail:type", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	reg := kyukyumq.Registry{
		"fail:type": {MaxRetries: 1, ExecutionTimeout: 5 * time.Second},
	}
	server := kyukyumq.NewBatchServer(pool, testQueue, 10, 2, reg, handler)

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	server.Start(ctx)

	c := handler.Count()
	// MaxRetries=1 means 1 archive-worthy failure, so it should be attempted at least once.
	// With set_vt(0) on failure under retry limit, PGMQ re-queues immediately.
	// So we should see at least 2 attempts (initial + 1 retry == MaxRetries+1).
	if c < 2 {
		t.Fatalf("expected at least 2 handler calls (initial + retry), got %d", c)
	}

	// Verify message is archived (use fresh context)
	checkCtx := context.Background()
	var archived int
	err = pool.QueryRow(checkCtx, `SELECT count(*) FROM pgmq.a_test_queue`).Scan(&archived)
	if err != nil {
		t.Fatalf("count archived: %v", err)
	}
	if archived == 0 {
		t.Fatalf("expected archived messages, got 0")
	}

	// Verify no messages remain in the active queue
	var remaining int
	_ = pool.QueryRow(checkCtx, `SELECT count(*) FROM pgmq.q_test_queue`).Scan(&remaining)
	if remaining != 0 {
		t.Fatalf("expected 0 active messages, got %d", remaining)
	}
}

func TestPauseResume(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	client := kyukyumq.NewClient(pool)

	// Pause the queue before enqueuing
	_, err := pool.Exec(ctx, `UPDATE queue_control SET is_paused = true WHERE queue_name = $1`, testQueue)
	if err != nil {
		t.Fatalf("pause queue: %v", err)
	}

	// Give the LISTEN/NOTIFY a moment to propagate
	time.Sleep(500 * time.Millisecond)

	_, err = client.Enqueue(ctx, testQueue, "pause:type", map[string]any{"n": 1})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	handler := &trackHandler{}
	reg := kyukyumq.Registry{"pause:type": {MaxRetries: 0, ExecutionTimeout: 5 * time.Second}}
	server := kyukyumq.NewBatchServer(pool, testQueue, 10, 30, reg, handler)

	ctx2, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	server.Start(ctx2)

	// While paused, no tasks should be processed
	if len(handler.Processed()) > 0 {
		t.Fatal("tasks were processed while queue was paused")
	}
}

func TestPauseResumeCycle(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	client := kyukyumq.NewClient(pool)
	// Enqueue first
	_, err := client.Enqueue(ctx, testQueue, "cycle:type", map[string]any{"n": 1})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Pause immediately
	_, err = pool.Exec(ctx, `UPDATE queue_control SET is_paused = true WHERE queue_name = $1`, testQueue)
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	handler2 := &trackHandler{}
	reg := kyukyumq.Registry{"cycle:type": {MaxRetries: 0, ExecutionTimeout: 5 * time.Second}}
	server := kyukyumq.NewBatchServer(pool, testQueue, 10, 30, reg, handler2)

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	go server.Start(ctx2)
	time.Sleep(1500 * time.Millisecond)

	// Resume
	_, err = pool.Exec(ctx, `UPDATE queue_control SET is_paused = false WHERE queue_name = $1`, testQueue)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}

	time.Sleep(3 * time.Second)
	cancel()

	if len(handler2.Processed()) == 0 {
		t.Fatal("tasks were not processed after resume")
	}
}

func TestInvalidJSONTaskArchived(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Send a valid JSON value that is not an object (so it fails Task unmarshaling)
	var msgID int64
	err := pool.QueryRow(ctx, `SELECT pgmq.send($1, $2);`, testQueue, `123`).Scan(&msgID)
	if err != nil {
		t.Fatalf("send non-object json: %v", err)
	}

	handler := &trackHandler{}
	reg := kyukyumq.Registry{"*": {MaxRetries: 0, ExecutionTimeout: 5 * time.Second}}
	server := kyukyumq.NewBatchServer(pool, testQueue, 10, 30, reg, handler)

	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	server.Start(ctx2)

	// The invalid message should be archived, handler should not have seen it
	if len(handler.Processed()) > 0 {
		t.Fatal("handler should not have received invalid task")
	}

	var archived int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM pgmq.a_test_queue`).Scan(&archived)
	if err != nil {
		t.Fatalf("count archived: %v", err)
	}
	if archived == 0 {
		t.Fatal("invalid message was not archived")
	}
}

func TestConcurrentStartSafety(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	client := kyukyumq.NewClient(pool)

	for i := range 5 {
		_, err := client.Enqueue(ctx, testQueue, "safe:type", map[string]any{"seq": i})
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	handler := &counterHandler{
		delay: 100 * time.Millisecond,
	}
	reg := kyukyumq.Registry{"safe:type": {MaxRetries: 0, ExecutionTimeout: 10 * time.Second}}
	server := kyukyumq.NewBatchServer(pool, testQueue, 10, 30, reg, handler)

	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Start(ctx)
		}()
	}
	wg.Wait()

	// The mutex should prevent concurrent execution; all tasks should be processed
	c := handler.Count()
	if c == 0 {
		t.Fatal("no tasks were processed")
	}
	t.Logf("processed %d batches (expected 1 with 5 tasks)", c)
}

func TestHeartbeatExtendsVisibility(t *testing.T) {
	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	client := kyukyumq.NewClient(pool)

	_, err := client.Enqueue(ctx, testQueue, "slow:type", map[string]any{"slow": true})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Short VT but processing needs longer — heartbeats should keep the message
	handler := &counterHandler{delay: 4 * time.Second}
	reg := kyukyumq.Registry{"slow:type": {MaxRetries: 0, ExecutionTimeout: 10 * time.Second}}
	server := kyukyumq.NewBatchServer(pool, testQueue, 10, 2, reg, handler)

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	server.Start(ctx2)

	// The handler should have been called exactly once
	if c := handler.Count(); c != 1 {
		t.Fatalf("expected 1 handler call, got %d", c)
	}

	// Message should be gone (use fresh context)
	checkCtx := context.Background()
	var remaining int
	_ = pool.QueryRow(checkCtx, `SELECT count(*) FROM pgmq.q_test_queue`).Scan(&remaining)
	if remaining != 0 {
		t.Fatalf("message was not consumed (remaining: %d)", remaining)
	}
}
