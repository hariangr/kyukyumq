package kyukyumq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TaskConfig struct {
	MaxRetries       int32
	ExecutionTimeout time.Duration
}

type Registry map[string]TaskConfig

type BatchHandler interface {
	ProcessBatch(ctx context.Context, tasks []Task) []error
}

type BatchServer struct {
	pool          *pgxpool.Pool
	queueName     string
	batchSize     int32
	baseVTSeconds int32
	registry      Registry
	handler       BatchHandler
	pollInterval  time.Duration
	coordinator   *QueueCoordinator
	mu            sync.Mutex
	logger        *slog.Logger
}

func NewBatchServer(pool *pgxpool.Pool, queue string, size int32, baseVT int32, reg Registry, h BatchHandler) *BatchServer {
	return &BatchServer{
		pool:          pool,
		queueName:     queue,
		batchSize:     size,
		baseVTSeconds: baseVT,
		registry:      reg,
		handler:       h,
		pollInterval:  500 * time.Millisecond,
		coordinator:   NewQueueCoordinator(pool, queue),
		logger:        slog.Default(),
	}
}

func (s *BatchServer) Start(ctx context.Context) error {
	if err := s.coordinator.InitAndListen(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			hasMessages, err := s.runBatch(ctx)
			if err != nil {
				s.logger.Error("batch run failed", "queue", s.queueName, "error", err)
				time.Sleep(s.pollInterval)
				continue
			}
			if !hasMessages {
				time.Sleep(s.pollInterval)
			}
		}
	}
}

func (s *BatchServer) runBatch(ctx context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.coordinator.IsPaused() {
		return false, nil
	}

	query := `SELECT msg_id, read_ct, message FROM pgmq.read($1::TEXT, $2::INTEGER, $3::INTEGER);`
	rows, err := s.pool.Query(ctx, query, s.queueName, s.baseVTSeconds, s.batchSize)
	if err != nil {
		return false, fmt.Errorf("pgmq.read: %w", err)
	}
	defer rows.Close()

	var msgIDs []int64
	var readCounts []int32
	var tasks []Task

	for rows.Next() {
		var id int64
		var count int32
		var raw []byte
		if err := rows.Scan(&id, &count, &raw); err != nil {
			return false, fmt.Errorf("scan row: %w", err)
		}

		var t Task
		if err := json.Unmarshal(raw, &t); err != nil {
			s.logger.Error("invalid task json, archiving", "msg_id", id, "error", err)
			if _, execErr := s.pool.Exec(ctx, `SELECT pgmq.archive($1::TEXT, $2::BIGINT);`, s.queueName, id); execErr != nil {
				s.logger.Error("failed to archive invalid task", "msg_id", id, "error", execErr)
			}
			continue
		}

		msgIDs = append(msgIDs, id)
		readCounts = append(readCounts, count)
		tasks = append(tasks, t)
	}

	if len(tasks) == 0 {
		return false, nil
	}

	heartbeatCtx, cancelHeartbeats := context.WithCancel(ctx)
	var stopHeartbeats []func()
	for _, id := range msgIDs {
		stopHeartbeats = append(stopHeartbeats, startHeartbeat(heartbeatCtx, s.pool, s.queueName, id, s.baseVTSeconds))
	}

	executionTimeout := s.resolveExecutionTimeout(tasks)

	handlerCtx, cancelHandler := context.WithTimeout(ctx, executionTimeout)
	errs := s.handler.ProcessBatch(handlerCtx, tasks)
	cancelHandler()

	cancelHeartbeats()
	for _, stop := range stopHeartbeats {
		stop()
	}

	s.settleBatch(ctx, msgIDs, readCounts, tasks, errs)
	return true, nil
}

func (s *BatchServer) resolveExecutionTimeout(tasks []Task) time.Duration {
	timeout := 5 * time.Minute
	for _, t := range tasks {
		if cfg, ok := s.registry[t.Type]; ok && cfg.ExecutionTimeout > timeout {
			timeout = cfg.ExecutionTimeout
		}
	}
	return timeout
}

func (s *BatchServer) settleBatch(ctx context.Context, ids []int64, counts []int32, tasks []Task, errs []error) {
	getErr := func(idx int) error {
		if errs == nil || idx >= len(errs) {
			return nil
		}
		return errs[idx]
	}

	for i, id := range ids {
		taskErr := getErr(i)
		maxRetries := int32(5)
		if cfg, ok := s.registry[tasks[i].Type]; ok {
			maxRetries = cfg.MaxRetries
		}

		switch {
		case taskErr == nil:
			if _, err := s.pool.Exec(ctx, `SELECT pgmq.delete($1::TEXT, $2::BIGINT);`, s.queueName, id); err != nil {
				s.logger.Error("failed to delete completed task",
					"msg_id", id, "queue", s.queueName, "error", err)
			}
		case counts[i] > maxRetries:
			if _, err := s.pool.Exec(ctx, `SELECT pgmq.archive($1::TEXT, $2::BIGINT);`, s.queueName, id); err != nil {
				s.logger.Error("failed to archive exhausted task",
					"msg_id", id, "queue", s.queueName, "error", err)
			}
		default:
			if _, err := s.pool.Exec(ctx, `SELECT pgmq.set_vt($1::TEXT, $2::BIGINT, 0);`, s.queueName, id); err != nil {
				s.logger.Error("failed to reset vt for retry task",
					"msg_id", id, "queue", s.queueName, "error", err)
			}
		}
	}
}
