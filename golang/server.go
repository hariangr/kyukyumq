package kyukyumq

import (
	"context"
	"encoding/json"
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
	if s.coordinator.IsPaused() {
		return false, nil
	}

	query := `SELECT msg_id, read_ct, message FROM pgmq.read($1, $2, $3);`
	rows, err := s.pool.Query(ctx, query, s.queueName, s.baseVTSeconds, s.batchSize)
	if err != nil {
		return false, err
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
			return false, err
		}

		var t Task
		if err := json.Unmarshal(raw, &t); err != nil {
			_, _ = s.pool.Exec(ctx, `SELECT pgmq.archive($1, $2);`, s.queueName, id)
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

	executionTimeout := 5 * time.Minute
	for _, t := range tasks {
		if cfg, ok := s.registry[t.Type]; ok && cfg.ExecutionTimeout < executionTimeout {
			executionTimeout = cfg.ExecutionTimeout
		}
	}

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

func (s *BatchServer) settleBatch(ctx context.Context, ids []int64, counts []int32, tasks []Task, errs []error) {
	getErr := func(idx int) error {
		if errs == nil || idx >= len(errs) {
			return nil
		}
		return errs[idx]
	}

	for i, id := range ids {
		taskErr := getErr(i)
		maxRetries := int32(5) // fallback default
		if cfg, ok := s.registry[tasks[i].Type]; ok {
			maxRetries = cfg.MaxRetries
		}

		if taskErr == nil {
			_, _ = s.pool.Exec(ctx, `SELECT pgmq.delete($1, $2);`, s.queueName, id)
		} else if counts[i] >= maxRetries {
			_, _ = s.pool.Exec(ctx, `SELECT pgmq.archive($1, $2);`, s.queueName, id)
		} else {
			_, _ = s.pool.Exec(ctx, `SELECT pgmq.set_vt($1, $2, 0);`, s.queueName, id)
		}
	}
}
