package kyukyumq

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type QueueCoordinator struct {
	pool     *pgxpool.Pool
	isPaused atomic.Bool
	queue    string
}

func NewQueueCoordinator(pool *pgxpool.Pool, queueName string) *QueueCoordinator {
	return &QueueCoordinator{
		pool:  pool,
		queue: queueName,
	}
}

func (q *QueueCoordinator) InitAndListen(ctx context.Context) error {
	var isPaused bool
	err := q.pool.QueryRow(ctx, "SELECT is_paused FROM queue_control WHERE queue_name = $1", q.queue).Scan(&isPaused)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	q.isPaused.Store(isPaused)

	go q.listenLoop(ctx)
	return nil
}

func (q *QueueCoordinator) IsPaused() bool {
	return q.isPaused.Load()
}

func (q *QueueCoordinator) listenLoop(ctx context.Context) {
	for {
		conn, err := q.pool.Acquire(ctx)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		_, err = conn.Exec(ctx, "LISTEN queue_control_channel;")
		if err != nil {
			conn.Release()
			time.Sleep(2 * time.Second)
			continue
		}

		for {
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				break
			}

			parts := strings.Split(notification.Payload, ":")
			if len(parts) == 2 && parts[0] == q.queue {
				switch parts[1] {
				case "paused":
					q.isPaused.Store(true)
				case "resumed":
					q.isPaused.Store(false)
				}
			}
		}

		conn.Release()
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
