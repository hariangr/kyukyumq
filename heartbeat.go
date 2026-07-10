package kyukyumq

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func startHeartbeat(ctx context.Context, exec executor, queue string, msgID int64, extendBySeconds int32) func() {
	if extendBySeconds <= 0 {
		return func() {}
	}

	stop := make(chan struct{})
	interval := time.Duration(extendBySeconds/2) * time.Second
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				query := `SELECT pgmq.set_vt($1::TEXT, $2::BIGINT, $3::INTEGER);`
				_, _ = exec.Exec(ctx, query, queue, msgID, extendBySeconds)
			}
		}
	}()

	return func() { close(stop) }
}
