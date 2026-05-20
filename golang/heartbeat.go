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
	stop := make(chan struct{})
	ticker := time.NewTicker(time.Duration(extendBySeconds/2) * time.Second)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				query := `SELECT * FROM pgmq.set_vt($1, $2, $3);`
				_, _ = exec.Exec(ctx, query, queue, msgID, extendBySeconds)
			}
		}
	}()

	return func() { close(stop) }
}
