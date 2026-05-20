package kyukyumq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Task struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Client struct {
	pool *pgxpool.Pool
}

func NewClient(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

func (c *Client) Enqueue(ctx context.Context, queueName string, taskType string, payload any) (int64, error) {
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal task payload: %w", err)
	}

	task := Task{
		Type:    taskType,
		Payload: rawPayload,
	}

	wrapped, err := json.Marshal(task)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal parent task struct: %w", err)
	}

	var msgID int64
	query := `SELECT * FROM pgmq.send($1, $2::jsonb);`
	err = c.pool.QueryRow(ctx, query, queueName, wrapped).Scan(&msgID)
	if err != nil {
		return 0, fmt.Errorf("pgmq.send execution failed: %w", err)
	}

	return msgID, nil
}
