# KyukyuMQ (🪶 kyukyumq)

An unopinionated, high-throughput batch task processing orchestration engine running entirely on Postgres via `pgx/v5` and the `PGMQ` extension. It effectively bridges the familiar, type-safe API mental model of Redis `Asynq` down into a pure relational Postgres foundation.

## Features

- **True Native Batch Pulling**: Pull chunks of workloads simultaneously with single database queries via functional table structures.
- **Dynamic Heartbeat VT Extension**: Automatically holds tasks and extends execution timeouts safely if your processing pipelines outlive basic polling thresholds.
- **Zero-Lag Instant Pausing**: Built around PostgreSQL asynchronous operational signaling engines (`LISTEN/NOTIFY`) for sub-millisecond atomic pause configurations without continuous polling pressure.
- **Granular Error Framework**: Safely individualizes errors inside multi-item lists, supporting targeted retries or archiving corrupted configurations.

## Database Prerequisites

Ensure you run these schema foundations on your Postgres instance before initialization:

```sql
-- Ensure PGMQ extension is loaded
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;

-- Control coordinates for instant pausing mechanisms
CREATE TABLE IF NOT EXISTS pgmq.queue_control (
    queue_name TEXT PRIMARY KEY,
    is_paused BOOLEAN NOT NULL DEFAULT FALSE
);

-- Asynchronous signaling trigger mapping definitions
CREATE OR REPLACE FUNCTION pgmq.notify_queue_control_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'queue_control_channel', 
        OLD.queue_name || ':' || CASE WHEN NEW.is_paused THEN 'paused' ELSE 'resumed' END
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER queue_control_trigger
AFTER UPDATE ON pgmq.queue_control
FOR EACH ROW
EXECUTE FUNCTION pgmq.notify_queue_control_change();
