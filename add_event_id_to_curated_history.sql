-- Adds a stable live-event identifier so Spark streaming can make Hive writes idempotent.
-- Run this once on an existing deployment before restarting spark_streaming.py.
-- Historical batch rows will keep event_id = NULL.

ALTER TABLE curated_history ADD COLUMNS (event_id STRING);

-- Validation: should return zero after the streaming job has deduplicated correctly.
SELECT event_id, COUNT(*) AS duplicate_count
FROM curated_history
WHERE event_id IS NOT NULL
GROUP BY event_id
HAVING COUNT(*) > 1;
