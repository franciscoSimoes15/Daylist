# Fast recommendation architecture for the Kafka + Spark + Hive music app

## Goal
Make the player fast by removing Hive and Spark model execution from the playback request path.

## New request path

```text
Frontend player
  -> POST /event/{play|like|skip}/next
      1. FastAPI publishes event to Kafka
      2. FastAPI immediately reads recs:{user_id} from Redis
      3. FastAPI returns songs in milliseconds when cache is warm
```

## Async update path

```text
Kafka topics
  -> Spark Structured Streaming
      -> update live user deltas
      -> generate refreshed top-N recommendations
      -> read song metadata from Redis first, Hive only for missing metadata
      -> write recs:{user_id} to Redis
      -> append raw event to Hive curated_history for future training
```

## Offline training path

```text
Hive curated_history
  -> train ALS model with Spark + implicit
  -> precompute top-N recommendations for every known user
  -> write recs:{user_id} to Redis
  -> write recs:global:trending fallback to Redis
  -> save model and JSONL backup to disk
```

## Files

- `fastapi_bridge_fast_recs.py`: FastAPI hot path; Redis-first recommendations; optional Hive fallback disabled by default.
- `spark_streaming_fast_cache.py`: Spark streaming speed layer; updates Redis recommendation cache and global trending cache.
- `train_spark_model_fast_precompute.py`: Offline trainer; precomputes recommendations and warms Redis.

## Important environment variables

```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
export ALLOW_HIVE_RECOMMEND_FALLBACK=false
export REDIS_TTL_SECONDS=3600
export REDIS_RECS_TTL_SECONDS=86400
export PRECOMPUTE_TOP_N=50
export WRITE_REDIS_PRECOMPUTED=true
```

## Recommended startup order

1. Start Redis.
2. Start Kafka.
3. Run `train_spark_model_fast_precompute.py` once to train and warm Redis.
4. Start `spark_streaming_fast_cache.py` to keep recommendations fresh from live events.
5. Start `fastapi_bridge_fast_recs.py` for the frontend.

## Frontend change

Prefer this endpoint for playback:

```text
POST /event/play/next?n=8
```

Body:

```json
{
  "user_id": 123,
  "recording_id": 456,
  "duration_ms": 180000,
  "position_ms": 0
}
```

This records the event and returns the current cached recommendation list in the same response.
