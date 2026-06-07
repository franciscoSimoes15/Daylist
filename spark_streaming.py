from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, from_json, from_unixtime, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import redis
import json
import pickle
import os
import socket
import subprocess
import time
from pathlib import Path
from urllib.parse import quote
import pandas as pd
from scipy.sparse import coo_matrix
from pyhive import hive


def load_local_env(env_path: str = ".env") -> None:
    path = Path(__file__).resolve().with_name(env_path)
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        print(f"WARNING: Invalid integer for {name}; using default {default}")
        return default


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


load_local_env()

# --- Config ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.204.131.11:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_PLAY", os.getenv("KAFKA_TOPIC", "music.events.play"))
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
KAFKA_FAIL_ON_DATA_LOSS = os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "true").lower()
KAFKA_LOCAL_FORWARD_ENABLED = env_bool("KAFKA_LOCAL_FORWARD_ENABLED", False)
KAFKA_LOCAL_FORWARD_BIND = os.getenv("KAFKA_LOCAL_FORWARD_BIND", "127.0.0.1:9092")
KAFKA_LOCAL_FORWARD_TARGET = os.getenv("KAFKA_LOCAL_FORWARD_TARGET", "10.204.131.11:9092")
KAFKA_LOCAL_FORWARD_REQUIRED = env_bool("KAFKA_LOCAL_FORWARD_REQUIRED", True)
KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS = env_int("KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS", 10)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = env_int("REDIS_PORT", 6379)
HIVE_SERVER2_IP = os.getenv("HIVE_SERVER2_IP", "10.84.128.48")
HIVE_SERVER2_PORT = env_int("HIVE_SERVER2_PORT", 10000)
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "francisco_jose_simoes")
HIVE_USERNAME = os.getenv("HIVE_USERNAME", "hadoop")
SPARK_CHECKPOINT_LOCATION = os.getenv(
    "SPARK_CHECKPOINT_LOCATION",
    os.getenv("SPARK_CHECKPOINT_PATH", "/tmp/daylist/checkpoints/music_events_play"),
)
SPARK_CHECKPOINT_AUTOVERSION = env_bool("SPARK_CHECKPOINT_AUTOVERSION", True)

# --- Global Brain State ---
BRAIN = {
    "time":         0,
    "model":        None,
    "u_enc":        None,
    "r_enc":        None,   # FIX: was missing in original
    "r_dec":        None,
    "matrix":       None,
    "live_deltas":  {}      # PERF: avoids CSR→LIL→CSR rebuild on every event
}
MODEL_PATH = os.getenv("MODEL_PATH", "./mbdump_small/models/als_model.pkl")
FEAT_PATH = os.getenv("FEATURE_PATH", os.getenv("FEAT_PATH", "./mbdump_small/features/implicit.tsv"))


def effective_checkpoint_location() -> str:
    if not SPARK_CHECKPOINT_AUTOVERSION:
        return SPARK_CHECKPOINT_LOCATION

    topic_slug = quote(KAFKA_TOPIC.replace(".", "_"), safe="_-")
    if SPARK_CHECKPOINT_LOCATION.rstrip("/").endswith(topic_slug):
        return SPARK_CHECKPOINT_LOCATION
    return f"{SPARK_CHECKPOINT_LOCATION.rstrip('/')}/{topic_slug}"


def split_host_port(value: str) -> tuple[str, int]:
    host, port = value.rsplit(":", 1)
    return host, int(port)


def is_tcp_open(host: str, port: int, timeout_seconds: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def wait_for_tcp(host: str, port: int, timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if is_tcp_open(host, port):
            return True
        time.sleep(0.25)
    return is_tcp_open(host, port)


def maybe_start_kafka_local_forward() -> bool:
    if not KAFKA_LOCAL_FORWARD_ENABLED:
        return True

    try:
        bind_host, bind_port = split_host_port(KAFKA_LOCAL_FORWARD_BIND)
        target_host, target_port = split_host_port(KAFKA_LOCAL_FORWARD_TARGET)
    except ValueError:
        print(
            "ERROR: Invalid Kafka local forward config: "
            f"bind={KAFKA_LOCAL_FORWARD_BIND} target={KAFKA_LOCAL_FORWARD_TARGET}"
        )
        return False

    if is_tcp_open(bind_host, bind_port):
        print(f"Kafka local forward already available at {KAFKA_LOCAL_FORWARD_BIND}")
        return True

    command = [
        "socat",
        f"TCP4-LISTEN:{bind_port},bind={bind_host},reuseaddr,fork",
        f"TCP4:{target_host}:{target_port}",
    ]
    try:
        subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if wait_for_tcp(bind_host, bind_port, KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS):
            print(f"Started Kafka local forward {KAFKA_LOCAL_FORWARD_BIND} -> {KAFKA_LOCAL_FORWARD_TARGET}")
            return True
        print(
            "ERROR: Kafka local forward did not become ready at "
            f"{KAFKA_LOCAL_FORWARD_BIND} within {KAFKA_LOCAL_FORWARD_READY_TIMEOUT_SECONDS} seconds"
        )
        return False
    except FileNotFoundError:
        print("ERROR: KAFKA_LOCAL_FORWARD_ENABLED=true but socat is not installed.")
        return False
    except Exception as e:
        print(f"ERROR: Failed to start Kafka local forward: {e}")
        return False


def update_brain():
    if not os.path.exists(MODEL_PATH): return False
    mtime = os.path.getmtime(MODEL_PATH)
    if mtime <= BRAIN["time"]: return True  # unchanged

    print("\n*** NEW BRAIN DETECTED! Loading... ***")
    try:
        with open(MODEL_PATH, 'rb') as f: data = pickle.load(f)
        train_df = pd.read_csv(FEAT_PATH, sep='\t')
        rows, cols = (train_df['user_id'].map(data['user_enc']),
                      train_df['recording_id'].map(data['rec_enc']))
        mask = rows.notna() & cols.notna()
        r, c = rows[mask].astype(int), cols[mask].astype(int)
        vals = train_df['implicit_score'].values[mask]

        BRAIN["matrix"]      = coo_matrix((vals, (r, c)), shape=(len(data['user_enc']), len(data['rec_enc']))).tocsr()
        BRAIN["model"]       = data['model']
        BRAIN["u_enc"]       = data['user_enc']
        BRAIN["r_enc"]       = data['rec_enc']
        BRAIN["r_dec"]       = data['rec_dec']
        BRAIN["live_deltas"] = {}   # reset — new matrix already contains latest history
        BRAIN["time"]        = mtime
        print("*** BRAIN LOAD SUCCESS! ***\n")
        return True
    except Exception as e:
        print(f"*** BRAIN LOAD FAIL: {e} ***\n")
        return False


def get_user_row(u_idx):
    """Return user's interaction row with live deltas applied."""
    row = BRAIN["matrix"][u_idx].tolil()  # LIL is fast for edit
    for (ui, ri), val in BRAIN["live_deltas"].items():
        if ui == u_idx:
            row[0, ri] += val
    return row.tocsr()  # CSR needed for brain


def connect_redis():
    for host in [REDIS_HOST, "host.docker.internal"]:
        try:
            r = redis.Redis(host=host, port=REDIS_PORT, db=0, socket_timeout=2)
            r.ping()
            print(f"Connected to Redis at {host}")
            return r
        except Exception:
            pass
    print("WARNING: Redis not available.")
    return None


def main():
    if not maybe_start_kafka_local_forward() and KAFKA_LOCAL_FORWARD_REQUIRED:
        raise RuntimeError(
            "Kafka local forward is required but unavailable. "
            "Install socat or set KAFKA_LOCAL_FORWARD_REQUIRED=false."
        )

    checkpoint_location = effective_checkpoint_location()

    print("=== Spark Streaming Config ===")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka subscribed topic: {KAFKA_TOPIC}")
    print(f"Kafka starting offsets: {KAFKA_STARTING_OFFSETS}")
    print(f"Kafka failOnDataLoss: {KAFKA_FAIL_ON_DATA_LOSS}")
    print(f"Spark checkpoint base: {SPARK_CHECKPOINT_LOCATION}")
    print(f"Spark checkpoint effective: {checkpoint_location}")

    # In spark_streaming.py, replace SparkSession builder:
    spark = SparkSession.builder.appName("Caveman-Speed-Layer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .enableHiveSupport().getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    if not update_brain():
        print("CRITICAL: No brain found. Run train_spark_model.py!")
        return

    # PERF: single Redis connection for the lifetime of the stream
    redis_client = connect_redis()

    schema = StructType([
        StructField("event_id",     StringType()),
        StructField("event_type",   StringType()),
        StructField("user_id",      IntegerType()),
        StructField("recording_id", IntegerType()),
        StructField("ts",           DoubleType()),
        StructField("duration_ms",  IntegerType()),
        StructField("source",       StringType()),
    ])

    df_kafka = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
        .option("failOnDataLoss", KAFKA_FAIL_ON_DATA_LOSS) \
        .load()

    parsed = df_kafka \
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        ) \
        .select("data.*", "kafka_topic", "kafka_partition", "kafka_offset") \
        .withColumn(
            "event_id",
            coalesce(
                col("event_id"),
                concat_ws(":", col("kafka_topic"), col("kafka_partition").cast("string"), col("kafka_offset").cast("string")),
            ),
        )

    history_has_event_id = "event_id" in spark.table(f"{HIVE_DATABASE}.curated_history").columns
    if not history_has_event_id:
        print(
            f"WARNING: {HIVE_DATABASE}.curated_history has no event_id column. "
            "Live writes cannot be idempotent until the Hive migration is applied."
        )

    def process_batch(batch_df, batch_id):
        nonlocal redis_client

        if batch_df.count() == 0: return
        update_brain()

        print(f"\n[Batch {batch_id}] Events = {batch_df.count()}")

        # --- 1. Write events to Hive ---
        # FIX: insertInto() instead of saveAsTable(mode=append).
        # saveAsTable issues a CTAS on every batch — on batch 2+ it tries to
        # re-create the table, the _temporary dir from the previous batch is
        # already gone, and HDFS throws FileNotFoundException during commit.
        # insertInto() issues a plain INSERT INTO on the existing table.
        try:
            hive_df = batch_df \
                .dropDuplicates(["event_id"]) \
                .withColumn("timestamp",   from_unixtime(col("ts"))) \
                .withColumn("time_of_day", lit("live")) \
                .withColumn("completed",   when(col("duration_ms") > 120000, True).otherwise(False)) \
                .select("user_id", "recording_id", "timestamp", "time_of_day", "duration_ms", "completed", "event_id")

            if history_has_event_id:
                existing_events = spark.table(f"{HIVE_DATABASE}.curated_history") \
                    .select("event_id") \
                    .where(col("event_id").isNotNull()) \
                    .dropDuplicates(["event_id"])
                hive_df = hive_df.join(existing_events, on="event_id", how="left_anti") \
                    .select("user_id", "recording_id", "timestamp", "time_of_day", "duration_ms", "completed", "event_id")
            else:
                hive_df = hive_df.select("user_id", "recording_id", "timestamp", "time_of_day", "duration_ms", "completed")

            rows_to_write = hive_df.count()
            if rows_to_write == 0:
                print(" -> Hive write skipped: all events already existed.")
            else:
                hive_df.write.insertInto(f"{HIVE_DATABASE}.curated_history")
                print(f" -> Hive write inserted rows: {rows_to_write}")
        except Exception as e:
            print(f" -> Hive write error: {e}")

        # --- 2. Reconnect Redis if needed ---
        try:
            redis_client.ping()
        except Exception:
            print(" -> Redis lost, reconnecting...")
            redis_client = connect_redis()
        if not redis_client: return

        # --- 3. Compute recommendations for all users in this batch ---
        pdf = batch_df.toPandas()

        # PERF: collect all rec IDs in one pass, then do a single Hive metadata query
        user_recs   = {}   # uid -> [recording_id, ...]
        all_rec_ids = set()

        for _, row in pdf.iterrows():
            uid, rid = int(row['user_id']), int(row['recording_id'])
            print(f" >> User {uid} -> Song {rid}")

            if uid not in BRAIN["u_enc"]:
                print(f" -> Cold Start: User {uid} not in training data.")
                continue

            u_idx = BRAIN["u_enc"][uid]

            # PERF: record delta instead of rebuilding the full matrix
            if rid in BRAIN["r_enc"]:
                r_idx = BRAIN["r_enc"][rid]
                key = (u_idx, r_idx)
                BRAIN["live_deltas"][key] = BRAIN["live_deltas"].get(key, 0) + 1.0
                print(f" -> Delta recorded: [{u_idx}, {r_idx}] total={BRAIN['live_deltas'][key]}")
            else:
                print(f" -> Song {rid} not in training vocab, skipping delta.")

            ids, _ = BRAIN["model"].recommend(u_idx, get_user_row(u_idx), N=8)
            live_recs = [int(BRAIN["r_dec"][i]) for i in ids]
            user_recs[uid] = live_recs
            all_rec_ids.update(live_recs)

        if not user_recs: return

        # --- 4. PERF: single Hive connection for all metadata in this batch ---
        song_dict = {}
        if all_rec_ids:
            try:
                conn = hive.Connection(
                    host=HIVE_SERVER2_IP, port=HIVE_SERVER2_PORT,
                    username=HIVE_USERNAME, database=HIVE_DATABASE
                )
                cursor = conn.cursor()
                cursor.execute(
                    f"SELECT r.id, r.name, ac.name "
                    f"FROM curated_recording r "
                    f"JOIN curated_artist_credit ac ON r.artist_credit = ac.id "
                    f"WHERE r.id IN ({','.join(map(str, all_rec_ids))})"
                )
                song_dict = {
                    int(r[0]): {"name": str(r[1]), "artist": str(r[2])}
                    for r in cursor.fetchall()
                }
                conn.close()
            except Exception as e:
                print(f" -> Hive metadata fetch failed: {e}")

        # --- 5. Push fresh recs to Redis ---
        for uid, live_recs in user_recs.items():
            real_songs = [
                {
                    "id":     s,
                    "name":   song_dict.get(s, {}).get("name",   f"Unknown {s}"),
                    "artist": song_dict.get(s, {}).get("artist", "Unknown"),
                }
                for s in live_recs
            ]
            redis_client.delete(f"recs:{uid}")
            redis_client.setex(f"recs:{uid}", 60, json.dumps(real_songs))
            print(f" -> Cache Update uid={uid}: {json.dumps(real_songs)}")

    parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="2 seconds") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()
