from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import redis
import json
import pickle
import os
import pandas as pd
from scipy.sparse import coo_matrix
from pyhive import hive

# --- Config ---
KAFKA_BOOTSTRAP_SERVERS = "10.204.131.11:9092"
KAFKA_TOPIC = "music.events.play"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
HIVE_SERVER2_IP = os.getenv("HIVE_SERVER2_IP", "10.84.128.48")
HIVE_SERVER2_PORT = int(os.getenv("HIVE_SERVER2_PORT", 10000))
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "francisco_jose_simoes")

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
MODEL_PATH = './mbdump_small/models/als_model.pkl'
FEAT_PATH  = './mbdump_small/features/implicit.tsv'


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
        StructField("user_id",      IntegerType()),
        StructField("recording_id", IntegerType()),
        StructField("ts",           DoubleType()),
        StructField("duration_ms",  IntegerType()),
        StructField("source",       StringType()),
    ])

    df_kafka = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed = df_kafka \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

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
                .withColumn("timestamp",   from_unixtime(col("ts"))) \
                .withColumn("time_of_day", lit("live")) \
                .withColumn("completed",   when(col("duration_ms") > 120000, True).otherwise(False)) \
                .select("user_id", "recording_id", "timestamp", "time_of_day", "duration_ms", "completed")
            hive_df.write.insertInto(f"{HIVE_DATABASE}.curated_history")
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
                    username='hadoop', database=HIVE_DATABASE
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
        .trigger(processingTime="2 seconds") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()