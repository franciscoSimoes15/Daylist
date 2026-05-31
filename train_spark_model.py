import math
import os
import pickle
from collections import defaultdict
from typing import Any, Iterable

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

import implicit
import mlflow
import numpy as np
import pandas as pd
import json
from scipy.sparse import coo_matrix
from sklearn.model_selection import train_test_split
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, least, lit

from implicit.nearest_neighbours import bm25_weight

DATABASE = os.getenv("HIVE_DATABASE", "francisco_jose_simoes")
FEATURE_DIR = os.getenv("FEATURE_DIR", "./mbdump_small/features")
MODEL_DIR = os.getenv("MODEL_DIR", "./mbdump_small/models")
FACTORS = int(os.getenv("ALS_FACTORS", "32"))
ITERATIONS = int(os.getenv("ALS_ITERATIONS", "20"))
REGULARIZATION = float(os.getenv("ALS_REGULARIZATION", "0.001"))
RANDOM_STATE = int(os.getenv("RANDOM_STATE", "42"))
TOP_K = int(os.getenv("EVAL_TOP_K", "10"))
PRECOMPUTE_TOP_N = int(os.getenv("PRECOMPUTE_TOP_N", "50"))
WRITE_REDIS_PRECOMPUTED = os.getenv("WRITE_REDIS_PRECOMPUTED", "true").lower() in {"1", "true", "yes"}
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_RECS_TTL_SECONDS = int(os.getenv("REDIS_RECS_TTL_SECONDS", "86400"))


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("Music-Recommender-Training").enableHiveSupport().getOrCreate()


def connect_redis() -> Any | None:
    if not WRITE_REDIS_PRECOMPUTED:
        print("Redis precompute write disabled.")
        return None
    try:
        import redis
    except ImportError:
        print("Redis package not installed; skipping Redis precompute write.")
        return None

    for host in [REDIS_HOST, "host.docker.internal", "127.0.0.1"]:
        try:
            client = redis.Redis(host=host, port=REDIS_PORT, db=0, socket_timeout=2)
            client.ping()
            print(f"Connected to Redis at {host}; will write precomputed recommendations.")
            return client
        except Exception:
            continue
    print("Redis unavailable; recommendations saved to disk only.")
    return None


def fetch_metadata_for_ids(spark: SparkSession, recording_ids: set[int]) -> dict[int, dict[str, str]]:
    if not recording_ids:
        return {}
    chunks = []
    ids = sorted(recording_ids)
    for start in range(0, len(ids), 1000):
        ids_sql = ",".join(str(i) for i in ids[start:start + 1000])
        chunks.append(spark.sql(f"""
            SELECT CAST(r.id AS INT) AS id, CAST(r.name AS STRING) AS name, CAST(a.name AS STRING) AS artist
            FROM {DATABASE}.curated_recording r
            JOIN {DATABASE}.curated_artist_credit_name acn ON r.artist_credit = acn.artist_credit
            JOIN {DATABASE}.curated_artist a ON acn.artist = a.id
            WHERE r.id IN ({ids_sql})
        """).toPandas())
    if not chunks:
        return {}
    meta_df = pd.concat(chunks, ignore_index=True)
    return {
        int(row["id"]): {"name": str(row["name"]), "artist": str(row["artist"])}
        for _, row in meta_df.iterrows()
    }


def precompute_and_write_recommendations(
    spark: SparkSession,
    model: Any,
    train_matrix,
    user_dec: dict[int, int],
    rec_dec: dict[int, int],
    output_dir: str,
) -> None:
    """Create user_id -> top-N song list outside the request path.

    This is the main latency fix: FastAPI can read recs:{user_id} from Redis
    instead of querying Hive or running Spark when the player needs the next song.
    """
    print(f"Precompute top {PRECOMPUTE_TOP_N} recommendations per user...")
    user_to_rids: dict[int, list[int]] = {}
    all_rids: set[int] = set()

    for user_idx, user_id in user_dec.items():
        try:
            rec_indices, _ = model.recommend(user_idx, train_matrix[user_idx], N=PRECOMPUTE_TOP_N)
        except Exception:
            continue
        rids = [int(rec_dec[int(i)]) for i in rec_indices]
        user_to_rids[int(user_id)] = rids
        all_rids.update(rids)

    metadata = fetch_metadata_for_ids(spark, all_rids)
    rows: list[dict[str, Any]] = []
    redis_client = connect_redis()
    pipe = redis_client.pipeline() if redis_client else None

    for user_id, rids in user_to_rids.items():
        songs = [
            {
                "id": rid,
                "name": metadata.get(rid, {}).get("name", f"Unknown {rid}"),
                "artist": metadata.get(rid, {}).get("artist", "Unknown"),
            }
            for rid in rids
        ]
        rows.append({"user_id": user_id, "songs_json": json.dumps(songs)})
        if pipe:
            pipe.setex(f"recs:{user_id}", REDIS_RECS_TTL_SECONDS, json.dumps(songs))

    # Also write a global fallback for cold-start users.
    popular_pdf = spark.sql(f"""
        SELECT CAST(r.id AS INT) AS id, CAST(r.name AS STRING) AS name, CAST(a.name AS STRING) AS artist, COUNT(*) AS score
        FROM {DATABASE}.curated_history h
        JOIN {DATABASE}.curated_recording r ON h.recording_id = r.id
        JOIN {DATABASE}.curated_artist_credit_name acn ON r.artist_credit = acn.artist_credit
        JOIN {DATABASE}.curated_artist a ON acn.artist = a.id
        GROUP BY r.id, r.name, a.name
        ORDER BY score DESC
        LIMIT {PRECOMPUTE_TOP_N}
    """).toPandas()
    trending = [
        {"id": int(r["id"]), "name": str(r["name"]), "artist": str(r["artist"]), "score": float(r["score"])}
        for _, r in popular_pdf.iterrows()
    ]
    if pipe:
        for rid, meta in metadata.items():
            pipe.setex(f"song:{rid}", REDIS_RECS_TTL_SECONDS, json.dumps(meta))
        pipe.setex("recs:global:trending", REDIS_RECS_TTL_SECONDS, json.dumps(trending))
        pipe.execute()
        print(f"Wrote {len(user_to_rids)} user recommendation lists to Redis.")

    out_path = os.path.join(output_dir, "precomputed_user_recs.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"Saved precomputed recommendations to {out_path}")


def precision_recall_at_k(model, train_matrix, test_by_user: dict[int, set[int]], rec_dec: dict[int, int], k: int) -> tuple[float, float]:
    precisions: list[float] = []
    recalls: list[float] = []

    for user_idx, truth_items in test_by_user.items():
        if not truth_items:
            continue
        try:
            rec_indices, _ = model.recommend(user_idx, train_matrix[user_idx], N=k)
        except Exception:
            continue
        recommended = {rec_dec[int(i)] for i in rec_indices}
        hits = len(recommended & truth_items)
        precisions.append(hits / k)
        recalls.append(hits / len(truth_items))

    if not precisions:
        return 0.0, 0.0
    return float(np.mean(precisions)), float(np.mean(recalls))


def main() -> None:
    print("Init Spark...")
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Fetch data from {DATABASE}.curated_history...")
    df_spark = spark.sql(f"""
        SELECT
            CAST(h.user_id AS INT) AS user_id,
            CAST(h.recording_id AS INT) AS recording_id,
            COUNT(*) AS plays,
            AVG(CAST(h.completed AS INT)) AS completion_rate,
            AVG(h.duration_ms) AS avg_duration
        FROM {DATABASE}.curated_history h
        JOIN {DATABASE}.curated_recording r ON h.recording_id = r.id
        WHERE h.user_id IS NOT NULL
          AND h.recording_id IS NOT NULL
          AND h.duration_ms IS NOT NULL
        GROUP BY h.user_id, h.recording_id
    """)

    interactions = df_spark.count()
    if interactions == 0:
        print("No interactions found. Cannot train model.")
        spark.stop()
        return

    training_data_spark = (
        df_spark.withColumn(
            "implicit_score",
            col("plays") * lit(1.0)
            + col("completion_rate") * lit(2.0)
            + least(col("avg_duration") / lit(300000.0), lit(5.0)) * lit(2.0),
        )
        .select("user_id", "recording_id", "implicit_score")
        .dropna()
        .filter(col("implicit_score") > 0)
    )

    implicit_df = training_data_spark.toPandas()

    # -----------------------------
    # FILTER LOW-SUPPORT ITEMS & LOW-ACTIVITY USERS
    # -----------------------------
    while True:
        prev = len(implicit_df)
        item_counts = implicit_df.groupby("recording_id").size()
        implicit_df = implicit_df[implicit_df["recording_id"].isin(item_counts[item_counts >= 3].index)]
        user_counts = implicit_df.groupby("user_id").size()
        implicit_df = implicit_df[implicit_df["user_id"].isin(user_counts[user_counts >= 5].index)]
        if len(implicit_df) == prev:
            break

    if implicit_df.empty:
        print("No positive implicit scores. Cannot train model.")
        spark.stop()
        return
    
    users = implicit_df["user_id"].nunique()
    items = implicit_df["recording_id"].nunique()

    print(f"After filtering: users={users}, items={items}, interactions={len(implicit_df)}")

    if users < 2 or items < 2:
        print("Not enough data after filtering.")
        spark.stop()
        return

    os.makedirs(FEATURE_DIR, exist_ok=True)
    os.makedirs(MODEL_DIR, exist_ok=True)
    feature_path = os.path.join(FEATURE_DIR, "implicit.tsv")
    model_path = os.path.join(MODEL_DIR, "als_model.pkl")
    implicit_df.to_csv(feature_path, sep="\t", index=False)

    users = implicit_df["user_id"].nunique()
    items = implicit_df["recording_id"].nunique()
    if users < 2 or items < 2:
        print(f"Not enough data to train. users={users}, items={items}")
        spark.stop()
        return

    print("Split train/test...")
    #train_df, test_df = train_test_split(implicit_df, test_size=0.2, random_state=RANDOM_STATE)
    # -----------------------------
    # LEAVE-ONE-OUT SPLIT PER USER
    # -----------------------------
    train_rows = []
    test_rows = []

    for user_id, group in implicit_df.groupby("user_id"):
        if len(group) < 5:
            train_rows.append(group)  # all to train, skip test
            continue

        test_sample = group.sample(1, random_state=RANDOM_STATE)
        train_sample = group.drop(test_sample.index)

        train_rows.append(train_sample)
        test_rows.append(test_sample)

    train_df = pd.concat(train_rows)
    test_df = pd.concat(test_rows)

    user_list = train_df["user_id"].drop_duplicates().tolist()
    rec_list = train_df["recording_id"].drop_duplicates().tolist()

    # IMPORTANT: include items from train + test to avoid leakage issues
    rec_list = list(set(
        train_df["recording_id"].tolist() +
        test_df["recording_id"].tolist()
    ))

    user_enc = {int(u): i for i, u in enumerate(user_list)}
    rec_enc = {int(r): i for i, r in enumerate(rec_list)}
    user_dec = {i: u for u, i in user_enc.items()}
    rec_dec = {i: r for r, i in rec_enc.items()}

    train_rows = train_df["user_id"].map(user_enc).astype(int)
    train_cols = train_df["recording_id"].map(rec_enc).astype(int)
    train_data = train_df["implicit_score"].astype(float).values

    train_matrix = coo_matrix(
        (train_data, (train_rows, train_cols)),
        shape=(len(user_list), len(rec_list)),
        dtype=np.float32,
    ).tocsr()

    train_matrix = bm25_weight(train_matrix, K1=20, B=0.3).tocsr()

    test_rows = test_df["user_id"].map(user_enc).astype(int).values
    test_cols = test_df["recording_id"].map(rec_enc).astype(int).values
    test_real = test_df["implicit_score"].astype(float).values

    test_by_user: dict[int, set[int]] = defaultdict(set)
    for _, row in test_df.iterrows():
        test_by_user[user_enc[int(row["user_id"])]].add(int(row["recording_id"]))

    mlflow.set_experiment("Music_Recommender_ALS")
    with mlflow.start_run():
        mlflow.log_param("factors", FACTORS)
        mlflow.log_param("iterations", ITERATIONS)
        mlflow.log_param("regularization", REGULARIZATION)
        mlflow.log_param("users", users)
        mlflow.log_param("items", items)
        mlflow.log_param("interactions", int(len(implicit_df)))

        print("Train ALS model...")
        model = implicit.als.AlternatingLeastSquares(
            factors=128,
            iterations=50,
            regularization=0.01,
            alpha=40,
            random_state=RANDOM_STATE,
        )
        model.fit(train_matrix)

        # LightFM handles cold-start + sparse better
        #from lightfm import LightFM
        #model = LightFM(no_components=64, loss='warp')

        print("Evaluate model...")
        print("Compute popularity baseline...")

        popular = (
            implicit_df.groupby("recording_id")
            .size()
            .sort_values(ascending=False)
            .head(TOP_K)
            .index.tolist()
        )

        popular_set = set(popular)

        popular_hits = 0
        total = 0

        for user_idx, truth_items in test_by_user.items():
            hits = len(popular_set & truth_items)
            popular_hits += hits
            total += len(truth_items)

        print("Popular Recall:", popular_hits / total)

        guesses = np.sum(model.user_factors[test_rows] * model.item_factors[test_cols], axis=1)
        #rmse = math.sqrt(float(np.mean((test_real - guesses) ** 2)))
        precision_k, recall_k = precision_recall_at_k(model, train_matrix, test_by_user, rec_dec, TOP_K)

        #mlflow.log_metric("rmse", rmse)
        mlflow.log_metric(f"precision_at_{TOP_K}", precision_k)
        mlflow.log_metric(f"recall_at_{TOP_K}", recall_k)

        #print(f"RMSE = {rmse:.4f}")
        print(f"Precision@{TOP_K} = {precision_k:.4f}")
        print(f"Recall@{TOP_K} = {recall_k:.4f}")

        print(f"Save model to {model_path}...")
        with open(model_path, "wb") as f:
            pickle.dump(
                {
                    "model": model,
                    "user_enc": user_enc,
                    "rec_enc": rec_enc,
                    "user_dec": user_dec,
                    "rec_dec": rec_dec,
                    "metrics": {
                        #"rmse": rmse,
                        f"precision_at_{TOP_K}": precision_k,
                        f"recall_at_{TOP_K}": recall_k,
                    },
                },
                f,
            )

        precompute_and_write_recommendations(
            spark=spark,
            model=model,
            train_matrix=train_matrix,
            user_dec=user_dec,
            rec_dec=rec_dec,
            output_dir=MODEL_DIR,
        )

    spark_users = df_spark.select("user_id").distinct().count()
    spark_items = df_spark.select("recording_id").distinct().count()
    sparsity = 1 - (interactions / (spark_users * spark_items)) if spark_users and spark_items else 0

    print("=== DATA SIZE REPORT ===")
    print("Interactions:", interactions)
    print("Users:", spark_users)
    print("Items:", spark_items)
    print("Avg interactions per user:", interactions / spark_users if spark_users else 0)
    print("Avg interactions per item:", interactions / spark_items if spark_items else 0)
    print("Sparsity:", sparsity)
    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()