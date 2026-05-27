import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"
import pandas as pd
import numpy as np
import os, pickle, math
from scipy.sparse import coo_matrix
import implicit
import mlflow
from sklearn.model_selection import train_test_split
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, least, lit

def main():
    print("Init Spark...")
    spark = SparkSession.builder \
        .appName("Caveman-Fetch-Data") \
        .enableHiveSupport() \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    DATABASE = "francisco_jose_simoes"

    print(f"Fetch data from {DATABASE}.curated_history...")
    # JOIN ADDED HERE -> KILL GHOST SONGS
    df_spark = spark.sql(f"""
        SELECT 
            CAST(h.user_id AS INT) as user_id,
            CAST(h.recording_id AS INT) as recording_id,
            COUNT(*) as plays,
            MEAN(CAST(h.completed AS INT)) as completion_rate,
            MEAN(h.duration_ms) as avg_duration
        FROM {DATABASE}.curated_history h
        JOIN {DATABASE}.curated_recording r ON h.recording_id = r.id
        WHERE h.user_id IS NOT NULL AND h.recording_id IS NOT NULL
        GROUP BY h.user_id, h.recording_id
    """)

    training_data_spark = df_spark.withColumn("implicit_score", 
        col("plays") * 1.0 + 
        col("completion_rate") * 2.0 + 
        least(col("avg_duration") / 300000.0, lit(1.0)) * 1.0
    ).select("user_id", "recording_id", "implicit_score").dropna()

    implicit_df = training_data_spark.toPandas()
    
    feat_dir = './mbdump_small/features/'
    os.makedirs(feat_dir, exist_ok=True)
    tsv_path = os.path.join(feat_dir, 'implicit.tsv')
    implicit_df.to_csv(tsv_path, sep='\t', index=False)

    print("Split Train vs Test...")
    # 80% Train, 20% Test
    train_df, test_df = train_test_split(implicit_df, test_size=0.2, random_state=42)

    user_list = implicit_df['user_id'].unique()
    rec_list = implicit_df['recording_id'].unique()
    
    user_enc = {u: i for i, u in enumerate(user_list)}
    rec_enc  = {r: i for i, r in enumerate(rec_list)}
    user_dec = {i: u for u, i in user_enc.items()}
    rec_dec  = {i: r for r, i in rec_enc.items()}

    # Train Matrix
    rows = train_df['user_id'].map(user_enc).astype(int)
    cols = train_df['recording_id'].map(rec_enc).astype(int)
    data = train_df['implicit_score'].values
    
    train_matrix = coo_matrix(
        (data, (rows, cols)),
        shape=(len(user_list), len(rec_list))
    ).tocsr()

    # Test Arrays
    test_rows = test_df['user_id'].map(user_enc).astype(int).values
    test_cols = test_df['recording_id'].map(rec_enc).astype(int).values
    test_real = test_df['implicit_score'].values

    # MLflow tracking
    mlflow.set_experiment("Caveman_Music_Recs")
    with mlflow.start_run():
        factors = 32
        iterations = 20
        reg = 0.001

        mlflow.log_param("factors", factors)
        mlflow.log_param("iterations", iterations)

        print("Train brain...")
        model = implicit.als.AlternatingLeastSquares(
            factors=factors, 
            iterations=iterations, 
            regularization=reg, 
            random_state=42
        )
        model.fit(train_matrix)

        print("Test brain...")
        # Guess = User Vector * Item Vector
        guesses = np.sum(model.user_factors[test_rows] * model.item_factors[test_cols], axis=1)
        
        # Math: RMSE
        mse = np.mean((test_real - guesses) ** 2)
        rmse = math.sqrt(mse)
        
        print(f"*** RMSE = {rmse} ***")
        mlflow.log_metric("rmse", rmse)

        out_dir = './mbdump_small/models/'
        os.makedirs(out_dir, exist_ok=True)
        model_path = os.path.join(out_dir, 'als_model.pkl')
        
        print(f"Save brain to {model_path}...")
        with open(model_path, 'wb') as f:
            pickle.dump({
                'model': model,
                'user_enc': user_enc,
                'rec_enc': rec_enc,
                'user_dec': user_dec,
                'rec_dec': rec_dec
            }, f)

    print("Done.")
    spark.stop()

if __name__ == "__main__":
    main()