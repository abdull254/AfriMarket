import os
import sys

import redis
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

sys.path.insert(0, "/shared_data")
import features
from online_features import ONLINE, RedisStore, compute_online_features

HDFS = os.getenv("HDFS_URI", "hdfs://hadoop-container:8020")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka-recovery:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

HISTORY_PATH = HDFS + "/fraud/processed/member2_transaction_history"
MODEL_PATH = HDFS + "/shared_data/member2_fraud_model"
THRESHOLD_PATH = HDFS + "/shared_data/member2_fraud_model_thresholds"

OUTPUT_PATH = HDFS + "/fraud/stream/member2_scored_transactions"
RAW_HISTORY_PATH = HDFS + "/fraud/stream/member2_raw_history"
CHECKPOINT_PATH = HDFS + "/fraud/checkpoints/member2_transaction_scoring"

MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "500")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# Set once the HDFS history has been loaded into Redis, so restarts skip it.
SEEDED_KEY = "afrimarket:seeded:" + HISTORY_PATH

COLUMNS = [
    "TransactionID", "TransactionDT", "TransactionAmt",
    "card1", "card2", "card3", "card5", "addr1", "dist1",
    "C1", "C2", "C5", "C6", "C9", "C11", "C13", "C14",
    "D1", "D4", "D10", "D15", "ProductCD", "card4", "card6",
    "P_emaildomain", "R_emaildomain", "DeviceType", "isFraud"
]

spark = (
    SparkSession.builder
    .master("spark://afrimarket-spark-master:7077")
    .appName("AfriMarketContinuousFraudScoring")
    .config("spark.hadoop.fs.defaultFS", HDFS)
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Live customer features come from Redis (online_features.py), not from HDFS.
# HDFS is read once at startup: to seed Redis with the purchase history and to
# know which transactions are already part of that history.
store = RedisStore(redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=0.2))

baseline_raw = spark.read.parquet(HISTORY_PATH).select(*COLUMNS)
event_schema = baseline_raw.schema

baseline_ids = baseline_raw.select(
    F.col("TransactionID").cast("long").alias("baseline_id")
).distinct().cache()

FEATURE_KEYS = ["TransactionID", "TransactionDT", "TransactionAmt", "card1", "addr1"]


def seed_redis():
    """Replay the HDFS history through the feature store once, in time order,
    so returning customers are not scored as first-time customers."""
    if store.r.get(SEEDED_KEY):
        print("Redis already seeded from", HISTORY_PATH)
        return
    rows = [r.asDict() for r in baseline_raw.select(*FEATURE_KEYS).collect()]
    feats = compute_online_features(rows, store)
    if any(f["features_degraded"] for f in feats):
        raise RuntimeError("Redis failed while seeding the history; not marking it as seeded.")
    store.r.set(SEEDED_KEY, len(rows))
    print(f"Seeded Redis with {len(rows):,} historical transactions")


seed_redis()

model = PipelineModel.load(MODEL_PATH)
threshold = spark.read.json(THRESHOLD_PATH).first()

if threshold is None:
    raise RuntimeError("Could not load model decision thresholds.")

REVIEW_AT = float(threshold["review_at"])
BLOCK_AT = float(threshold["block_at"])


def save_mysql(rows):
    import pymysql

    connection = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "afrimarket-mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "fraud"),
        connect_timeout=10,
        autocommit=False
    )

    sql = """
        INSERT INTO scores
            (TransactionID, TransactionDT, risk_score, decision)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            TransactionDT = VALUES(TransactionDT),
            risk_score = VALUES(risk_score),
            decision = VALUES(decision)
    """

    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def handle_batch(kafka_batch, batch_id):
    parsed = (
        kafka_batch
        .select(
            F.from_json(
                F.col("value").cast("string"), event_schema
            ).alias("event")
        )
        .select("event.*")
        .filter(F.col("TransactionID").isNotNull())
        .dropDuplicates(["TransactionID"])
    )

    if parsed.isEmpty():
        print(f"Batch {batch_id}: no valid events")
        return

    # Skip transactions that are already part of the training history.
    new_raw = parsed.select(*COLUMNS).join(
        baseline_ids,
        F.col("TransactionID").cast("long") == F.col("baseline_id"),
        "left_anti"
    )

    # A micro-batch is small (at most MAX_OFFSETS_PER_TRIGGER rows), so the
    # feature lookup runs on the driver: read each card's totals from Redis,
    # then add the purchase. Re-sent transactions get their cached features.
    rows = [r.asDict() for r in new_raw.select(*FEATURE_KEYS).collect()]
    if not rows:
        print(f"Batch {batch_id}: all transactions already processed")
        return

    feats = compute_online_features(rows, store)
    feat_df = spark.createDataFrame(
        [(int(r["TransactionID"]), *[float(f[c]) for c in ONLINE], f["features_degraded"])
         for r, f in zip(rows, feats)],
        "TransactionID long, " + ", ".join(f"{c} double" for c in ONLINE)
        + ", features_degraded boolean"
    )

    # Same row features and missing-value rules as training (features.py).
    prepared = features.fill_missing(
        features.add_row_features(features.clean(new_raw))
        .withColumn("TransactionID", F.col("TransactionID").cast("long"))
        .join(feat_df, "TransactionID", "left")
    )

    scored = (
        model.transform(prepared)
        .withColumn("risk_score", vector_to_array("probability")[1])
        .withColumn(
            "decision",
            F.when(
                F.col("risk_score") >= BLOCK_AT,
                # Never auto-block on incomplete information: send to an analyst instead.
                F.when(F.col("features_degraded"), "REVIEW").otherwise("BLOCK")
            )
             .when(F.col("risk_score") >= REVIEW_AT, "REVIEW")
             .otherwise("APPROVE")
        )
        .withColumn("scored_at", F.current_timestamp())
        .select(
            F.col("TransactionID").cast("long").alias("TransactionID"),
            F.col("TransactionDT").cast("double").alias("TransactionDT"),
            "isFraud", "risk_score", "prediction", "decision",
            "features_degraded", "scored_at"
        )
        .persist()
    )

    try:
        count = scored.count()

        if count == 0:
            print(f"Batch {batch_id}: no new records to score")
            return

        # Save scores separately from the existing batch-scoring results.
        batch_path = f"{OUTPUT_PATH}/batch_id={batch_id}"
        scored.write.mode("overwrite").parquet(batch_path)

        rows = [
            (
                int(r.TransactionID),
                float(r.TransactionDT) if r.TransactionDT is not None else None,
                float(r.risk_score),
                r.decision
            )
            for r in scored.select(
                "TransactionID", "TransactionDT", "risk_score", "decision"
            ).collect()
        ]

        # MySQL uses TransactionID as the primary key, making retries safe.
        save_mysql(rows)

        # Keep the raw events in the HDFS data lake for retraining.
        raw_batch_path = f"{RAW_HISTORY_PATH}/batch_id={batch_id}"
        new_raw.write.mode("overwrite").parquet(raw_batch_path)

        print(f"Batch {batch_id}: saved {count} scores to HDFS and MySQL")
        scored.groupBy("decision").count().orderBy("decision").show()

    finally:
        scored.unpersist()


query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", os.getenv("STARTING_OFFSETS", "latest"))
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .option("failOnDataLoss", "true")
    .load()
    .writeStream
    .queryName("afrimarket_transaction_fraud_scoring")
    .foreachBatch(handle_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print("AfriMarket continuous fraud scoring is running.")
print("Kafka:", KAFKA, "Topic:", TOPIC)
print("Historical baseline:", HISTORY_PATH)
print("Live features: Redis at", f"{REDIS_HOST}:{REDIS_PORT}")
print("HDFS scores:", OUTPUT_PATH)
print("Raw event archive:", RAW_HISTORY_PATH)
print("Checkpoint:", CHECKPOINT_PATH)
print("Maximum offsets per trigger:", MAX_OFFSETS)
print("Thresholds: REVIEW >=", REVIEW_AT, "BLOCK >=", BLOCK_AT)

query.awaitTermination()
