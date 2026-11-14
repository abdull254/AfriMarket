import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

sys.path.insert(0, "/shared_data")
import features

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

# Use the existing 24,000-row HDFS history as the baseline.
baseline_raw = spark.read.parquet(HISTORY_PATH).select(*COLUMNS)
event_schema = baseline_raw.schema

baseline = (
    features.add_uid(features.clean(baseline_raw))
    .withColumn("_is_new", F.lit(False))
)

baseline_ids = baseline.select(
    F.col("TransactionID").cast("long").alias("baseline_id")
).distinct()

model = PipelineModel.load(MODEL_PATH)
threshold = spark.read.json(THRESHOLD_PATH).first()

if threshold is None:
    raise RuntimeError("Could not load model decision thresholds.")

REVIEW_AT = float(threshold["review_at"])
BLOCK_AT = float(threshold["block_at"])


def read_prior_stream_batches(current_batch_id):
    """Read previous raw batches, excluding the current batch for retry safety."""
    root = spark._jvm.org.apache.hadoop.fs.Path(RAW_HISTORY_PATH)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())

    if not fs.exists(root):
        return None

    statuses = fs.listStatus(root)
    current_name = f"batch_id={current_batch_id}"

    paths = [
        str(status.getPath())
        for status in statuses
        if status.isDirectory()
        and status.getPath().getName() != current_name
    ]

    if not paths:
        return None

    return spark.read.parquet(*paths).select(*COLUMNS)


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

    prior_raw = read_prior_stream_batches(batch_id)

    if prior_raw is not None:
        prior_stream = (
            features.add_uid(features.clean(prior_raw))
            .withColumn("_is_new", F.lit(False))
        )
        prior_ids = prior_stream.select(
            F.col("TransactionID").cast("long").alias("baseline_id")
        ).distinct()
        seen_ids = baseline_ids.unionByName(prior_ids).distinct()
    else:
        prior_stream = None
        seen_ids = baseline_ids

    new_rows = (
        features.add_uid(features.clean(parsed.select(*COLUMNS)))
        .join(
            seen_ids,
            F.col("TransactionID").cast("long") == F.col("baseline_id"),
            "left_anti"
        )
        .withColumn("_is_new", F.lit(True))
    )

    if new_rows.isEmpty():
        print(f"Batch {batch_id}: all transactions already processed")
        return

    customer_uids = new_rows.select("uid").distinct()

    customer_history = baseline.join(customer_uids, "uid", "inner")

    if prior_stream is not None:
        previous_customer_history = prior_stream.join(
            customer_uids, "uid", "inner"
        )
        customer_history = customer_history.unionByName(
            previous_customer_history, allowMissingColumns=True
        )

    combined = customer_history.unionByName(
        new_rows, allowMissingColumns=True
    )

    prepared = features.add_features(combined).filter(F.col("_is_new"))

    scored = (
        model.transform(prepared)
        .withColumn("risk_score", vector_to_array("probability")[1])
        .withColumn(
            "decision",
            F.when(F.col("risk_score") >= BLOCK_AT, "BLOCK")
             .when(F.col("risk_score") >= REVIEW_AT, "REVIEW")
             .otherwise("APPROVE")
        )
        .withColumn("scored_at", F.current_timestamp())
        .select(
            F.col("TransactionID").cast("long").alias("TransactionID"),
            F.col("TransactionDT").cast("double").alias("TransactionDT"),
            "isFraud", "risk_score", "prediction", "decision", "scored_at"
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

        # Persist raw events only after the score sinks succeed.
        raw_batch_path = f"{RAW_HISTORY_PATH}/batch_id={batch_id}"
        new_rows.select(*COLUMNS).write.mode("overwrite").parquet(
            raw_batch_path
        )

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
print("HDFS scores:", OUTPUT_PATH)
print("Rolling raw history:", RAW_HISTORY_PATH)
print("Checkpoint:", CHECKPOINT_PATH)
print("Maximum offsets per trigger:", MAX_OFFSETS)
print("Thresholds: REVIEW >=", REVIEW_AT, "BLOCK >=", BLOCK_AT)

query.awaitTermination()
