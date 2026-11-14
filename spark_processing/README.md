# AfriMarket Spark Processing and ML Integration (CIS 641 – Group 6)

**Member 2: Spark Processing and ML Integration**

## Overview

This component processes e-commerce transaction events published to Kafka, prepares transaction features using PySpark, and applies the trained Gradient Boosted Trees (GBT) fraud detection model developed by Member 4.

Each transaction receives a fraud risk score between 0 and 1 and a decision:

- **APPROVE** – Transaction is considered low risk.
- **REVIEW** – Transaction requires further investigation.
- **BLOCK** – Transaction is considered high risk.

Scored transactions are stored in Hadoop HDFS in Parquet format for downstream analysis and integration with the project's database and dashboard components.

## Files

| File | Description |
|---|---|
| `publish_events.py` | Publishes transaction events from the project dataset to Kafka. |
| `score_transactions.py` | Reads Kafka events, prepares features, loads the trained GBT model, scores transactions, and saves results to HDFS. |
| `README.md` | Documentation for the Spark processing component. |

The shared `features.py` module and trained model are maintained by Member 4 and reused by this component.

## Processing Workflow

1. **Kafka ingestion:** Reads transaction events from the Kafka `transactions` topic.
2. **Historical data retrieval:** Loads transaction history from the source dataset and historical transaction IDs from HDFS.
3. **Feature preparation:** Uses the shared feature engineering module to generate the numeric, categorical, and transaction-history features required by the trained model.
4. **Historical filtering:** Identifies Kafka transactions not already included in the historical transaction set.
5. **Model integration:** Loads the trained GBT PipelineModel and saved decision thresholds from HDFS.
6. **Fraud scoring:** Generates a risk score and assigns APPROVE, REVIEW, or BLOCK.
7. **HDFS storage:** Saves the scored transaction records in Parquet format.

The current implementation uses precomputed historical features to support reliable batch scoring. Kafka ingestion and Spark processing are implemented; continuous Structured Streaming integration remains a future improvement.

## Technologies

| Technology | Purpose |
|---|---|
| Apache Kafka | Transaction event ingestion and messaging. |
| Apache Spark 3.5.0 | Distributed data processing and scoring. |
| PySpark | Data preparation and model execution. |
| Spark MLlib | Loading and applying the trained GBT model. |
| Hadoop HDFS | Historical data access and scored output storage. |
| Parquet | Columnar storage format for scored output. |
| Docker | Containerized execution environment. |

## Test Results

The batch-scoring pipeline was successfully executed using the Spark standalone cluster with two worker nodes.

- Source transaction dataset: 30,000 transactions.
- Historical transaction records: 24,000.
- Kafka transactions selected for scoring: 6,000.
- Trained model: Gradient Boosted Trees (GBT).
- Review threshold: 0.55.
- Block threshold: 0.95.

### Fraud Decision Summary

| Decision | Transactions | Percentage |
|---|---:|---:|
| APPROVE | 5,466 | 91.10% |
| REVIEW | 500 | 8.33% |
| BLOCK | 34 | 0.57% |
| **Total** | **6,000** | **100%** |

### Evaluation Against Known Fraud Labels

| Actual Label | APPROVE | REVIEW | BLOCK |
|---|---:|---:|---:|
| Non-fraud (0) | 5,365 | 422 | 8 |
| Fraud (1) | 101 | 78 | 26 |

The model assigned REVIEW or BLOCK to 104 of the 205 transactions labelled as fraudulent in the evaluation dataset.

### HDFS Output

Scored transactions were successfully saved to:

`/fraud/processed/member2_scored_transactions`

Output format: Snappy-compressed Parquet.

The output was read back from HDFS and verified to contain exactly 6,000 records.

The saved records include:

- `TransactionID`
- `TransactionDT`
- `isFraud`
- `risk_score`
- `prediction`
- `decision`
- `scored_at`

## Running the Pipeline

### 1. Publish Transaction Events to Kafka

Run the publisher from the project environment:

```bash
docker exec afrimarket-spark-master spark-submit \
  --master spark://afrimarket-spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /shared_data/publish_events.py
```

The publisher sends transaction events to the Kafka `transactions` topic.

### 2. Run Spark Scoring

Submit the scoring job from the Spark master container:

```bash
docker exec afrimarket-spark-master spark-submit \
  --master spark://afrimarket-spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.shuffle.partitions=8 \
  /tmp/score_transactions.py
```

### 3. Verify HDFS Output

Run `docker exec hadoop-container hdfs dfs -ls -h /fraud/processed/member2_scored_transactions`.

### 4. Read the Scored Data

The output can be read using PySpark. The HDFS output path is `hdfs://6eea8ba9e649:8020/fraud/processed/member2_scored_transactions`. The output was verified to contain 6,000 records.

## Future Improvements

- Integrate the batch-scoring logic into a continuously running Spark Structured Streaming application.
- Persist transaction-level risk scores and decisions into the project MySQL database, in coordination with Member 3.
- Measure processing throughput and scoring latency.
- Integrate scored transaction records with the complaint investigation and dashboard components.
- Evaluate performance with larger transaction volumes.
