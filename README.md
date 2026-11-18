# AfriMarket – Real-Time E-Commerce Fraud Intelligence Platform

**CIS 641 – Big Data Systems | Group 6**

## 1. Project Overview

AfriMarket is a containerized big-data platform for identifying potentially fraudulent e-commerce transactions and supporting complaint investigations.

The platform combines Kafka event ingestion, Hadoop distributed storage, Apache Spark processing, Spark MLlib fraud scoring, MySQL score persistence, Cassandra complaint storage, and a Streamlit dashboard.

The system is designed to:
- Ingest transaction and customer complaint events.
- Process historical and incoming transaction data.
- Assign each transaction a fraud risk score between 0 and 1.
- Classify transactions as APPROVE, REVIEW, or BLOCK.
- Store scored transactions for investigation and reporting.
- Support complaint lookup and investigation through the database layer.

## 2. System Architecture

```text
                  AfriMarket CSV Datasets
                    /                 \
                   /                   \
          Transaction CSV          Complaint CSV
                 |                       |
                 +------ Python Producer-+
                             |
                          Apache Kafka
                       /       |       \
                      /        |        \
             transactions  complaints  invalid_records
                  |             |
                  |             +---- Complaint consumer/
                  |                    Cassandra loader
                  |                    (implemented; runtime integration tested successfully)
                  |
             Spark Structured Streaming
                  |
          Feature Engineering + MLlib <---> Redis
                  |                        (live per-card features)
                  |
           Fraud Risk Classification
                  |
              +---+---+
              |       |
             HDFS    MySQL
              |       |
              +---+---+
                  |
             Streamlit Dashboard

          Cassandra stores complaint records
          for complaint investigation and lookup.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Apache Kafka | Transaction and complaint event ingestion |
| Apache Hadoop HDFS | Distributed storage |
| Redis 7.2 | Live per-card feature state for real-time scoring |
| Apache Spark 3.5.0 | Distributed and streaming processing |
| PySpark / Spark MLlib | Feature engineering and fraud scoring |
| MySQL 8.0 | Transaction scores and investigation cases |
| Cassandra 4.1 | Complaint storage and lookup |
| Python 3.11 | Producers and application code |
| Streamlit | Interactive dashboard |
| Docker Compose | Container orchestration |

## 4. Repository Structure

- `compose.yaml` – integrated service configuration.
- `data/` – transaction and complaint datasets.
- `kafka/config/` – Kafka broker configuration.
- `ingestion/` – Python producers and Kafka topic utilities.
- `spark_processing/` – Spark batch and streaming jobs.
- `mysql/init/` – MySQL database schema.
- `cassandra/init/` – Cassandra keyspace and table definitions.
- `dashboard/` – Streamlit application and dependencies.
- `train_fraud_model.py`, `features.py` – model training and shared feature engineering.
- `online_features.py`, `score_snippet.py` – live customer features from Redis and the scoring step.
- `complaint_analysis.py` – complaint keyword tagging and link to transactions.
- `tests/`, `parity_test.py` – feature tests and the training-vs-live feature check.
- `results/` – outputs from the latest training run.
- `README_ML.md` – full details of the model, results, and live features.

Trained model files are not stored in Git. Running `train_fraud_model.py` creates them.

## 5. Datasets

**Transactions:** `data/afrimarket_transactions.csv`
- 30,000 IEEE-CIS transaction records.
- Includes transaction identifiers, amounts, timestamps, device and transaction features.
- `isFraud` is retained for evaluation and must not be used as a model input.

**Complaints:** `data/afrimarket_complaints.csv`
- 1,200 synthetic complaint records.
- Includes ComplaintID, TransactionID, date, category, text, and status.
- TransactionID links complaints to transactions.

## 6. Fraud Detection Model

Member 4 developed the Spark MLlib fraud model using Gradient Boosted Trees (GBT), with Logistic Regression as a baseline. Full details are in `README_ML.md`.

The data is split by time: oldest 70% for training, next 15% for validation, newest 15% for test. The model and the decision thresholds are chosen on validation, and the test set is only used to report results.

Results on the test set (4,502 transactions over 31.4 days, 3.4% fraud, so random guessing scores AUC-PR 0.035):

| Model | AUC-PR | AUC-ROC |
|---|---:|---:|
| Logistic Regression | 0.105 | 0.793 |
| Gradient Boosted Trees (used) | 0.346 | 0.798 |
| Gradient Boosted Trees, no ID columns | 0.399 | 0.808 |

Thresholds: REVIEW at 0.55, BLOCK at 0.9. On test, REVIEW catches 33% of fraud dollars with 360 honest orders flagged, for net savings of about $2,760 in 31 days (assuming $15 per false alarm).

These are model-development results, not a guarantee of live production performance.

Transactions receive one of three decisions:

| Decision | Meaning |
|---|---|
| APPROVE | Lower predicted fraud risk |
| REVIEW | Requires investigation |
| BLOCK | Higher predicted fraud risk |

The streaming job loads its decision thresholds from the saved model artifacts.


## 7. Data Processing Workflows

### 7.1 Transaction ingestion

The Python producer reads the transaction CSV and publishes records to Kafka's `transactions` topic. Records are keyed by `card1`, so each card's purchases land in one Kafka partition and reach Spark in time order, which the Redis live features need. Invalid records are sent to `invalid_records`.

### 7.2 Complaint ingestion

The Python producer reads the complaint CSV and publishes records to Kafka's `complaints` topic.

The `complaint-consumer` service reads the `complaints` topic and writes each valid event to both Cassandra lookup tables: `complaints_by_id` and `complaints_by_transaction`. It commits the Kafka offset only after both writes succeed. The consumer connects to the existing Kafka and Cassandra containers on `myNetwork`.

**Verified runtime integration test (November 18, 2026):**
- Processed all 1,200 complaint messages from Kafka.
- Confirmed 1,200 records in `complaints_by_id`.
- Confirmed 1,200 records in `complaints_by_transaction`.
- Verified that all three Kafka partitions reached zero consumer lag.
- Inspected sample Cassandra records and confirmed the expected complaint fields.

### 7.3 Spark batch processing

The batch-scoring component:
1. Reads transaction events from Kafka.
2. Retrieves historical transaction data.
3. Filters transactions already represented in the historical dataset.
4. Prepares model features.
5. Loads the trained model and decision thresholds.
6. Scores transactions and assigns decisions.
7. Saves scored records to HDFS in Parquet format.

See `spark_processing/README.md` for the batch-scoring implementation and its test results.

### 7.4 Continuous transaction scoring

`spark_processing/stream_transactions.py` implements the continuous Structured Streaming workflow.

It:
1. Reads events from Kafka's `transactions` topic.
2. Skips TransactionIDs that are already in the historical baseline.
3. Gets each card's live customer features from Redis (`online_features.py`): it reads the card's running totals first, then adds the new purchase, so a purchase never counts itself. A re-sent message gets the same cached features and is not counted twice.
4. Adds the per-purchase features (hour of day, cents) with the shared `features.py` code used in training.
5. Loads the trained MLlib model and decision thresholds.
6. Calculates risk scores and assigns APPROVE, REVIEW, or BLOCK. If Redis is unavailable, the purchase is scored as a first-time customer and never auto-blocked: BLOCK becomes REVIEW.
7. Writes scored batches to HDFS in Parquet format.
8. Persists transaction scores and decisions to MySQL.
9. Archives the raw events in HDFS for retraining, and uses a persistent HDFS checkpoint to support recovery.

Live scoring never rebuilds features from HDFS history. HDFS is read once at startup: the stream loads the historical baseline into Redis the first time it runs (and records that it did, so restarts skip this step), so returning customers are not treated as new.

Required model, threshold, historical-data, and feature artifacts must be available before starting the streaming job.

Checkpoint location:

`/fraud/checkpoints/member2_transaction_scoring`

**Important:** Preserve this checkpoint when restarting the stream. Do not delete it during routine troubleshooting.

### 7.5 Database integration

**MySQL – `fraud` database**
- `scores`: transaction risk scores and decisions.
- `investigation_cases`: investigation cases linked to scored transactions.

**Cassandra – `afrimarket` keyspace**
- `complaints_by_id`: complaint lookup by complaint ID.
- `complaints_by_transaction`: complaint lookup by transaction ID.

The Cassandra schema is initialized from `cassandra/init/01_schema.cql`. Both tables use Bintou's schema: `complaint_id` is text, `transaction_id` is bigint, and the remaining complaint fields are text. `complaints_by_id` is keyed by `complaint_id`; `complaints_by_transaction` uses `(transaction_id, complaint_id)` as its composite primary key.


## 8. Environment Configuration

Create a local `.env` file in the project root. Never commit this file.

Required variables: `MYSQL_USER`, `MYSQL_PASSWORD`, and `MYSQL_ROOT_PASSWORD`.

Set secure credentials before initializing a fresh MySQL volume. Existing MySQL volumes retain their initialized credentials.

Internal service addresses:
- Kafka: `kafka-recovery:9092`
- Hadoop HDFS: `hadoop-container:8020`
- Spark master: `spark://afrimarket-spark-master:7077`
- MySQL: `mysql:3306`
- Cassandra: `cassandra:9042`
- Redis: `redis:6379`

## 9. Docker Services and Ports

| Component | Container | Host port |
|---|---|---:|
| Kafka | kafka-recovery | 9099 |
| Hadoop NameNode UI | hadoop-container | 9870 |
| Hadoop YARN UI | hadoop-container | 8088 |
| Spark master | afrimarket-spark-master | 7077, 8080 |
| Spark worker 1 | afrimarket-spark-worker-1 | 8081 |
| Spark worker 2 | afrimarket-spark-worker-2 | 8082 |
| MySQL | afrimarket-mysql | 3307 |
| Cassandra | afrimarket-cassandra | 9042 |
| Redis | afrimarket-redis | (internal only) |
| Streamlit | afrimarket-dashboard | 8501 |

The Compose configuration references existing Docker volumes and the `myNetwork` network. Create these resources in a fresh environment before starting the stack:

- Network: `myNetwork`
- Volume: `kafka_recovery_data`
- Volume: `my_shared_volume`
- Volume: `afrimarket_mysql_data`
- Volume: `afrimarket_cassandra_data`
- Volume: `afrimarket_hdfs_data`
- Volume: `afrimarket_redis_data`

Create the network and volumes with:

    docker network create myNetwork
    docker volume create kafka_recovery_data
    docker volume create my_shared_volume
    docker volume create afrimarket_mysql_data
    docker volume create afrimarket_cassandra_data
    docker volume create afrimarket_hdfs_data
    docker volume create afrimarket_redis_data

### Hadoop and persistent HDFS storage

The custom Hadoop image is built from `hadoop/Dockerfile`. It includes:

- `hadoop/conf/core-site.xml`: sets the HDFS endpoint to `hdfs://hadoop-container:8020`.
- `hadoop/conf/hdfs-site.xml`: sets the replication factor to 1 for this single-DataNode development environment.

The `afrimarket_hdfs_data` volume is mounted at `/tmp/hadoop-root/dfs` and stores the NameNode and DataNode directories. The separate `my_shared_volume` is mounted at `/data` for shared files.

**Fresh environment only:** after creating the empty HDFS volume and building the Hadoop image, initialize its NameNode exactly once before starting the Hadoop service:

    docker compose build hadoop-container
    docker compose run --rm --no-deps --entrypoint /hadoop/bin/hdfs hadoop-container namenode -format

Then start the platform as described below. Never run the format command against an existing initialized HDFS volume. Formatting creates a new namespace and can make existing HDFS data inaccessible.

For an existing environment, reuse its initialized `afrimarket_hdfs_data` volume. Do not format it, delete it, or run `docker compose down -v` when its data must be preserved. This includes the Spark streaming checkpoint at `/fraud/checkpoints/member2_transaction_scoring`.

The Spark image is built from `spark_processing/Dockerfile`, which installs Python 3.11 and PySpark 3.5.0 on Apache Spark 3.5.0. Docker Compose builds this image for the Spark master and reuses it for both workers.



### 9.1 Preparing Member 2 streaming artifacts

On a fresh environment, initialize HDFS as described above and start the Hadoop and Spark services before preparing these artifacts. The following steps reproduce the Member 2 setup.

**Important distinction:** `my_shared_volume` is a Docker shared filesystem mounted at `/shared_data` in the Spark containers. HDFS is a separate filesystem accessed through `hdfs://hadoop-container:8020`. Copying a file into the Docker volume does not automatically put it into HDFS.

#### Step 1: Copy the source files and transaction data into the Spark shared volume

Run these commands from the repository directory:

    docker cp ~/AfriMarket/features.py afrimarket-spark-master:/shared_data/features.py
    docker cp ~/AfriMarket/online_features.py afrimarket-spark-master:/shared_data/online_features.py
    docker cp ~/AfriMarket/train_fraud_model.py afrimarket-spark-master:/shared_data/train_fraud_model.py
    docker cp ~/AfriMarket/spark_processing/stream_transactions.py afrimarket-spark-master:/shared_data/stream_transactions.py
    docker cp ~/AfriMarket/data/afrimarket_transactions.csv afrimarket-spark-master:/shared_data/afrimarket_transactions.csv

The Spark master and both workers mount the same `my_shared_volume` at `/shared_data`. These copies therefore make the files available through that shared volume. The Compose volume mount alone does not copy repository files into it.

#### Step 2: Create the historical transaction baseline

The historical baseline is a Parquet dataset created from the first 24,000 transactions after ordering by `TransactionDT`:

    docker exec afrimarket-spark-master python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.appName('AfriMarketHistoricalData').config('spark.hadoop.fs.defaultFS','hdfs://hadoop-container:8020').getOrCreate(); s.sparkContext.setLogLevel('ERROR'); df=s.read.option('header',True).option('inferSchema',True).csv('file:///shared_data/afrimarket_transactions.csv'); history=df.orderBy('TransactionDT').limit(24000); history.write.mode('overwrite').parquet('hdfs://hadoop-container:8020/fraud/processed/member2_transaction_history'); print('Historical records saved:',history.count()); s.stop()"

This creates:

`/fraud/processed/member2_transaction_history`

#### Step 3: Prepare the model-training input

Create a separate CSV containing the header and first 24,000 data rows:

    head -n 24001 ~/AfriMarket/data/afrimarket_transactions.csv > ~/AfriMarket/data/member2_training_transactions.csv

Copy it into the shared volume:

    docker cp ~/AfriMarket/data/member2_training_transactions.csv afrimarket-spark-master:/shared_data/member2_training_transactions.csv

Stage this CSV in HDFS for the training script:

    docker exec afrimarket-spark-master python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.appName('AfriMarketTrainingDataStaging').config('spark.hadoop.fs.defaultFS','hdfs://hadoop-container:8020').getOrCreate(); s.sparkContext.setLogLevel('ERROR'); df=s.read.option('header',True).option('inferSchema',True).csv('file:///shared_data/member2_training_transactions.csv'); df.write.mode('overwrite').option('header',True).csv('hdfs://hadoop-container:8020/fraud/processed/member2_training_csv'); print('Training records staged:',df.count()); s.stop()"

#### Step 4: Train and save the model and thresholds

Submit the training script:

    docker exec afrimarket-spark-master spark-submit --master spark://afrimarket-spark-master:7077 --conf spark.hadoop.fs.defaultFS=hdfs://hadoop-container:8020 --py-files /shared_data/features.py /shared_data/train_fraud_model.py --transactions hdfs://hadoop-container:8020/fraud/processed/member2_training_csv --model-dir /shared_data/member2_fraud_model --results-dir /shared_data/member2_training_results

The training script saves the fitted Spark MLlib pipeline to:

`/shared_data/member2_fraud_model`

It also generates the threshold directory automatically by appending `_thresholds` to the model directory:

`/shared_data/member2_fraud_model_thresholds`

Because the Spark session's default filesystem is configured as HDFS, these two artifact paths are HDFS paths, not directories in the Docker shared volume. The `review_at` and `block_at` values are calculated during training and may change when the data or training run changes.

#### Step 5: Verify the artifacts

Check that the source files are available in the Spark shared volume:

    docker exec afrimarket-spark-master ls -lh /shared_data/features.py /shared_data/train_fraud_model.py /shared_data/stream_transactions.py

Check the historical dataset:

    docker exec hadoop-container hdfs dfs -ls /fraud/processed/member2_transaction_history

Check the model:

    docker exec hadoop-container hdfs dfs -ls /shared_data/member2_fraud_model

Check the thresholds:

    docker exec hadoop-container hdfs dfs -ls /shared_data/member2_fraud_model_thresholds

**Before rerunning:** the history and training-data staging commands overwrite their target HDFS paths. Model training also overwrites the model and threshold directories. Coordinate before regenerating artifacts in an active environment, and preserve the streaming checkpoint at `/fraud/checkpoints/member2_transaction_scoring`.


## 10. Starting the Platform

Validate the Compose configuration:

    docker compose config --quiet

Build the application images:

    docker compose build hadoop-container spark-master producer complaint-consumer dashboard

Start the services:

    docker compose up -d

Check service status:

    docker compose ps

For an existing environment with running containers, avoid recreating services unnecessarily. Preserve named volumes and the Spark streaming checkpoint.

## 11. Kafka Topics and Data Publishing

Create the Kafka topics:

    docker compose exec producer python create_topics.py

Publish transaction events:

    docker compose exec producer python producer.py transactions --start 0 --limit 30000 --delay 0.0

Publish complaint events:

    docker compose exec producer python producer.py complaints --start 0 --limit 1200 --delay 0.0

For a slower live demonstration, publish 100 transactions with a half-second delay:

    docker compose exec producer python producer.py transactions --start 0 --limit 100 --delay 0.5

The producer reports confirmed, failed, and invalid records.

The complaint consumer uses the independently managed Kafka (`kafka-recovery`) and Cassandra (`afrimarket-cassandra`) containers on `myNetwork`. Build it with:

    docker compose build complaint-consumer

After confirming both existing containers are running, start the consumer without asking Compose to create or manage its dependencies:

    docker compose up -d --no-deps complaint-consumer

Inspect its logs with:

    docker compose logs -f complaint-consumer

Verify that the consumer is ready before publishing complaint events. The `--no-deps` option is important in this environment because Kafka and Cassandra already exist as standalone containers.


## 12. Running Continuous Spark Scoring

Before starting the stream, verify that these artifacts are available:

- Historical transactions: `/fraud/processed/member2_transaction_history`
- Trained model: `/shared_data/member2_fraud_model`
- Model thresholds: `/shared_data/member2_fraud_model_thresholds`
- Shared `features.py` and `online_features.py` modules in the Spark shared volume.
- The `redis` service running (`docker compose ps redis`).
- Streaming script: `/shared_data/stream_transactions.py`

Submit the streaming job using Spark 3.5.0 and its Kafka connector:

    docker exec afrimarket-spark-master spark-submit --master spark://afrimarket-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /shared_data/stream_transactions.py

The default maximum Kafka offsets per trigger is 500. The Spark master container passes the MySQL login from `.env` and the Redis address to the job (see `compose.yaml`).

On its first run the job prints `Seeded Redis with 24,000 historical transactions`. Later runs print `Redis already seeded`.

Do not launch a second copy of the same streaming job against the existing checkpoint.

## 13. Verifying Results

Check HDFS safe mode and DataNode status:

    docker exec hadoop-container hdfs dfsadmin -safemode get
    docker exec hadoop-container hdfs dfsadmin -report

Check HDFS filesystem integrity:

    docker exec hadoop-container hdfs fsck /

For this single-DataNode development setup, expect one live DataNode, replication factor 1, and no missing or corrupt blocks.

List Kafka topics:

    docker exec kafka-recovery /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-recovery:9092 --list

List HDFS streaming output:

    docker exec hadoop-container hdfs dfs -ls /fraud/stream/member2_scored_transactions

Check the streaming checkpoint:

    docker exec hadoop-container hdfs dfs -ls /fraud/checkpoints/member2_transaction_scoring

Check the number of MySQL scores:

    docker exec afrimarket-mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -D fraud -e "SELECT COUNT(*) FROM scores;"'

Open the dashboard at:

    http://localhost:8501

The dashboard depends on MySQL and Cassandra being available and populated.

## 14. Team Responsibilities

| Member | Responsibility |
|---|---|
| Member 1 – Marienne | Kafka ingestion, producers, topic creation, and Hadoop integration |
| Member 2 – Muhammed | Spark processing, Structured Streaming, and ML model integration |
| Member 3 – Bintou | MySQL, Cassandra, and dashboard integration |
| Member 4 – Abdullahi | Fraud model development, training, and shared feature engineering |

## 15. Known Limitations and Next Steps

- Add `features_degraded` and `scored_at` columns to the MySQL `scores` table, so degraded scoring and end-to-end latency can be seen in the dashboard.
- Complete end-to-end validation of complaint-to-transaction investigation in the dashboard.
- Ensure all teammates can build or obtain the same Spark 3.5.0/Python image.
- Verify model and historical-data provisioning on a fresh environment.
- Measure continuous-stream throughput and end-to-end scoring latency.
- Test recovery using the persistent Structured Streaming checkpoint.
- Evaluate scoring with larger data volumes.
- Improve complaint-to-transaction investigation integration.

## 16. Data and Security Notes

- Never commit `.env` or real credentials.
- Keep local Kafka recovery data and backups out of Git.
- Do not remove persistent Docker volumes during routine troubleshooting.
- Do not delete the Spark streaming checkpoint unless intentionally resetting the stream.
- Configure secure passwords for shared environments.
- This project is an academic prototype, not a production payment authorization system.

