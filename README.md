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
                  |                    (integration pending)
                  |
             Spark Structured Streaming
                  |
          Feature Engineering + MLlib
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

Model artifacts, historical data, and shared feature modules are maintained separately in the project environment.

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

Member 4 developed the Spark MLlib fraud model using Gradient Boosted Trees (GBT), with Logistic Regression as a baseline.

Reported model evaluation:

| Model | AUC-PR | AUC-ROC |
|---|---:|---:|
| Logistic Regression | 0.121 | 0.792 |
| Gradient Boosted Trees | 0.370 | 0.846 |

The reported evaluation used the 30,000-row dataset. These are model-development results, not a guarantee of live production performance.

Transactions receive one of three decisions:

| Decision | Meaning |
|---|---|
| APPROVE | Lower predicted fraud risk |
| REVIEW | Requires investigation |
| BLOCK | Higher predicted fraud risk |

The streaming job loads its decision thresholds from the saved model artifacts.


## 7. Data Processing Workflows

### 7.1 Transaction ingestion

The Python producer reads the transaction CSV and publishes records to Kafka's `transactions` topic. Records are keyed by TransactionID. Invalid records are sent to `invalid_records`.

### 7.2 Complaint ingestion

The Python producer reads the complaint CSV and publishes records to Kafka's `complaints` topic.

A Kafka consumer that loads these events into both Cassandra complaint tables is still required for a complete automated complaint-ingestion workflow.

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
2. Uses the historical transaction baseline and previously processed stream batches.
3. Filters previously processed TransactionIDs.
4. Builds features using the shared `features.py` module.
5. Loads the trained MLlib model and decision thresholds.
6. Calculates risk scores and assigns APPROVE, REVIEW, or BLOCK.
7. Writes scored batches to HDFS in Parquet format.
8. Persists transaction scores and decisions to MySQL.
9. Uses a persistent HDFS checkpoint to support recovery.

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

The Cassandra schema is initialized from `cassandra/init/01_schema.cql`.


## 8. Environment Configuration

Create a local `.env` file in the project root. Never commit this file.

Required variables: `MYSQL_USER`, `MYSQL_PASSWORD`, and `MYSQL_ROOT_PASSWORD`.

Set secure credentials before initializing a fresh MySQL volume. Existing MySQL volumes retain their initialized credentials.

Internal service addresses:
- Kafka: `kafka-recovery:9092`
- Hadoop HDFS: `hadoop-container:8020`
- Spark master: `spark://afrimarket-spark-master:7077`
- MySQL: `afrimarket-mysql:3306`
- Cassandra: `afrimarket-cassandra:9042`

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
| Streamlit | afrimarket-dashboard | 8501 |

The Compose configuration references existing Docker volumes and the `myNetwork` network. Create these resources in a fresh environment before starting the stack:

- Network: myNetwork
- Volume: kafka_recovery_data
- Volume: my_shared_volume
- Volume: afrimarket_mysql_data
- Volume: afrimarket_cassandra_data

The Spark services require the locally built `afrimarket-spark-python311` image. A fresh environment must obtain or build the matching image before starting those services.


## 10. Starting the Platform

Validate the Compose configuration:

    docker compose config --quiet

Build the producer and dashboard images:

    docker compose build producer dashboard

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


## 12. Running Continuous Spark Scoring

Before starting the stream, verify that these artifacts are available:

- Historical transactions: `/fraud/processed/member2_transaction_history`
- Trained model: `/shared_data/member2_fraud_model`
- Model thresholds: `/shared_data/member2_fraud_model_thresholds`
- Shared `features.py` module in the Spark shared volume.
- Streaming script: `/shared_data/stream_transactions.py`

Submit the streaming job using Spark 3.5.0 and its Kafka connector:

    docker exec afrimarket-spark-master spark-submit --master spark://afrimarket-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /shared_data/stream_transactions.py

The default maximum Kafka offsets per trigger is 500.

Do not launch a second copy of the same streaming job against the existing checkpoint.

## 13. Verifying Results

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

- Complete and test the Kafka complaint consumer that writes events to Cassandra.
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

