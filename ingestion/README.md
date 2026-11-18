# AfriMarket Ingestion

This directory contains the Python Kafka producer and the Cassandra complaint consumer.

## Components

- `scripts/producer.py` reads the transaction and complaint CSV files and publishes events to Kafka.
- `scripts/create_topics.py` creates the Kafka topics.
- `scripts/complaint_consumer.py` consumes complaint events and stores them in Cassandra.
- `python/Dockerfile` defines the shared Python image and installs the Kafka, Cassandra, HDFS, and pandas dependencies.

## Complaint event workflow

The producer publishes complaint records to the Kafka `complaints` topic. Events use these source fields:

- `ComplaintID`
- `TransactionID`
- `ComplaintDate`
- `ComplaintCategory`
- `ComplaintText`
- `ComplaintStatus`

The consumer converts `TransactionID` to an integer and writes each valid event to both tables in the `afrimarket` keyspace:

**`complaints_by_id`**

| Column | Type | Role |
|---|---|---|
| `complaint_id` | text | Primary key |
| `transaction_id` | bigint | Transaction reference |
| `complaint_date` | text | Complaint date |
| `complaint_category` | text | Complaint category |
| `complaint_text` | text | Complaint details |
| `complaint_status` | text | Complaint status |

**`complaints_by_transaction`**

| Column | Type | Role |
|---|---|---|
| `transaction_id` | bigint | Partition key |
| `complaint_id` | text | Clustering column |
| `complaint_date` | text | Complaint date |
| `complaint_category` | text | Complaint category |
| `complaint_text` | text | Complaint details |
| `complaint_status` | text | Complaint status |

The table definitions are maintained in `../cassandra/init/01_schema.cql`.

## Consumer configuration

The Compose service accepts these environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka-recovery:9092` | Kafka bootstrap address |
| `KAFKA_TOPIC` | `complaints` | Input topic |
| `KAFKA_GROUP_ID` | `afrimarket-complaint-cassandra` | Consumer group |
| `CASSANDRA_HOST` | `afrimarket-cassandra` | Existing Cassandra container hostname |
| `CASSANDRA_PORT` | `9042` | Cassandra native transport port |
| `CASSANDRA_KEYSPACE` | `afrimarket` | Cassandra keyspace |
| `LOG_LEVEL` | `INFO` | Logging level |

The root Compose configuration sets `CASSANDRA_HOST` to `afrimarket-cassandra`, the hostname of the existing standalone Cassandra container on `myNetwork`.

## Build and run

From the repository root:

    docker compose build complaint-consumer
    docker compose up -d --no-deps complaint-consumer

Inspect consumer logs:

    docker compose logs -f complaint-consumer

Publish complaint events using the producer service:

    docker compose exec producer python producer.py complaints --start 0 --limit 1200 --delay 0.0

Check stored records:

    docker exec afrimarket-cassandra cqlsh -e "SELECT COUNT(*) FROM afrimarket.complaints_by_id; SELECT COUNT(*) FROM afrimarket.complaints_by_transaction;"

The count queries are useful for small project validation datasets; unrestricted Cassandra counts can be expensive on large production tables.

## Delivery and failure behavior

The consumer disables automatic Kafka offset commits. For each valid event, it writes to both Cassandra tables and then commits the corresponding Kafka offset. If a write or processing step fails, the offset is left uncommitted so Kafka can redeliver the event. The outer process retries after an exception.

Both writes use the complaint ID and transaction ID as primary-key values. Reprocessing the same event therefore targets the same Cassandra rows.

Invalid events are logged and left uncommitted for investigation. A persistently invalid event can block progress for its partition until it is addressed.

## Validation status

The local Cassandra schema is aligned with the two table definitions above. The complaint consumer has been tested successfully against the existing Kafka and Cassandra containers.

**Verified runtime integration test (November 18, 2026):**
- Processed all 1,200 complaint messages from the Kafka `complaints` topic.
- Confirmed 1,200 records in `afrimarket.complaints_by_id`.
- Confirmed 1,200 records in `afrimarket.complaints_by_transaction`.
- Verified zero consumer lag across all three Kafka partitions.
- Inspected sample records and confirmed the expected complaint fields.

The consumer writes to both Cassandra lookup tables before committing each Kafka offset. The consumer must be started with `--no-deps` because Kafka and Cassandra are independently managed containers already attached to `myNetwork`.
