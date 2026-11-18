import json
import logging
import os
import signal
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-recovery:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "complaints")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "afrimarket-complaint-cassandra")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "afrimarket-cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "afrimarket")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("complaint-consumer")

running = True


def stop_handler(signum, frame):
    global running
    running = False
    logger.info("Shutdown requested")


signal.signal(signal.SIGTERM, stop_handler)
signal.signal(signal.SIGINT, stop_handler)


def required_text(message, field):
    value = message.get(field)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required field: {field}")
    return str(value).strip()


def normalize_complaint(message):
    complaint_id = required_text(message, "ComplaintID")
    transaction_id = int(required_text(message, "TransactionID"))
    complaint_date = required_text(message, "ComplaintDate")
    complaint_text = required_text(message, "ComplaintText")

    category = message.get("ComplaintCategory")
    status = message.get("ComplaintStatus")

    return (
        complaint_id,
        transaction_id,
        complaint_date,
        None if category is None else str(category),
        complaint_text,
        None if status is None else str(status),
    )


def main():
    logger.info("Connecting to Cassandra at %s:%s", CASSANDRA_HOST, CASSANDRA_PORT)

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = None
    consumer = None

    try:
        session = cluster.connect()
        session.set_keyspace(CASSANDRA_KEYSPACE)

        by_id = session.prepare("""
            INSERT INTO complaints_by_id (
                complaint_id, transaction_id, complaint_date,
                complaint_category, complaint_text, complaint_status
            ) VALUES (?, ?, ?, ?, ?, ?)
        """)

        by_transaction = session.prepare("""
            INSERT INTO complaints_by_transaction (
                transaction_id, complaint_id, complaint_date,
                complaint_category, complaint_text, complaint_status
            ) VALUES (?, ?, ?, ?, ?, ?)
        """)

        by_id.consistency_level = ConsistencyLevel.LOCAL_ONE
        by_transaction.consistency_level = ConsistencyLevel.LOCAL_ONE

        logger.info("Connected to Cassandra keyspace '%s'", CASSANDRA_KEYSPACE)
        logger.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP)

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000,
            max_poll_records=100,
        )

        logger.info(
            "Listening to topic '%s' with consumer group '%s'",
            KAFKA_TOPIC,
            KAFKA_GROUP_ID,
        )

        while running:
            records = consumer.poll(timeout_ms=1000, max_records=100)

            for partition, messages in records.items():
                for record in messages:
                    try:
                        values = normalize_complaint(record.value)

                        session.execute(by_id, values)
                        session.execute(
                            by_transaction,
                            (
                                values[1], values[0], values[2],
                                values[3], values[4], values[5],
                            ),
                        )

                        consumer.commit({
                            TopicPartition(record.topic, record.partition):
                                OffsetAndMetadata(record.offset + 1, "")
                        })
                        logger.info(
                            "Stored complaint %s for transaction %s",
                            values[0],
                            values[1],
                        )

                    except (ValueError, TypeError, json.JSONDecodeError) as exc:
                        logger.error(
                            "Invalid complaint at partition %s offset %s: %s",
                            record.partition,
                            record.offset,
                            exc,
                        )
                        # Do not commit invalid records; retain them for investigation.
                        raise

                    except Exception:
                        logger.exception(
                            "Failed to process complaint at partition %s offset %s",
                            record.partition,
                            record.offset,
                        )
                        # Leave the offset uncommitted so Kafka can redeliver it.
                        raise

    finally:
        if consumer is not None:
            consumer.close()
        if session is not None:
            session.shutdown()
        cluster.shutdown()
        logger.info("Complaint consumer stopped")


if __name__ == "__main__":
    while True:
        try:
            main()
            break
        except Exception:
            logger.exception("Consumer failed; retrying in 10 seconds")
            if not running:
                break
            time.sleep(10)
