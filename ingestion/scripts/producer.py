import argparse, time
from datetime import datetime, timezone
from kafka import KafkaProducer
import config
from common import load_dataset, key_column, row_to_message, to_json_bytes, validate

# --- Options typed after the script name ---
p = argparse.ArgumentParser()
p.add_argument("topic", choices=list(config.TOPICS))   # "transactions" or "complaints"
p.add_argument("--start", type=int, default=0)         # first row to send
p.add_argument("--limit", type=int, default=None)      # stop before this row
p.add_argument("--delay", type=float, default=0.0)     # seconds between messages (live demo)
args = p.parse_args()

# --- Load the CSV and keep the chosen rows ---
# --- Load the CSV ---
df = load_dataset(args.topic)

if args.topic == "transactions":
    # Keep only the columns the model needs, plus the readable date
    # (transaction_time is created in common.py from TransactionDT)
    df = df[config.TXN_COLUMNS + ["transaction_time"]]
else:
    # Send complaints in time order (oldest first), like in real life
    df = df.sort_values(config.COMPLAINT_DATE_COL)

# --- Keep the chosen rows (--start and --limit) ---
df = df.iloc[args.start:args.limit]

key_col = key_column(args.topic)
print(f"Sending {len(df):,} rows to '{args.topic}'")

# --- Connect to Kafka, with safety settings ---
producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_BOOTSTRAP,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=to_json_bytes,
    linger_ms=50,
    compression_type="gzip",
    max_request_size=5_000_000,
    acks="all",                                 # wait until Kafka confirms each message is stored
    retries=10,                                 # if sending fails, try again up to 10 times
    retry_backoff_ms=1000,                      # wait 1 second between retries
    request_timeout_ms=120_000,                 # give a slow VM up to 2 minutes to answer
    max_in_flight_requests_per_connection=1,    # keep messages in order when retrying
)

# --- Count what Kafka really confirmed ---
stats = {"ok": 0, "failed": 0, "invalid": 0}

def on_success(metadata):
    stats["ok"] += 1                            # Kafka confirmed this message

def on_error(exc):
    stats["failed"] += 1                        # failed even after all retries
    print(f"  !! send failed: {exc}")

# --- Send each row as one message ---
for i, row in enumerate(df.to_dict(orient="records"), start=1):
    msg = row_to_message(row)
        # --- Check the row first: bad rows go to the invalid_records topic ---
    problems = validate(args.topic, msg)
    if problems:
        stats["invalid"] += 1
        print(f"  !! invalid {key_col}={msg.get(key_col)}: {problems}")
        bad = {"topic": args.topic, "problems": problems, "record": msg,
               "rejected_at": datetime.now(timezone.utc).isoformat()}
        producer.send(config.INVALID_TOPIC, key=msg.get(key_col), value=bad)
        continue    # skip the normal sending for this row
    msg["sent_at"] = datetime.now(timezone.utc).isoformat()
    future = producer.send(args.topic, key=msg.get(key_col), value=msg)
    future.add_callback(on_success)             # called when Kafka confirms
    future.add_errback(on_error)                # called if it fails

    if args.delay > 0:                          # live demo mode
        print(f"  -> {args.topic}  {key_col}={msg.get(key_col)}")
        producer.flush()
        time.sleep(args.delay)
    elif i % 5000 == 0:                         # bulk mode: show progress
        print(f"  {i:,} queued...")

producer.flush()
producer.close()
print(f"Done: {stats['ok']:,} confirmed by Kafka, {stats['failed']:,} failed, {stats['invalid']:,} invalid")
