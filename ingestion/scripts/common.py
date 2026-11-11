import json, math, time
import pandas as pd
import config


def load_transactions():
    df = pd.read_csv(config.TRANSACTIONS_FILE).copy()
    if config.TXN_TIME_COL in df.columns:
        ref = pd.Timestamp(config.REFERENCE_DATE)
        df["transaction_time"] = ref + pd.to_timedelta(df[config.TXN_TIME_COL], unit="s")
    return df


def load_complaints():
    df = pd.read_csv(config.COMPLAINTS_FILE)
    if config.COMPLAINT_DATE_COL and config.COMPLAINT_DATE_COL in df.columns:
        df[config.COMPLAINT_DATE_COL] = pd.to_datetime(df[config.COMPLAINT_DATE_COL], errors="coerce")
    return df


def load_dataset(topic):
    return load_transactions() if topic == "transactions" else load_complaints()


def key_column(topic):
    return config.TXN_ID_COL if topic == "transactions" else config.COMPLAINT_TXN_COL


def row_to_message(row):
    out = {}
    for k, v in row.items():
        if isinstance(v, pd.Timestamp):
            out[k] = None if pd.isna(v) else v.isoformat()
        elif hasattr(v, "item"):
            v = v.item()
            out[k] = None if isinstance(v, float) and math.isnan(v) else v
        elif isinstance(v, float) and math.isnan(v):
            out[k] = None
        else:
            out[k] = v
    return out


def to_json_bytes(obj):
    return json.dumps(obj).encode("utf-8")


def hdfs_client(wait_seconds=120):
    from hdfs import InsecureClient
    client = InsecureClient(config.HDFS_URL, user=config.HDFS_USER)
    deadline = time.time() + wait_seconds
    while True:
        try:
            client.makedirs("/fraud")
            return client
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError(f"HDFS not ready: {exc}")
            print("Waiting for HDFS...")
            time.sleep(5)


def validate(topic, msg):
    """Check one message. Returns a list of problems (an empty list = the message is OK)."""
    problems = []

    # Which values must not be empty
    if topic == "transactions":
        required = ["TransactionID", "TransactionDT", "TransactionAmt"]
    else:
        required = ["ComplaintID", "TransactionID", "ComplaintDate", "ComplaintText"]

    # Rule 1: required values must be present
    for col in required:
        if msg.get(col) is None or msg.get(col) == "":
            problems.append(f"missing {col}")

    # Rule 2: TransactionID must be a whole number (it links transactions and complaints)
    tid = msg.get("TransactionID")
    if tid is not None:
        try:
            msg["TransactionID"] = int(tid)
        except (TypeError, ValueError):
            problems.append("TransactionID is not a whole number")

    # Rule 3: rules for each topic
    if topic == "transactions":
        amt = msg.get("TransactionAmt")
        if amt is not None:
            try:
                if float(amt) <= 0:
                    problems.append("TransactionAmt must be greater than 0")
            except (TypeError, ValueError):
                problems.append("TransactionAmt is not a number")
    else:
        text = msg.get("ComplaintText")
        if text is not None and len(str(text).strip()) < 5:
            problems.append("ComplaintText is too short")

    return problems