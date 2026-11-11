import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
HDFS_URL = os.getenv("HDFS_URL", "http://hadoop:9870")
HDFS_USER = "root"
DATA_DIR = os.getenv("DATA_DIR", "/data")

TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions.csv")
COMPLAINTS_FILE = os.path.join(DATA_DIR, "complaints.csv")

# Column names in your AfriMarket files
TXN_ID_COL = "TransactionID"
TXN_TIME_COL = "TransactionDT"
FRAUD_COL = "isFraud"
# Columns the producer sends for each transaction (the model needs them).
# When features.py needs more columns, add their names to this list.


# Columns the producer sends for each transaction (same as features.py needs).
TXN_COLUMNS = [
    # ID and time
    "TransactionID", "TransactionDT",
    # Numeric columns used by features.py (BASE_NUMERIC)
    "TransactionAmt", "card1", "card2", "card3", "card5", "addr1", "dist1",
    "C1", "C2", "C5", "C6", "C9", "C11", "C13", "C14", "D1", "D4", "D10", "D15",
    # Text columns used by features.py (CATEGORICAL)
    "ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain", "DeviceType",
    # The answer: for evaluation only, NOT a model input
    "isFraud",
]
COMPLAINT_TXN_COL = "TransactionID"
COMPLAINT_DATE_COL = "ComplaintDate"

# TransactionDT = seconds since this date (matches your complaint dates)
REFERENCE_DATE = "2026-01-01"

TOPICS = {"transactions": TRANSACTIONS_FILE, "complaints": COMPLAINTS_FILE}
PARTITIONS = 3
# Topic for rows that fail validation (missing or wrong values)
INVALID_TOPIC = "invalid_records"

HDFS_RAW = "/fraud/raw"
HDFS_REPORTS = "/fraud/reports"
