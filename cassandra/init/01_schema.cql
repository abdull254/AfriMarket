-- Create the AfriMarket keyspace
CREATE KEYSPACE IF NOT EXISTS afrimarket
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE afrimarket;

-- Find an individual complaint using ComplaintID
CREATE TABLE IF NOT EXISTS complaints_by_id (
    complaint_id text PRIMARY KEY,
    transaction_id bigint,
    complaint_date text,
    complaint_category text,
    complaint_text text,
    complaint_status text
);

-- Find complaints associated with a transaction
CREATE TABLE IF NOT EXISTS complaints_by_transaction (
    transaction_id bigint,
    complaint_id text,
    complaint_date text,
    complaint_category text,
    complaint_text text,
    complaint_status text,
    PRIMARY KEY (transaction_id, complaint_id)
);
