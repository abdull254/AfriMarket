CREATE DATABASE IF NOT EXISTS fraud;
USE fraud;

CREATE TABLE IF NOT EXISTS scores (
    TransactionID BIGINT NOT NULL,
    TransactionDT DOUBLE,
    risk_score DOUBLE NOT NULL,
    decision VARCHAR(10) NOT NULL,
    stored_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (TransactionID),
    INDEX idx_scores_decision (decision),
    CHECK (risk_score >= 0 AND risk_score <= 1),
    CHECK (decision IN ('APPROVE', 'REVIEW', 'BLOCK'))
);

CREATE TABLE IF NOT EXISTS investigation_cases (
    case_id BIGINT NOT NULL AUTO_INCREMENT,
    TransactionID BIGINT NOT NULL,
    case_status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    assigned_to VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (case_id),
    FOREIGN KEY (TransactionID) REFERENCES scores(TransactionID),
    CHECK (case_status IN ('OPEN', 'IN_PROGRESS', 'RESOLVED', 'CLOSED'))
);
