-- AfriMarket star schema: readable customers, sellers, products, dates and devices
-- around the transactions. Filled by data_generation/load_star_schema.py from the
-- CSV files in data/enriched/ (made by data_generation/generate_star_schema.py).
--
-- MySQL runs this file only when its data volume is new. On an existing volume,
-- load_star_schema.py --create runs it for you.
USE fraud;

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id        VARCHAR(12) NOT NULL,
    card1              INT NOT NULL,
    addr1              INT,
    country            VARCHAR(40) NOT NULL,
    city               VARCHAR(40) NOT NULL,
    currency           CHAR(3) NOT NULL,
    signup_date        DATE NOT NULL,
    email_domain       VARCHAR(60),
    phone_masked       VARCHAR(30),
    preferred_payment  VARCHAR(20) NOT NULL,
    segment            VARCHAR(20) NOT NULL,
    PRIMARY KEY (customer_id),
    INDEX idx_customer_card (card1, addr1),
    INDEX idx_customer_country (country)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id      VARCHAR(10) NOT NULL,
    seller_name    VARCHAR(80) NOT NULL,
    country        VARCHAR(40) NOT NULL,
    city           VARCHAR(40) NOT NULL,
    main_category  VARCHAR(40) NOT NULL,
    joined_date    DATE NOT NULL,
    rating         DECIMAL(2,1) NOT NULL,
    is_verified    BOOLEAN NOT NULL,
    PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id         VARCHAR(10) NOT NULL,
    product_name       VARCHAR(80) NOT NULL,
    category           VARCHAR(40) NOT NULL,
    is_physical        BOOLEAN NOT NULL,
    unit_price_usd     DECIMAL(10,2) NOT NULL,
    default_seller_id  VARCHAR(10) NOT NULL,
    PRIMARY KEY (product_id),
    INDEX idx_product_category (category),
    FOREIGN KEY (default_seller_id) REFERENCES dim_seller(seller_id)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INT NOT NULL,
    date          DATE NOT NULL,
    year          SMALLINT NOT NULL,
    quarter       TINYINT NOT NULL,
    month         TINYINT NOT NULL,
    month_name    VARCHAR(10) NOT NULL,
    week_of_year  TINYINT NOT NULL,
    day_of_month  TINYINT NOT NULL,
    weekday       TINYINT NOT NULL,
    weekday_name  VARCHAR(10) NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    is_month_end  BOOLEAN NOT NULL,
    PRIMARY KEY (date_key)
);

CREATE TABLE IF NOT EXISTS dim_device (
    device_id           VARCHAR(16) NOT NULL,
    device_type         VARCHAR(20),
    device_info         VARCHAR(100),
    first_seen          DATETIME NOT NULL,
    last_seen           DATETIME NOT NULL,
    txn_count           INT NOT NULL,
    distinct_customers  INT NOT NULL,
    is_shared           BOOLEAN NOT NULL,
    PRIMARY KEY (device_id)
);

CREATE TABLE IF NOT EXISTS fact_transaction (
    TransactionID               BIGINT NOT NULL,
    TransactionDT               BIGINT NOT NULL,
    transaction_ts_utc          DATETIME NOT NULL,
    transaction_ts_local        DATETIME NOT NULL,
    utc_offset                  TINYINT NOT NULL,
    date_key                    INT NOT NULL,
    local_hour                  TINYINT NOT NULL,
    local_weekday               VARCHAR(10) NOT NULL,
    is_weekend                  BOOLEAN NOT NULL,
    customer_id                 VARCHAR(12) NOT NULL,
    seller_id                   VARCHAR(10) NOT NULL,
    product_id                  VARCHAR(10) NOT NULL,
    category                    VARCHAR(40) NOT NULL,
    quantity                    SMALLINT NOT NULL,
    TransactionAmt              DECIMAL(12,3) NOT NULL,
    currency                    CHAR(3) NOT NULL,
    fx_rate_usd                 DECIMAL(10,2) NOT NULL,
    amount_local                DECIMAL(16,2) NOT NULL,
    payment_method              VARCHAR(20) NOT NULL,
    card_network                VARCHAR(20),
    card_type                   VARCHAR(20),
    country                     VARCHAR(40) NOT NULL,
    city                        VARCHAR(40) NOT NULL,
    device_id                   VARCHAR(16) NOT NULL,
    ip_address                  VARCHAR(15) NOT NULL,
    is_new_device_for_customer  BOOLEAN NOT NULL,
    is_new_ip_for_customer      BOOLEAN NOT NULL,
    device_customers_so_far     INT NOT NULL,
    ip_customers_so_far         INT NOT NULL,
    isFraud                     TINYINT NOT NULL,   -- label, for evaluation only
    PRIMARY KEY (TransactionID),
    INDEX idx_fact_customer (customer_id),
    INDEX idx_fact_seller (seller_id),
    INDEX idx_fact_date (date_key),
    INDEX idx_fact_payment (payment_method),
    INDEX idx_fact_device (device_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (device_id) REFERENCES dim_device(device_id)
);

CREATE TABLE IF NOT EXISTS fact_delivery (
    delivery_id       VARCHAR(12) NOT NULL,
    TransactionID     BIGINT NOT NULL,
    delivery_type     VARCHAR(10) NOT NULL,
    courier           VARCHAR(40),
    origin_city       VARCHAR(40),
    destination_city  VARCHAR(40) NOT NULL,
    cross_border      BOOLEAN NOT NULL,
    shipped_at        DATETIME,
    promised_at       DATETIME,
    delivered_at      DATETIME,
    delivery_days     DECIMAL(5,1),
    delivery_status   VARCHAR(12) NOT NULL,
    is_late           BOOLEAN NOT NULL,
    PRIMARY KEY (delivery_id),
    UNIQUE KEY uq_delivery_txn (TransactionID),
    INDEX idx_delivery_status (delivery_status),
    FOREIGN KEY (TransactionID) REFERENCES fact_transaction(TransactionID),
    CHECK (delivery_status IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED', 'CANCELLED', 'LOST', 'FAILED'))
);

CREATE TABLE IF NOT EXISTS fact_refund (
    refund_id      VARCHAR(10) NOT NULL,
    TransactionID  BIGINT NOT NULL,
    refund_type    VARCHAR(12) NOT NULL,
    reason         VARCHAR(60) NOT NULL,
    amount_usd     DECIMAL(12,2) NOT NULL,
    requested_at   DATETIME NOT NULL,
    resolved_at    DATETIME,
    refund_status  VARCHAR(15) NOT NULL,
    PRIMARY KEY (refund_id),
    INDEX idx_refund_txn (TransactionID),
    INDEX idx_refund_type (refund_type),
    FOREIGN KEY (TransactionID) REFERENCES fact_transaction(TransactionID),
    CHECK (refund_type IN ('REFUND', 'RETURN', 'CHARGEBACK'))
);

CREATE TABLE IF NOT EXISTS device_ip_history (
    customer_id  VARCHAR(12) NOT NULL,
    device_id    VARCHAR(16) NOT NULL,
    ip_address   VARCHAR(15) NOT NULL,
    first_seen   DATETIME NOT NULL,
    last_seen    DATETIME NOT NULL,
    txn_count    INT NOT NULL,
    PRIMARY KEY (customer_id, device_id, ip_address),
    INDEX idx_history_device (device_id),
    INDEX idx_history_ip (ip_address),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (device_id) REFERENCES dim_device(device_id)
);

CREATE TABLE IF NOT EXISTS complaints_enriched (
    ComplaintID            VARCHAR(10) NOT NULL,
    TransactionID          BIGINT NOT NULL,
    ComplaintDate          DATETIME NOT NULL,
    ComplaintCategory      VARCHAR(30) NOT NULL,
    ComplaintText          TEXT NOT NULL,
    ComplaintStatus        VARCHAR(20) NOT NULL,
    channel                VARCHAR(20) NOT NULL,
    priority               VARCHAR(10) NOT NULL,
    first_response_at      DATETIME,
    resolved_at            DATETIME,
    resolution_hours       DECIMAL(8,1),
    customer_id            VARCHAR(12) NOT NULL,
    seller_id              VARCHAR(10) NOT NULL,
    product_id             VARCHAR(10) NOT NULL,
    delivery_status        VARCHAR(12),
    refund_id              VARCHAR(10),
    source                 VARCHAR(30) NOT NULL,
    OriginalComplaintText  TEXT,
    PRIMARY KEY (ComplaintID),
    INDEX idx_complaint_txn (TransactionID),
    INDEX idx_complaint_category (ComplaintCategory),
    FOREIGN KEY (TransactionID) REFERENCES fact_transaction(TransactionID),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (refund_id) REFERENCES fact_refund(refund_id)
);
