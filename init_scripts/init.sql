CREATE TABLE IF NOT EXISTS eligible_customers (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    amount INTEGER NOT NULL,
    cashback NUMERIC(10, 2) NOT NULL,
    merchant_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    payment_method VARCHAR(20),
    batch_id BIGINT
);

CREATE TABLE IF NOT EXISTS error_table (
    id SERIAL PRIMARY KEY,
    value TEXT,
    event_timestamp TIMESTAMP,
    batch_id BIGINT
);


