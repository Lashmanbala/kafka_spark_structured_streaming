CREATE TABLE eligible_customers (
    customer_id VARCHAR(50),
    amount DOUBLE PRECISION,
    cashback DOUBLE PRECISION,
    merchant_id VARCHAR(50),
    timestamp TIMESTAMP
);

CREATE TABLE error_table(
    value VARCHAR(50),
    event_timestamp TIMESTAMP,
    batch_id INT
);
