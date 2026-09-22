import time
import random
import json
from quixstreams import Application
from datetime import datetime, timedelta
import logging

log = logging.getLogger("producer")

customer_id = [f"cust_{i}" for i in range(1, 6)]      
merchant_id = [f"merch_{i}" for i in range(1, 4)]
product_id = [f"prod_{i}" for i in range(1, 8)] 
payment_methods = ["card", "upi", "wallet"]

LATE_EVERY = 15    # Every 15th evnt is a late event
LATE_MINUTES = 10  # How far in the past a late event is
V2_SCHEMA_EVERY = 2 # event carries the newer optional field
MALFORMED_EVERY = 30 # broken customer_id / timestamp
DUPLICATE_EVERY = 20 # same event sent twice

TRANSACTIONS_TOPIC = "cashback_topic"

def every(n, counter):
    if n > 0 and counter % n == 0:
        return True

def build_transaction(counter ):
    event_time = datetime.now()

    if every(LATE_EVERY, counter):  # Late event: happened LATE_MINUTES ago but is sent now.
        log.warning(f"trans_{counter} is a LATE event ({LATE_MINUTES} min old)")
        event_time -= timedelta(minutes=LATE_MINUTES)

    event = {
        "transaction_id": f"trans_{counter}",
        "customer_id": random.choice(customer_id),
        "timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "product_id": random.choice(product_id),
        "amount": random.randint(100, 1000),
        "merchant_id": random.choice(merchant_id),
    }

    # Schema evolution: newer producers add an optional field, older ones don't.
    # Consumers must cope with both shapes.
    if every(V2_SCHEMA_EVERY, counter):
        event["payment_method"] = random.choice(payment_methods)

    # Alternates between breaking customer_id and breaking timestamp on 30, 60, 90,....
    # trans 30 breaks customer_id and trans 60 breaks timestamp alternatively and so on.
    if every(MALFORMED_EVERY, counter):
        if (counter // MALFORMED_EVERY) % 2 == 1:
            broken_field = "customer_id"
        else:
            broken_field = "timestamp"
        
        log.warning(f"trans_{counter} is MALFORMED ({broken_field} = null)")
        event[broken_field] = None
    
    return event

def make_on_delivery(label):
    def callback(err, msg):
        """Called by the Kafka client once the broker acks (or rejects) a record."""
        if err is not None:
            log.error("%s delivery FAILED for key=%s: %s", label, msg.key(), err)
            return
        log.info("%s delivered -> %s [partition %d] @ offset %d", label, msg.topic(), msg.partition(), msg.offset())
    return callback

def main():

    app = Application(
        broker_address='localhost:9092',
        loglevel='INFO',
        producer_extra_config={
            "acks": "all",
            "enable.idempotence": True,
        },
    )

    transaction_counter = 1

    with app.get_producer() as producer:
        try:
            while True:
                txn = build_transaction(transaction_counter)
                payload = json.dumps(txn)   

                if txn["customer_id"]:
                    key = txn["customer_id"]    # so all events for a customer land in the same partition and stay in order.
                else:
                    key = "unknown"   # Malformed events may have no customer_id, so fall back to a fixed key.
                
                producer.produce(
                    topic='cashback_topic', 
                    key=key,
                    value=payload,
                    on_delivery=make_on_delivery(TRANSACTIONS_TOPIC),
                    )

                # Duplicate: resend the exact same event (same transaction_id).
                # Downstream should dedupe with dropDuplicates.
                if every(DUPLICATE_EVERY, transaction_counter):
                    log.warning("Re-sending duplicate of %s", txn["transaction_id"])
                    producer.produce(
                        topic=TRANSACTIONS_TOPIC,
                        key=key,
                        value=payload,
                        on_delivery=make_on_delivery(TRANSACTIONS_TOPIC),
                    )

                transaction_counter += 1
                time.sleep(30)
                
        except KeyboardInterrupt:
            print('Stopped by User...')

if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    main()