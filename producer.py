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

def every(n, counter):
    if n > 0 and counter % n == 0:
        return True

def build_transaction(counter):
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
    # trans 30 breaks 30 breaks and trans 60 breaks timestamp.
    if every(MALFORMED_EVERY, counter):
        if (counter // MALFORMED_EVERY) % 2 == 1:
            broken_field = "customer_id"
        else:
            broken_field = "timestamp"
        
        log.warning(f"trans_{counter} is MALFORMED ({broken_field} = null)")
        event[broken_field] = None
    
    return event

def main():
    transaction_counter = 1

    app = Application(
        broker_address='localhost:9092',
        loglevel='DEBUG',
    )

    try:
        while True:
            event = {'transaction_id':f'trans_{transaction_counter}',
                    'customer_id' : random.choice(customer_id),
                    'timestamp' : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'product_id' : random.choice(product_id),
                    'amount' : random.randint(100, 1000),
                    'merchant_id' : random.choice(merchant_id)
                    }
        
            with app.get_producer() as producer:
                logging.info(f'got record for trans_{transaction_counter}')

                producer.produce(topic='cashback_topic', value=json.dumps(event))

                logging.info('Produced the record into kafka...sleeping...')

            transaction_counter += 1
            time.sleep(30)
            
    except KeyboardInterrupt:
        print('Stopped by User...')

if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    main()