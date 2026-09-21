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

LATE_EVERY = 15    # Means broken customer_id or timestamp
LATE_MINUTES = 10  # How far in the past a late event is

def every(n, counter):
    if n > 0 and counter % n == 0:
        return True

def build_transaction(counter):
    if every(LATE_EVERY, counter):
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