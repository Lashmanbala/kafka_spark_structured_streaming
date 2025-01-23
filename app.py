import time
import random
import json

transaction_counter = 1
customer_id = [f"cust_{i}" for i in range(1, 6)]      
merchant_id = [f"merch_{i}" for i in range(1, 4)]
product_id = [f"prod_{i}" for i in range(1, 8)] 

try:
    while True:
        event = {'transaction_id':f'trans_{transaction_counter}',
            'customer_id' : random.choice(customer_id),
            'timestamp' : time.time(),
            'product_id' : random.choice(product_id),
            'amount' : random.randint(100, 1000),
            'merchant_id' : random.choice(merchant_id)
            }
    
        print(event)
    
        transaction_counter += 1

        time.sleep(5)
except KeyboardInterrupt:
    print('Stopped by User...')

