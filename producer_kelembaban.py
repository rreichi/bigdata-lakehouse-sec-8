from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

gudangs = ['G1', 'G2', 'G3']

while True:
    data = {
        "gudang_id": random.choice(gudangs),
        "kelembaban": random.randint(65, 80)
    }
    producer.send('sensor-kelembaban-gudang', value=data)
    print("Kirim kelembaban:", data)
    time.sleep(1)
