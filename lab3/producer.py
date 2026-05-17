import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['broker1:29092', 'broker2:29093'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Producer connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print("Kafka brokers not available, retrying in 5 seconds...")
            time.sleep(5)

producer = get_producer()

with open('Divvy_Trips_2019_Q4.csv', mode='r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file) 
    
    count = 0
    for row in csv_reader:
        producer.send('Topic1', value=row)
        producer.send('Topic2', value=row)
        
        count += 1
        if count % 10000 == 0:
            print(f"Відправлено {count} повідомлень...")

producer.flush()
print("Всі дані успішно відправлено!")