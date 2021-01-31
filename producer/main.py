import os
import time
from kafka import KafkaProducer
import kafka.errors
import csv
import time
from datetime import datetime
import json

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "crime-reporting"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

file = csv.reader(open('data_real.csv'), delimiter=',')
headers = next(file, None)

print("Data Loaded!")
print("Headers: ")
print(headers)

data_list = list(file)
print("Initiating reporting...")

for i, line in enumerate(data_list):
    producer.send(TOPIC, key=bytes(line[0], 'utf-8'), value=line)
    print('Case Number: ' + str(line[1]) + ' sent!')

    current_date_time = datetime.strptime(line[2], "%Y-%m-%d %H:%M:%S")

    if len(data_list) > i+1:
      next_event = data_list[i+1]
      next_date_time = datetime.strptime(next_event[2], "%Y-%m-%d %H:%M:%S")
    
      difference = next_date_time - current_date_time
      print('Sleeping ' + str(difference.seconds/60) + ' seconds...')
      time.sleep(difference.seconds/60)


