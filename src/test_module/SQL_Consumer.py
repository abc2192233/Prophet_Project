from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('test',
                         bootstrap_servers='m.cdh:9092',
                         auto_offset_reset='earliest'
                         )

i = 0

for message in consumer:
    print(i, '\t', message)
    i = i + 1
    if i > 10000:
        exit(0)
