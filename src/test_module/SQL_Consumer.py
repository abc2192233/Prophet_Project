from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('cpu_info',
                         bootstrap_servers='m.cdh:9092',
                         auto_offset_reset='earliest'
                         )

i = 0

for message in consumer:
    print(i, '\t', message.value)
    i = i + 1
    if i > 11:
        exit(0)
