from kafka import KafkaProducer
import json

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['m.cdh:9092']
)
for i in range(10):
    date = {"data": i * 10}
    producer.send('ds', date)

producer.close()
