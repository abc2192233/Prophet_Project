from kafka import KafkaProducer
import json

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['m.cdh:9092']
)
for i in range(10):
    data = {'a': i}
    producer.send('ds', data)

producer.close()
