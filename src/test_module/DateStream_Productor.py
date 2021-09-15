from kafka import KafkaProducer
import json

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['m.cdh:9092']
)
data = {"dat": [
    {"start": 1630035432, "end": 1624539900, "endpoint": "10.140.0.10", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1628247240, "value": 10}, {"timestamp": 1628247270, "value": 0},
                {"timestamp": 1628247270, "value": 0}], "comparison": 0}], "err": ""}
producer.send('source_topic', data)

producer.close()
