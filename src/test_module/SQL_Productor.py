from kafka import KafkaProducer
import json

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['m.cdh:9092']
)
# for i in range(10):
#     data = {
#         "dat": i,
#         "err": i
#     }
#     print(i)
#     producer.send('source_topic', data)


# for i in range(1):
#     data = {
#         "dat": [
#             {
#                 "start": 1629168429,
#                 "end": 1624539900,
#                 "endpoint": "10.140.0.10",
#                 "nid": "",
#                 "counter": "cpu.loadavg.1",
#                 "dstype": "GAUGE",
#                 "step": 30,
#                 "values": [
#                     {
#                         "timestamp": 1630035626,
#                         "value": i * 10
#                     },
#                     {
#                         "timestamp": 1628247270,
#                         "value": 0
#                     },
#                     {
#                         "timestamp": 1630035690,
#                         "value": 0
#                     }
#                 ],
#                 "comparison": 0
#             },
#             {
#                 "start": 1629168429,
#                 "end": 1624539900,
#                 "endpoint": "10.140.0.10",
#                 "nid": "",
#                 "counter": "cpu.loadavg.1",
#                 "dstype": "GAUGE",
#                 "step": 30,
#                 "values": [
#                     {
#                         "timestamp": 1628247240,
#                         "value": i * 10
#                     },
#                     {
#                         "timestamp": 1630035432,
#                         "value": 0
#                     },
#                     {
#                         "timestamp": 1630035432,
#                         "value": 0
#                     }
#                 ],
#                 "comparison": 0
#             },
#             {
#                 "start": 1630035432,
#                 "end": 1624539900,
#                 "endpoint": "10.140.0.10",
#                 "nid": "",
#                 "counter": "cpu.loadavg.1",
#                 "dstype": "GAUGE",
#                 "step": 30,
#                 "values": [
#                     {
#                         "timestamp": 1628247240,
#                         "value": i * 10
#                     },
#                     {
#                         "timestamp": 1628247270,
#                         "value": 0
#                     },
#                     {
#                         "timestamp": 1628247270,
#                         "value": 0
#                     }
#                 ],
#                 "comparison": 0
#             },
#             {
#                 "start": 1629168429,
#                 "end": 1624539900,
#                 "endpoint": "10.140.0.11",
#                 "nid": "",
#                 "counter": "cpu.loadavg.1",
#                 "dstype": "GAUGE",
#                 "step": 30,
#                 "values": [
#                     {
#                         "timestamp": 1628247240,
#                         "value": 0
#                     },
#                     {
#                         "timestamp": 1628247270,
#                         "value": 0
#                     }
#                 ],
#                 "comparison": 0
#             }
#         ],
#         "err": ""
#     }
#     print(i)
#     producer.send('source_topic', data)

data = {"dat": [
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.19", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.21", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.22", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.23", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.24", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.25", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.26", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.27", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.28", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.29", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.30", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 0.1}, {"timestamp": 1629168480, "value": 0.2},
                {"timestamp": 1629168510, "value": 0.3}, {"timestamp": 1629168540, "value": 0.4},
                {"timestamp": 1629168570, "value": 0.5}, {"timestamp": 1629168600, "value": 0.6}],
     "comparison": 0},
    {"start": 1629168429, "end": 1629168609, "endpoint": "10.66.21.31", "nid": "", "counter": "cpu.loadavg.1",
     "dstype": "GAUGE", "step": 30,
     "values": [{"timestamp": 1629168450, "value": 1.000000}, {"timestamp": 1629168480, "value": 2.000000},
                {"timestamp": 1629168510, "value": 3.000000}, {"timestamp": 1629168540, "value": 4.000000},
                {"timestamp": 1629168570, "value": 5.000000}, {"timestamp": 1629168600, "value": 6.000000}],
     "comparison": 0}], "err": ""}
producer.send('source_topic', data)

producer.close()
