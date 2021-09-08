from pyflink.common.serialization import JsonRowDeserializationSchema, DeserializationSchema, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import DataTypes

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars('file:///root/PycharmProjects/Prophet_Project/lib/flink-sql-connector-kafka_2.11-1.12.3.jar')

deserialization_schema = JsonRowDeserializationSchema.builder()

kafka_consumer = FlinkKafkaConsumer(
    topics='ds',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'm.cdh:9092'})

ds = env.add_source(kafka_consumer)
ds.connect()
ds.print()

env.execute('test')
