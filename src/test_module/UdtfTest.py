from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableFunction, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udtf, udf


class Split(TableFunction):
    def eval(self, string):
        for s in string.init_data(" "):
            yield s, len(s)


env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
table_env.get_config().get_configuration().set_string("pipeline.jars",
                                                      "file:///root/PycharmProjects/Prophet_Project/lib/flink-sql-connector-kafka_2.11-1.12.3.jar;file:///root/PycharmProjects/Prophet_Project/lib/json.jar")
kafka_bootstrap_server = 'm.cdh:9092'
kafka_source_topic = 'source_topic'
kafka_scan_startup_mode = 'earliest-offset'

table_env.execute_sql(
    f"""
        CREATE TABLE source_table(
          a STRING
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{kafka_source_topic}',
          'properties.bootstrap.servers' = '{kafka_bootstrap_server}',
          'scan.startup.mode' = '{kafka_scan_startup_mode}',
          'format' = 'json'
          
          
          
          
           
        )
        """
)

split = udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()])
table_env.create_temporary_function('split', split)
table_env.execute_sql('SELECT * FROM source_table, LATERAL TABLE(split(a)) as T(word, length)').print()
