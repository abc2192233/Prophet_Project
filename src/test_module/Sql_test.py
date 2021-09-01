from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udtf, udf
import src.config.Config as Config
import src.udf.TrainFunc
from src.udf.TrainFunc import TrainFunc
import pandas as pd


@udf(result_type=DataTypes.TIMESTAMP(3))
def add(x):
    time_stamp = pd.to_datetime(x, utc=True, unit='s').tz_convert('Asia/Shanghai').tz_localize(None)
    return time_stamp


env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()

t_env = StreamTableEnvironment.create(environment_settings=env_settings)
config = Config.connect_config()
t_env.get_config().get_configuration().set_string("pipeline.jars", config.jar_path)


t_env.create_temporary_function("add", add)

kafka_bootstrap_server = 'm.cdh:9092'
kafka_source_topic = 'test'
kafka_scan_startup_mode = 'earliest-offset'

source_ddl = f"""
    CREATE TABLE source_table(
        time_stamp  STRING,
        data_info   TIMESTAMP(3),
        record_time TIMESTAMP(3) METADATA FROM 'timestamp'
--         WATERMARK FOR record_time AS record_time - INTERVAL '3' SECOND 
    ) WITH (
      'connector' = 'kafka',
      'topic' = '{kafka_source_topic}',
      'properties.bootstrap.servers' = '{kafka_bootstrap_server}',
      'scan.startup.mode' = '{kafka_scan_startup_mode}',
      'format' = 'json'
    )
    """

t_env.execute_sql(source_ddl)
t_env.execute_sql('DESCRIBE  source_table').print()
t_env.execute_sql('SELECT * FROM source_table').print()

# t_env.execute_sql("""
# SELECT TUMBLE_START(ts, INTERVAL '3' SECOND ) as wStart, ts
# FROM source_table
# GROUP BY TUMBLE(ts, INTERVAL '3' SECOND ), ts
# """).print()
#
# # t_env.sql_query("""
# SELECT
#     HOP_START(ts, INTERVAL '1' SECOND, INTERVAL '60' SECOND) AS start_time
# FROM
#     source_table
# """).execute_insert("sink").wait()

# t_env.execute_sql("""
#     WITH temp_table AS (
#     SELECT add(dat[1].`start`) as date_time FROM source_table
#     )
#     ALTER temp_table WATERMARK FOR date_time AS date_time - INTERVAL '5' SECOND
# """).print()
