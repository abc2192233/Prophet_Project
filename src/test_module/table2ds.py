from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from src.config import Config

env_stream = StreamExecutionEnvironment.get_execution_environment()
env_table = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(stream_execution_environment=env_stream, environment_settings=env_table)
config = Config.connect_config()
t_env.get_config().get_configuration().set_string("pipeline.jars", config.jar_path)

t_env.execute_sql("""
            CREATE TABLE source_table(
               data STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'ds',
                'properties.bootstrap.servers' = 'm.cdh:9092',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
""")

# t_env.execute_sql('SELECT * FROM source_table').print()

ds = t_env.to_append_stream(
    t_env.from_path('source_table'),
    Types.ROW([Types.STRING()]))

ds.print()

env_stream.execute()

