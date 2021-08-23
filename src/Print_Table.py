from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.typeinfo import Types


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)
    # specify connector and format jars
    t_env.get_config().get_configuration().set_string("pipeline.jars",
                                                      "file:///root/PycharmProjects/Prophet_Project/lib/flink-sql-connector-kafka_2.11-1.12.3.jar;file:///root/PycharmProjects/Prophet_Project/lib/json.jar")

    source_ddl = """
            CREATE TABLE source_table(
                dat ARRAY<ROW<`start` BIGINT, `end` BIGINT, `endpoint` STRING, `nid` STRING, `counter` STRING, `dstype` STRING, `step` INT, `values` ARRAY<ROW<`timestamp` BIGINT, `value` DOUBLE>>, `comparsion` BIGINT>>,
                err STRING
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'source_topic',
              'properties.bootstrap.servers' = 'm.cdh:9092',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    sink_ddl = """
            CREATE TABLE sink_table(
                dat ARRAY<ROW<`start` BIGINT, `end` BIGINT, `endpoint` STRING, `nid` STRING, `counter` STRING, `dstype` STRING, `step` INT, `values` ARRAY<ROW<`timestamp` BIGINT, `value` DOUBLE>>, `comparsion` BIGINT>>,
                err STRING
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'sink_topic',
              'properties.bootstrap.servers' = 'm.cdh:9092',
              'format' = 'json'
            )
            """

    t_env.execute_sql("""
        CREATE TABLE print (
                err STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # t_env.execute_sql('SELECT dat FROM source_table').print()
    # print(t_env.execute_sql('SELECT dat FROM source_table').print())
    # print(t_env.execute_sql('SELECT dat FROM source_table').print())

    t_env.sql_query("SELECT dat, err FROM source_table") \
        .execute_insert("sink_table").wait()


if __name__ == '__main__':
    log_processing()
