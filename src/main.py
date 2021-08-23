# use a StreamTableEnvironment to execute the queries
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableFunction


env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

t_env.get_config().get_configuration().set_string("pipeline.jars",
                                                  "file:///root/PycharmProjects/Prophet_Project/lib/flink-sql-connector-kafka_2.11-1.12.3.jar;file:///root/PycharmProjects/Prophet_Project/lib/json.jar")
kafka_bootstrap_server = 'm.cdh:9092'
kafka_source_topic = 'source_topic'
kafka_scan_startup_mode = 'earliest-offset'

source_ddl = f"""
        CREATE TABLE source_table(
            dat ARRAY< ROW<`start` BIGINT, `end` BIGINT, `endpoint` STRING, `nid` STRING, `counter` STRING, `dstype` STRING, `step` INT, `values` ARRAY<ROW<`timestamp` BIGINT, `value` DOUBLE>>, `comparsion` BIGINT>>,
            err VARCHAR 
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{kafka_source_topic}',
          'properties.bootstrap.servers' = '{kafka_bootstrap_server}',
          'scan.startup.mode' = '{kafka_scan_startup_mode}',
          'format' = 'json'
        )
        """

t_env.execute_sql(source_ddl)

t_env.execute_sql("""
    CREATE TABLE print_table (
        dat ARRAY<ROW<`start` BIGINT, `end` BIGINT, `endpoint` STRING, `nid` STRING, `counter` STRING, `dstype` STRING, `step` INT, `values` ARRAY<ROW<`timestamp` BIGINT, `value` DOUBLE>>, `comparsion` BIGINT>>
    ) WITH (
        'connector' = 'print'
    )
""")

# t_env.execute_sql('INSERT INTO print_table SELECT dat as dat FROM source_table where err IS NOT NULL').wait()

t_env.execute_sql('SELECT dat[1].`endpoint` as ip1, dat[2].`endpoint` as ip2 FROM source_table').print()







