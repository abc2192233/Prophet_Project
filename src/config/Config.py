class connect_config:
    def __init__(self):
        # 定义kafka连接配置
        self.kafka_bootstrap_server = 'm.cdh:9092'
        self.kafka_source_topic = 'source_topic'
        self.kafka_scan_startup_mode = 'latest-offset'    # 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'

        # 定义Redis连接配置
        self.redis_host = 'm.cdh'
        self.redis_password = '123456'
        self.redis_port = '6379'
        self.redis_history_info_db = 0
        self.redis_offline_model_db = 1
        self.redis_online_model_db = 2

        # jar包路径
        self.jar_path = "file:///root/PycharmProjects/Prophet_Project/lib/flink-sql-connector-kafka_2.11-1.12.3.jar;file:///root/PycharmProjects/Prophet_Project/lib/json.jar;file:///root/PycharmProjects/Prophet_Project/lib/flink-sql-connector-hbase-2.2_2.11-1.12.3.jar"

        # 定义table_sql语法
        self.source_ddl = f"""
            CREATE TABLE source_table(
                dat ARRAY< ROW<`start` STRING, `end` STRING, `endpoint` STRING, `nid` STRING, `counter` STRING, `dstype` STRING, `step` STRING, `values` ARRAY< ROW<`timestamp` STRING, `value` STRING>>, `comparsion` STRING>>,
                err STRING,
                record_time TIMESTAMP(3) METADATA FROM 'timestamp',
                WATERMARK FOR record_time AS record_time - INTERVAL '3' SECOND 
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{self.kafka_source_topic}',
                'properties.bootstrap.servers' = '{self.kafka_bootstrap_server}',
                'scan.startup.mode' = '{self.kafka_scan_startup_mode}',
                'format' = 'json'
            )
            """

        self.mid_ddl = """
             CREATE TABLE mid_table(
                err STRING 
                ) WITH (
                  'connector' = 'blackhole'
                )
                """

        self.online_predict_period = 120
        self.online_predict_frequency = '30s'
