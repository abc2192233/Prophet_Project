from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.common.typeinfo import Types
from pyflink.table.udf import udf, udtf, TableFunction
from datetime import datetime


@udf(input_types=DataTypes.ARRAY(
    DataTypes.ROW([DataTypes.FIELD('start', DataTypes.STRING()),
                   DataTypes.FIELD('end', DataTypes.BIGINT()),
                   DataTypes.FIELD('endpoint', DataTypes.STRING()),
                   DataTypes.FIELD('nid', DataTypes.STRING()),
                   DataTypes.FIELD('counter', DataTypes.STRING()),
                   DataTypes.FIELD('dstype', DataTypes.STRING()),
                   DataTypes.FIELD('step', DataTypes.STRING()),
                   DataTypes.FIELD('values', DataTypes.ARRAY(
                       DataTypes.ROW([
                           DataTypes.FIELD('timestamp', DataTypes.BIGINT()),
                           DataTypes.FIELD('value', DataTypes.DOUBLE())
                       ]))),
                   DataTypes.FIELD('comparsion', DataTypes.BIGINT())
                   ])), result_type=DataTypes.INT())
def get_node_num(i):
    j = 0
    for x in i:
        j = j + 1
    return j


class Split(TableFunction):
    def eval(self, x):
        for s in x:
            for b in s[7]:
                self.add_one(s[2], b[0], b[1])
                yield s[2], b[0], b[1]

    def add_one(self, ip, timestamp, value):
        import redis
        import pickle
        r = redis.StrictRedis(**self.redis_params)

        key_value = {
            'timestamp': timestamp,
            'value': value
        }
        r.rpush(ip, pickle.dumps(key_value))

    def __init__(self):
        self.model_name = 'online_ml_model'
        self.redis_params = dict(host='m.cdh', password='123456', port='6379', db=0)
        self.clf = self.load_model()

        self.interval_dump_seconds = 180
        self.last_dump_time = datetime.now()
        self.classes = list(range(10))

        self.metric_counter = None
        self.metric_predict_accuracy = 0
        self.metric_distribution_y = None
        self.metric_total_1_day = None
        self.metric_right_1_day = None

    def open(self, function_context):
        metric_group = function_context.get_metric_group().add_group('online_ml')
        self.metric_counter = metric_group.counter('sample_count')
        metric_group.gauge('prediction_accuracy', lambda: int(self.metric_predict_accuracy * 100))
        self.metric_distribution_y = metric_group.distribution("metric_distribution_y")
        self.metric_total_1_day = metric_group.meter('total_1_day', time_span_in_seconds=86400)
        self.metric_right_1_day = metric_group.meter('right_1_day', time_span_in_seconds=86400)

    def load_model(self):
        import redis
        import pickle
        import logging

        r = redis.StrictRedis(**self.redis_params)
        clf = None

        try:
            clf = pickle.load(r.get(self.model_name))
        except TypeError:
            logging.info('Redis 内没有指定名称的模型，因此初始化一个新模型')
        except (redis.exceptions.RedisError, TypeError, Exception):
            logging.warning('Redis 出现异常，因此初始化一个新模型')
        finally:
            clf = clf

        return clf

    def dump_model(self):
        import pickle
        import redis
        import logging

        if (datetime.now() - self.last_dump_time).seconds >= self.interval_dump_seconds:
            r = redis.StrictRedis(**self.redis_params)
            try:
                r.set(self.model_name, pickle.dump(self.clf, protocol=pickle.HIGHEST_PROTOCOL))
            except (redis.exceptions.RedisError, TypeError, Exception):
                logging.warning('无法连接 Redis 以存储模型数据')
            self.last_dump_time = datetime.now()


def log_processing():
    kafka_bootstrap_server = 'm.cdh:9092'
    kafka_source_topic = 'cpu_info'
    kafka_scan_startup_mode = 'earliest-offset'
    # 'earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' and 'specific-offsets'

    # env = StreamExecutionEnvironment.get_execution_environment()
    # env_settings = EnvironmentSettings.Builder().use_blink_planner().build()
    # t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)

    # specify connector and format jars
    t_env.get_config().get_configuration().set_string("pipeline.jars",
                                                      "file:///root/PycharmProjects/Prophet_Project/lib/flink-sql-connector-kafka_2.11-1.12.3.jar;file:///root/PycharmProjects/Prophet_Project/lib/json.jar")

    source_ddl = f"""
            CREATE TABLE source_table(
                dat ARRAY< ROW<`start` STRING, `end` STRING, `endpoint` STRING, `nid` STRING, `counter` STRING, `dstype` STRING, `step` STRING, `values` ARRAY< ROW<`timestamp` STRING, `value` STRING>>, `comparsion` STRING>>,
                err VARCHAR 
            ) WITH (
              'connector' = 'kafka',
              'topic' = '{kafka_source_topic}',
              'properties.bootstrap.servers' = '{kafka_bootstrap_server}',
              'scan.startup.mode' = '{kafka_scan_startup_mode}',
              'format' = 'json'
            )
            """

    mid_ddl = f"""
            CREATE TABLE mid_table(
                dat ROW<`start` BIGINT, `end` BIGINT, `endpoint` STRING, `nid` STRING, `counter` STRING, `dstype` STRING, `step` INT, `values` ARRAY<ROW<`timestamp` BIGINT, `value` DOUBLE>>, `comparsion` BIGINT>,
                err VARCHAR 
            ) WITH (
              'connector' = 'print'
            )
            """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(mid_ddl)

    # t_env.execute_sql('SELECT get_node_num(node_num) AS y FROM (SELECT node_num FROM (SELECT CARDINALITY (dat) AS node_num FROM source_table))').print()
    split = udtf(Split(), input_types=DataTypes.ARRAY(
        DataTypes.ROW([DataTypes.FIELD('start', DataTypes.STRING()),
                       DataTypes.FIELD('end', DataTypes.STRING()),
                       DataTypes.FIELD('endpoint', DataTypes.STRING()),
                       DataTypes.FIELD('nid', DataTypes.STRING()),
                       DataTypes.FIELD('counter', DataTypes.STRING()),
                       DataTypes.FIELD('dstype', DataTypes.STRING()),
                       DataTypes.FIELD('step', DataTypes.STRING()),
                       DataTypes.FIELD('values', DataTypes.ARRAY(
                           DataTypes.ROW([
                               DataTypes.FIELD('timestamp', DataTypes.STRING()),
                               DataTypes.FIELD('value', DataTypes.STRING())
                           ]))),
                       DataTypes.FIELD('comparsion', DataTypes.STRING())
                       ])), result_types=[DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()])

    t_env.create_temporary_function('split', split)
    t_env.create_temporary_function('get_node_num', get_node_num)

    t_env.execute_sql('DESCRIBE source_table')

    t_env.execute_sql(
        'SELECT ip, time_stamp, cpu_value FROM source_table, LATERAL TABLE(split(dat)) as T(ip, time_stamp, cpu_value)').print()
    # t_env.execute_sql('SELECT get_node_num(dat) as num FROM source_table').print()


if __name__ == '__main__':
    log_processing()
