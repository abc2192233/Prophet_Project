import redis
from pyflink.common import WatermarkStrategy, Duration, Row
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udtf, udaf
import src.config.Config as Config
import src.udf.TrainFunc_SQL
from src.udf import udaf_test
from src.udf.SplitFunc import SplitFunc
from src.udf.TrainFunc_DS import init_data
from src.udf.TrainFunc_SQL import TrainFunc
from src.udf.String2Timestamp import str2timestamp

env_stream = StreamExecutionEnvironment.get_execution_environment()
env_table = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(stream_execution_environment=env_stream, environment_settings=env_table)
config = Config.connect_config()
t_env.get_config().get_configuration().set_string("pipeline.jars", config.jar_path)
split_func = udtf(SplitFunc(), input_types=DataTypes.ARRAY(
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
                   ])), result_types=[DataTypes.STRING(), DataTypes.TIMESTAMP(3), DataTypes.STRING()])
t_env.create_temporary_function('split_func', split_func)
t_env.execute_sql(config.source_ddl)

t_env.execute_sql("""
CREATE VIEW temp_view AS(
    WITH temp_table AS(
        SELECT ip, time_stamp, metric, record_time
        FROM source_table, LATERAL TABLE(split_func(dat))
            AS T(ip, time_stamp, metric)
        )
    -- SELECT ex1, ex2, ex3 FROM temp_table, LATERAL TABLE(train_func(dat)) AS T(ex1, ex2, ex3)
    SELECT ip, CAST(time_stamp AS STRING) AS time_stamp, metric FROM temp_table)
""")

ds = t_env.to_append_stream(t_env.from_path('temp_view'),
                            Types.ROW([Types.STRING(), Types.STRING(), Types.STRING()]))


def upload(s):
    redis_connect = redis.StrictRedis(host=config.redis_host, password=config.redis_password,
                                      port=config.redis_port, db=config.redis_history_info_db)
    redis_connect.lpush(s[0], s[1] + ':' + s[2])
    yield s[0], s[1] + ':' + s[2]


ds = ds.key_by(lambda s: s[0]).flat_map(upload)
ds.print()
env_stream.execute()
