from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udtf
import src.config.Config as Config
import src.udf.TrainFunc
from src.udf.TrainFunc import TrainFunc
from src.udf.String2Timestamp import str2timestamp


def predict_job():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)
    config = Config.connect_config()
    t_env.get_config().get_configuration().set_string("pipeline.jars", config.jar_path)

    train_func = udtf(TrainFunc(), input_types=DataTypes.ARRAY(
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

    t_env.create_temporary_function('train_func', train_func)
    t_env.create_temporary_function('str2timestamp', str2timestamp)

    t_env.execute_sql(config.source_ddl)
    t_env.execute_sql(config.mid_ddl)

    t_env.execute_sql('SELECT * FROM source_table').print()


