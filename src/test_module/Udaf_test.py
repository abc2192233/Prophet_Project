from pyflink.common.typeinfo import Types
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udtf, udf, AggregateFunction
import src.config.Config as Config
from pyflink.common import Row
from pyflink.table import ListView


def predict():
    # class WeightedAvg(AggregateFunction):
    #
    #     def create_accumulator(self):
    #         # Row(sum, train_c)
    #         return Row(0, 0)
    #
    #     def get_value(self, accumulator):
    #         return accumulator[0]
    #
    #     def accumulate(self, accumulator, value, ):
    #         accumulator[0] += value
    #
    #     def retract(self, accumulator, value):
    #         accumulator[0] += value
    #
    #     def get_result_type(self):
    #         return DataTypes.BIGINT()
    #
    #     def get_accumulator_type(self):
    #         return DataTypes.ROW([
    #             DataTypes.FIELD("f0", DataTypes.BIGINT()),
    #             DataTypes.FIELD("f1", DataTypes.BIGINT())])
    #
    #
    # class ListViewConcatAggregateFunction(AggregateFunction):
    #
    #     def get_value(self, accumulator):
    #         # the ListView is iterable
    #         return accumulator[1].join(accumulator[0])
    #
    #     def create_accumulator(self):
    #         return Row(ListView(), '')
    #
    #     def accumulate(self, accumulator, *args):
    #         accumulator[1] = args[1]
    #         # the ListView support add, clear and iterate operations.
    #         accumulator[0].add(args[0])
    #
    #     def get_accumulator_type(self):
    #         return DataTypes.ROW([
    #             # declare the first column of the accumulator as a string ListView.
    #             DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
    #             DataTypes.FIELD("f1", DataTypes.BIGINT())])
    #
    #     def get_result_type(self):
    #         return DataTypes.STRING()

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().in_streaming_mode().build()

    t_env = StreamTableEnvironment.create(environment_settings=env_settings)
    config = Config.connect_config()
    t_env.get_config().get_configuration().set_string("pipeline.jars", config.jar_path)

    # t_env.create_temporary_function("weighted_avg", WeightedAvg())
    # t_env.create_temporary_function("list_avg", ListViewConcatAggregateFunction())

    kafka_bootstrap_server = 'm.cdh:9092'
    kafka_source_topic = 'udaf_test'
    kafka_scan_startup_mode = 'earliest-offset'

    source_ddl = f"""
        CREATE TABLE source_table(
            data BIGINT,
            record_time TIMESTAMP(3) METADATA FROM 'timestamp',
            WATERMARK FOR record_time AS record_time - INTERVAL '3' SECOND 
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{kafka_source_topic}',
          'properties.bootstrap.servers' = '{kafka_bootstrap_server}',
          'scan.startup.mode' = '{kafka_scan_startup_mode}',
          'format' = 'json'
        )
        """
    t_env.execute_sql(source_ddl)
    # t_env.execute_sql("""
    #     CREATE VIEW test_view AS (
    #         WITH temp_table AS (
    #         SELECT TUMBLE_START(record_time, INTERVAL '3' SECOND ) as wStart, record_time, data
    #         FROM source_table
    #         GROUP BY TUMBLE(record_time, INTERVAL '3' SECOND ), record_time, data )
    #         SELECT * FROM temp_table
    #     )
    # """)

    # t_env.execute_sql('SELECT train_c(weighted_avg(data)) AS sum_value FROM source_table').print()
    # convert a Table to a DataStream
    # ds = table.to_append_stream(table, Types.ROW([Types.INT(), Types.STRING()]))

    # convert a Table to a DataStream
    # ds = table.to_append_stream(table, Types.ROW([Types.INT(), Types.STRING()]))


if __name__ == '__main__':
    predict()
