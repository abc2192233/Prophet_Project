from pyflink.table.udf import TableFunction
import pandas as pd


class SplitFunc(TableFunction):
    def eval(self, cpu_info_json):
        for node_cpu_info in cpu_info_json:
            for time_value in node_cpu_info[7]:
                time_stamp = pd.to_datetime(time_value[0], utc=True, unit='s').tz_convert('Asia/Shanghai').tz_localize(None)
                yield node_cpu_info[2], time_stamp, time_value[1]

