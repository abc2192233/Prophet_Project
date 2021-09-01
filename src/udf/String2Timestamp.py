from pyflink.table import DataTypes
from pyflink.table.udf import udf
import pandas as pd


@udf(result_type=DataTypes.TIMESTAMP(3))
def str2timestamp(x):
    time_stamp = pd.to_datetime(x, utc=True, unit='s').tz_convert('Asia/Shanghai').tz_localize(None)
    return time_stamp

