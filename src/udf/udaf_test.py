from pyflink.table import DataTypes
from pyflink.table.udf import udaf


@udaf(result_type=DataTypes.FLOAT(), func_type="pandas")
def count_test(v):
    # i = 1
    # for x in v:
    #     i += 1
    return 1
