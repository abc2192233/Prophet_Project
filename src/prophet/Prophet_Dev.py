import pandas as pd
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly
import plotly.io as pio
import redis
import pickle
import pandas as pd

r = redis.StrictRedis(host='m.cdh', password='123456', port='6379', db=0)
dat = r.lrange("39.107.26.28", 0, -1)

for x in dat:
    js = pickle.loads(x)
    timestamp = pd.to_datetime(js['timestamp'], unit='s')
    value = js['value']
    print(timestamp, value)



# df = pd.read_csv('/root/jupyter/example_wp_peyton_manning.csv')
#
# m = Prophet()
# m.fit(df)
#
# future = m.make_future_dataframe(freq='D', periods=24)
#
# forecast = m.predict(future)
