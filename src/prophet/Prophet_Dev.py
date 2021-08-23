import pandas as pd
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly
import plotly.io as pio
import redis
import pickle
import pandas as pd
import json
from prophet.serialize import model_to_json, model_from_json

r = redis.StrictRedis(host='m.cdh', password='123456', port='6379', db=0)
dat = r.lrange("39.107.26.28", 0, -1)
df = pd.DataFrame(columns=['ds', 'y'])
i = 0
for x in dat:
    js = pickle.loads(x)
    timestamp = pd.to_datetime(js['timestamp'], unit='s')
    value = js['value']
    df.loc[i] = [timestamp, value]
    i = i + 1

m = Prophet()
m.fit(df)
future = m.make_future_dataframe(freq='T', periods=60)
forecast = m.predict(future)
print(forecast.tail())

with open('/root/PycharmProjects/Prophet_Project/MachineLearningModel/serialized_model.json', 'w') as fout:
    json.dump(model_to_json(m), fout)  # Save model
