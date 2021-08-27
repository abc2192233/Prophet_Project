import datetime

import pandas as pd
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly
import plotly.io as pio
import redis
import pickle
import pandas as pd
import json
from prophet.serialize import model_to_json, model_from_json
import time


def get_history_cpu_info(redis_c):
    ip = redis_c.keys('*')
    return ip


def deserialization_history_cpu_info(source):
    for ip in source:
        print('Loading data of ' + ip.decode('gbk'))
        cpu_source_info_list = r.lrange(ip, 0, -1)
        df = pd.DataFrame(columns=['ds', 'y'])
        i = 0
        for cpu_source_info in cpu_source_info_list:
            js = pickle.loads(cpu_source_info)
            timestamp = pd.to_datetime(js['timestamp'], utc=True, unit='s').tz_convert('Asia/Shanghai').tz_localize(
                None)
            value = js['value']
            df.loc[i] = [timestamp, value]
            i = i + 1
        predict_cpu_info(df, ip)
        del df


def predict_cpu_info(df, ip):
    print('Training ' + ip.decode('gbk'))
    m = Prophet()
    m.fit(df)
    future = m.make_future_dataframe(freq='30s', periods=28800)
    forecast = m.predict(future)
    save_cpu_info_model(m, ip)
    m.make_future_dataframe()


def save_cpu_info_model(m, ip):
    print('Saving ' + ip.decode('gbk'))
    redis_c = redis.StrictRedis(host='m.cdh', password='123456', port='6379', db=1)

    file_name = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S') + 'serialized_model.json'
    with open(f'/root/PycharmProjects/Prophet_Project/MachineLearningModel/{file_name}', 'w') as fileout:
        json.dump(model_to_json(m), fileout)  # Save model

    model = json.dumps(model_to_json(m))
    model_name = b'model_' + ip
    redis_c.set(model_name, model)


time_start = time.time()
r = redis.StrictRedis(host='m.cdh', password='123456', port='6379', db=0)
cpu_ip_list = get_history_cpu_info(r)
deserialization_history_cpu_info(cpu_ip_list)

time_end = time.time()
print('totally cost', time_end - time_start)

# ip = r.keys('*')
# print(ip[1])
#
# dat = r.lrange(ip[1], 0, -1)
# df = pd.DataFrame(columns=['ds', 'y'])
#
# i = 0
# for x in dat:
#     js = pickle.loads(x)
#     timestamp = pd.to_datetime(js['timestamp'], utc=True, unit='s').tz_convert('Asia/Shanghai').tz_localize(None)
#     value = js['value']
#     df.loc[i] = [timestamp, value]
#     i = i + 1
#     # print(timestamp, value)
#
# m = Prophet()
# m.fit(df)
# future = m.make_future_dataframe(freq='T', periods=1440)
# forecast = m.predict(future)
#
# file_name = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S') + 'serialized_model.json'
# with open(f'/root/PycharmProjects/Prophet_Project/MachineLearningModel/{file_name}', 'w') as fout:
#     json.dump(model_to_json(m), fout)  # Save model
#
# model = json.dumps(model_to_json(m))
# r.set('model', model)
#
# time_end = time.time()
# print('totally cost', time_end-time_start)
#
# date = forecast['ds'][1]
#
