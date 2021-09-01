from datetime import datetime

import redis
from prophet import Prophet
from pyflink.table.udf import TableFunction
import src.config.Config as Config
import pandas as pd


class TrainFunc(TableFunction):
    def eval(self, cpu_info_json):
        # clf = self.load_model()
        # future = clf.make_future_dataframe(freq='30s', periods=120)
        # forecast = clf.predict(future)
        self.cpu_info_count += 1
        for node_cpu_info in cpu_info_json:
            self.ip = node_cpu_info[2]
            # forecast = self.train_model()
            for time_value in node_cpu_info[7]:
                # yield node_cpu_info[2], time_value[0], dt
                time_stamp = pd.to_datetime(time_value[0], utc=True, unit='s').tz_convert('Asia/Shanghai').tz_localize(
                    None)
                yield node_cpu_info[2], time_stamp, time_value[1]
                # yield node_cpu_info[2], time_value[0], forecast['ds'][0].strftime('%Y-%m-%d %H:%M:%S')
                # self.add_one(s[2], b[0], b[1])
                # if b[0] == '1628247240':
                #     yield s[2], b[0], 'hi'
                # else:
                #     clf = self.load_model()
                #     future = clf.make_future_dataframe(freq='30s', periods=120)
                #     forecast = clf.predict(future)
                #     yield s[2], b[0], forecast['ds'][1].strftime('%Y-%m-%d %H:%M:%S')

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
        self.cpu_info_count = 0
        self.config = Config.connect_config()
        self.ip = None
        self.model = Prophet()

    def load_model(self):
        import redis
        import json
        from prophet.serialize import model_from_json

        redis_connect = redis.StrictRedis(host=self.config.redis_host, password=self.config.redis_password,
                                          port=self.config.redis_port, db=self.config.redis_offline_model_db)
        ip = 'model_' + self.ip
        model = model_from_json(json.loads(redis_connect.get(ip)))

        return model

    def dump_model(self):
        import redis
        import json
        from prophet.serialize import model_to_json

        redis_connect = redis.StrictRedis(host=self.config.redis_host, password=self.config.redis_password,
                                          port=self.config.redis_port, db=self.config.redis_online_model_db)

        model_name = 'online_model_' + self.ip
        model = json.dumps(model_to_json(self.model))
        redis_connect.set(model_name, model)

    def train_model(self):
        self.model = self.load_model()
        future = self.model.make_future_dataframe(freq=self.config.online_predict_frequency,
                                                  periods=self.config.online_predict_period)
        forecast = self.model.predict(future)
        self.dump_model()

        return forecast
