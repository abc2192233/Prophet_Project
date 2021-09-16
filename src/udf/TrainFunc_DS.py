import json
import pickle

import pandas as pd
import redis
from prophet import Prophet

from prophet.serialize import model_from_json, model_to_json

from src.config import Config


def stan_init(m):
    """Retrieve parameters from a trained model.

    Retrieve parameters from a trained model in the format
    used to initialize a new Stan model.

    Parameters
    ----------
    m: A trained model of the Prophet class.

    Returns
    -------
    A Dictionary containing retrieved parameters of m.

    """
    res = {}
    for pname in ['k', 'm', 'sigma_obs']:
        res[pname] = m.params[pname][0][0]
    for pname in ['delta', 'beta']:
        res[pname] = m.params[pname][0]
    return res


class train_c:
    def __init__(self):
        self.config = Config.connect_config()
        self.cpu_info_count = 0
        self.window = None
        self.ip_dict = dict()
        self.data_frame = None
        self.current_ip = None
        self.preliminary_model = None
        self.finished_model = None
        self.forecast = None

    def train(self, window, ip, time_stamp, metric):
        if self.is_window_change(window):
            self.creat_and_train_dataframe()
            # print(self.data_frame)
        self.add_ip_dict(ip, time_stamp, metric)

    def is_window_change(self, window):
        if self.window is None:
            self.window = window
            return False
        elif self.window != window and (
                (pd.to_datetime(window) - pd.to_datetime(self.window)) / pd.Timedelta(seconds=3)) >= 5:
            self.window = window
            return True
        elif self.window != window and (
                (pd.to_datetime(window) - pd.to_datetime(self.window)) / pd.Timedelta(seconds=3)) < 5:
            return False
        elif self.window == window:
            return False

    def add_ip_dict(self, ip, time_stamp, metric):
        if ip not in self.ip_dict:
            self.ip_dict.update({ip: dict()})
        self.ip_dict[ip].update({time_stamp: metric})

    def creat_and_train_dataframe(self):
        for ip in self.ip_dict:
            self.data_frame = pd.DataFrame(columns=['ds', 'y'])
            i = 0
            for x in range(len(self.ip_dict[ip])):
                self.data_frame.loc[i] = self.ip_dict[ip].popitem()
                i += 1
            self.current_ip = ip
            self.load_model()
            self.train_model()
            self.dump_model()
            self.predict()
            self.push_predict_data()

    def push_predict_data(self):
        redis_connect = redis.StrictRedis(host=self.config.redis_host, password=self.config.redis_password,
                                          port=self.config.redis_port, db=3)
        for x in range(self.forecast.shape[0]):
            redis_connect.hset(self.current_ip, key=str(self.forecast.iloc[x]['ds']),
                               value=pickle.dumps(self.forecast.iloc[x].to_json()))

    def predict(self):
        future = self.preliminary_model.make_future_dataframe(freq=self.config.online_predict_frequency,
                                                              periods=self.config.online_predict_period,
                                                              include_history=False)
        self.forecast = self.preliminary_model.predict(future)

    def train_model(self):
        self.finished_model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True).fit(
            self.data_frame, init=stan_init(self.preliminary_model))
        # print(json.dumps(model_to_json(self.finished_model)))
        # print(stan_init(Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True, n_changepoints=0).fit(self.data_frame)))

    def load_model(self):
        redis_connect = redis.StrictRedis(host=self.config.redis_host, password=self.config.redis_password,
                                          port=self.config.redis_port, db=self.config.redis_offline_model_db)
        model_name = 'model_' + self.current_ip
        self.preliminary_model = model_from_json(json.loads(redis_connect.get(model_name)))

    def dump_model(self):
        redis_connect = redis.StrictRedis(host=self.config.redis_host, password=self.config.redis_password,
                                          port=self.config.redis_port, db=self.config.redis_online_model_db)

        model_name = 'online_model_' + self.current_ip
        model = json.dumps(model_to_json(self.preliminary_model))
        redis_connect.set(model_name, model)


c = train_c()


def init_data(s):
    c.train(s[0], s[1], s[2], s[3])
    yield c.current_ip, s[0], s[1], s[2], s[3]
    # yield s[0], s[1], s[2], s[3]  # 0:window_start 1:ip 2:time_stamp 3:metric


def test_func(s):
    c.train(s[0], s[1], s[2], s[3])
    yield c.current_ip, s[0], s[1], s[2], s[3]


# if __name__ == '__main__':
#     # for x in range(10, 60):
#     #     test_func(['2021-09-09 11:29:30.000', '172.30.29.185', f'2021-09-10 14:{x}:00.000', x/100])
#     test_func(['2021-09-09 11:30:30.000', '172.30.29.185', '2021-09-17 15:49:40.000', '1'])
#     test_func(['2021-09-09 11:30:30.000', '172.30.29.185', '2021-09-17 15:49:45.000', '1'])
#     test_func(['2021-09-09 11:30:30.000', '172.30.29.185', '2021-08-17 10:49:50.000', '0.9'])
#     test_func(['2021-09-09 11:30:45.000', '172.30.29.185', '2021-08-17 10:50:40.000', '0.8'])
#     # test_func(['2021-09-09 11:32:30.000', '172.30.29.185', '2021-08-17 10:50:45.000', '1.0'])
