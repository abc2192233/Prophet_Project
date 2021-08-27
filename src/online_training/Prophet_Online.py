import json

from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
import time

time_start = time.time()

with open('/root/PycharmProjects/Prophet_Project/MachineLearningModel/20210824102322serialized_model.json', 'r') as fin:
    m = model_from_json(json.load(fin))  # Load model


print(type(m))

time_end = time.time()
print('totally cost', time_end-time_start)