from src.config.Config import connect_config


def load_model(ip):
    import redis
    import json
    from prophet.serialize import model_from_json

    self = connect_config()
    redis_connect = redis.StrictRedis(host=self.redis_host, password=self.redis_password,
                                      port=self.redis_port, db=self.redis_train_model_db)

    ip = 'model_' + ip
    model = model_from_json(json.loads(redis_connect.get(ip)))

    return model


print(load_model('10.140.0.10'))
