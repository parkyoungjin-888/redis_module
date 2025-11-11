import time
import redis
from typing import Type
from pydantic import BaseModel


def decode_dict(byte_dict):
    decoded = {}
    for key, value in byte_dict.items():
        decoded_key = key.decode('utf-8')
        if isinstance(value, bytes):
            decoded_value = value.decode('utf-8')
        elif isinstance(value, dict):
            decoded_value = decode_dict(value)
        else:
            decoded_value = value
        decoded[decoded_key] = decoded_value
    return decoded


class RedisStreamControl:
    def __init__(self, host: str, port: int, db: int, stream: str, ttl: int,
                 data_model: Type[BaseModel] = None, consumer_group: str = None, consumer_name: str = None):
        self._redis = redis.Redis(host=host, port=port, db=db)
        self._stream = stream
        self._ttl = ttl
        self._data_model = data_model
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name

    def _create_consumer_group(self):
        try:
            self._redis.xgroup_create(self._stream, self.consumer_group, id='0', mkstream=True)
        except redis.exceptions.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                pass
            else:
                raise e

    def put_data(self, data_list: list[dict]):
        batch = [self._data_model(**data).model_dump() for data in data_list]

        pipe = self._redis.pipeline()
        for data in batch:
            pipe.xadd(self._stream, data)
        data_ids = pipe.execute()
        if self._ttl:
            current_time = time.time()
            ttl_data = {data_id: current_time + self._ttl for data_id in data_ids}
            pipe.zadd(f'{self._stream}_ttl', ttl_data)
        pipe.execute()
        return data_ids

    def get_data(self, count: int = 1):
        if self.consumer_group is None or self.consumer_name is None:
            raise Exception('consumer is not set, get is not work')

        self._create_consumer_group()

        messages = self._redis.xreadgroup(self.consumer_group, self.consumer_name, {self._stream: '>'}, count)
        if messages:
            _, data_list = messages[0]
            batch = [decode_dict(data) for data_id, data in data_list]
            return batch
        return None

    def remove_expired_data(self):
        now = time.time()
        expired_ids = self._redis.zrangebyscore(f'{self._stream}_ttl', '-inf', now)

        if expired_ids:
            self._redis.xdel(self._stream, *expired_ids)
            self._redis.zremrangebyscore(f'{self._stream}_ttl', '-inf', now)
        return len(expired_ids)


# if __name__ == '__main__':
#     from datetime import datetime
#     import cv2
#
#     redis_stream_control = RedisStreamControl('127.0.0.1', 6379, 0, 'test_stream', 15,
#                                               'group_1', 'consumer_1')
#
#     test_raw_data_list = []
#     for i in range(5):
#         value = i * 100.0
#         test_data = {'io_id': 'io_1', 'timestamp': datetime.now().timestamp(), 'value': value}
#         test_raw_data_list.append(test_data)
#         print(i)
#         time.sleep(1)
#     redis_stream_control.put_raw_data(batch=test_raw_data_list)
#
#     raw_data_1 = redis_stream_control.get_raw_data(count=2)
#     raw_data_2 = redis_stream_control.get_raw_data(count=2)
#
#     # while True:
#     #     redis_stream_control.remove_expired_data()
#     #     time.sleep(0.1)
#
#     # img_data = redis_stream_control.get_img_data(count=2)
#
#     print('end')
