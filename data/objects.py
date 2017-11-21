import pandas
import sklearn
import numpy
import random
from datetime import datetime
from redis_io import io


class DataFrame(pandas.DataFrame):
    KEYS = [
        'user_id',
        'feature_week',
    ]
    def __init__(self, data=None, fields=None, frame=None):
        if frame is None:
            if not isinstance(fields, list):
                raise ValueError("fields mush be a list")
            if not all(isinstance(f, str) for f in fields):
                raise ValueError("all fields must be strings")
            if (not isinstance(data, tuple)
               or not all(isinstance(row, tuple) for row in data)):
                raise ValueError("data mush be a tuple of tuples")
            if not data:
                super(DataFrame, self).__init__([{}])
            if not all(len(row) == len(data[0]) for row in data):
                raise ValueError("data has rows of different sizes")
            if len(fields) > len(data[0]):
                raise ValueError("number of fields is larger the number of columns")
            dicts = []
            for row in data:
                dicts.append({fields[i]: row[i] for i in range(len(fields))})
            super(DataFrame, self).__init__(dicts)
        if frame is not None:
            super(DataFrame, self).__init__(frame)

    def to_msgpack(self, path_or_buf=None, encoding='utf-8', **kwargs):
        return self.to_pandas_frame().to_msgpack(path_or_buf, encoding, **kwargs)

    def to_pandas_frame(self):
        return pandas.DataFrame(data=self)

    def to_console(self):
        with pandas.option_context('display.max_rows', None, 'display.max_columns', None):
            print(self)

    def publish_dicts(self):
        df = self.to_pandas_frame()
        df['_id'] = range(len(self))
        return df.to_dict('records')

    def publish_dict(self):
        dicts = self.publish_dicts()
        return {d['_id']: {k: d[k] for k in d if k != '_id'}
                for d in dicts}

    def get_feature_fields(self):
        fields = self.columns.values.tolist()
        return [f for f in fields if f not in self.KEYS and f != 'dropout']

    def get_length(self):
        return self.shape[0]


class DataPool:
    name_prefix = 'MLM::frame_pool::'

    def __init__(self):
        self.pool = io.load(self.name_prefix) if io.load(self.name_prefix) else []

    def get_redis_key(self, _id):
        return self.name_prefix + str(hash(str(_id)))

    def save(self, frame):
        if not isinstance(frame, DataFrame):
            raise ValueError("FramePool only accept FeatureFrame")
        io.raw_save(self.get_redis_key(len(self.pool)), frame.to_msgpack(compress='zlib'))
        _id = len(self.pool)
        self.pool.append(dict(key=self.get_redis_key(_id),
                              is_splitted=False,
                              training_rows=[],
                              frac=None,
                              splitted_timestamp=None))
        io.save(self.name_prefix, self.pool)
        return _id

    def split(self, _id, frac):
        if _id < 0 or _id >= len(self.pool):
            raise ValueError("Invalid FeatureFrame id")
        frame = pandas.read_msgpack(io.raw_load(self.get_redis_key(_id)))
        self.pool[_id]['is_splitted'] = True
        self.pool[_id]['frac'] = frac
        self.pool[_id]['splitted_timestamp'] = datetime.now()
        training_rows = []
        nweek = max(frame['feature_week'].unique())
        for week in range(1, nweek+1):
            idx = frame[frame['feature_week'] == week].index.tolist()
            training_rows.extend(random.sample(idx, int(frac*len(idx))))
        self.pool[_id]['training_rows'] = training_rows
        io.save(self.name_prefix, self.pool)
        return DataFrame(frame=frame)

    def get_is_splitted(self, _id):
        if _id < 0 or _id >= len(self.pool):
            raise ValueError("Invalid FeatureFrame id")
        return self.pool[_id]['is_splitted']

    def get_frac(self, _id):
        if _id < 0 or _id >= len(self.pool):
            raise ValueError("Invalid FeatureFrame id")
        return self.pool[_id]['frac']

    def load(self, _id):
        if _id < 0 or _id >= len(self.pool):
            raise ValueError("Invalid FeatureFrame id")
        frame = pandas.read_msgpack(io.raw_load(self.get_redis_key(_id)))
        return DataFrame(frame=frame)

    def load_training(self, _id):
        if not self.get_is_splitted(_id):
            raise ValueError("FeatureFrame has not been splitted yet")
        frame = self.load(_id)
        training_rows = self.pool[_id]['training_rows']
        return frame.iloc[training_rows, :]

    def load_testing(self, _id):
        if not self.get_is_splitted(_id):
            raise ValueError("FeatureFrame has not been splitted yet")
        frame = self.load(_id)
        training_rows = self.pool[_id]['training_rows']
        testing_rows = list(set(frame.index.tolist()) - set(training_rows))
        return frame.iloc[testing_rows, :]

    def publish(self):
        dicts = []
        for _id in range(len(self.pool)):
            d = dict()
            d['_id'] = _id
            frame = self.load(_id)
            d['features'] = frame.get_feature_fields()
            d['length'] = frame.get_length()
            d['is_splitted'] = self.get_is_splitted(_id)
            d['frac'] = ("%.2f" % self.get_frac(_id)) \
                if self.get_frac(_id) else None
            d['splitted_timestamp'] = str(self.pool[_id]['splitted_timestamp']) \
                if self.pool[_id]['splitted_timestamp'] else None
            dicts.append(d)
        return dicts

    def publish_one(self, _id):
        if _id is None:
            raise ValueError("Invalid Frame id")
        frame = self.load(_id)
        features = frame.get_feature_fields()
        keys = frame.get_key_fields()
        return [{'_id': i, 'name': feature}
                for i, feature in enumerate(features)]

    def clear(self):
        io.delete(self.name_prefix)
        self.__init__()
