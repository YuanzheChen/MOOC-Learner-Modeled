import pickle
from datetime import datetime
from redis_io import io


class ClassifierList:
    def __init__(self, classifiers, weeks, trained_on=set(), tested_on=list(), course=None):
        if not isinstance(classifiers, list):
            raise ValueError("Invalid list of classifiers")
        if not len(classifiers) == len(weeks):
            raise ValueError("Length of classifier list not equal to the length of week lists")
        if not all(isinstance(w, tuple) and len(w) == 2
                   and isinstance(w[0], int) and isinstance(w[1], int) for w in weeks):
            raise ValueError("Invalid list of weeks")
        if not isinstance(trained_on, set) and all(isinstance(feature, str) for feature in trained_on):
            raise ValueError("Invalid list of trained on features")
        if not isinstance(tested_on, list) and all(isinstance(record, dict) for record in tested_on):
            raise ValueError("Invalid list of tested on records")
        self.classifiers = classifiers
        self.weeks = weeks
        self.trained_on = trained_on
        self.tested_on = tested_on
        self.created_time = datetime.now()

    def get_classifiers(self):
        return self.classifiers

    def get_weeks(self):
        return self.weeks

    def get_trained_on(self):
        return self.trained_on

    def get_is_tested(self):
        return len(self.tested_on) > 0

    def get_created_timestamp(self):
        return self.created_time

    def to_pickle(self):
        return pickle.dumps(self)

    def publish_trained_on(self):
        return ", ".join(self.trained_on)

    def publish_weeks(self):
        return ", ".join([str(data_week)+"-->"+str(target_week)
                          for data_week, target_week in self.weeks])

    def publish(self):
        d={}
        return d


class ClassifierPool:
    name_prefix = 'MLM::classifier_pool::'

    def __init__(self):
        self.pool = io.load(self.name_prefix) if io.load(self.name_prefix) else []

    def get_redis_key(self, _id):
        return self.name_prefix + str(hash(str(_id)))

    def save(self, frame):
        if not isinstance(frame, ClassifierList):
            raise ValueError("ClassifierPool only accept ClassifierList")
        io.raw_save(self.get_redis_key(len(self.pool)), frame.to_pickle())
        _id = len(self.pool)
        self.pool.append(self.get_redis_key(len(self.pool)))
        io.save(self.name_prefix, self.pool)
        return _id

    def load(self, _id):
        frame = pickle.loads(io.raw_load(self.get_redis_key(_id)))
        return frame

    def size(self):
        return len(self.pool)

    def publish(self):
        dicts = []
        for _id in range(len(self.pool)):
            d = dict()
            d['_id'] = _id
            frame = self.load(_id)
            d['weeks'] = frame.publish_weeks()
            d['trained_on'] = frame.publish_trained_on()
            d['is_tested'] = frame.get_is_tested()
            d['created_timestamp'] = str(frame.get_created_timestamp())
            dicts.append(d)
        return dicts

    def clear(self):
        io.delete(self.name_prefix)
        self.__init__()