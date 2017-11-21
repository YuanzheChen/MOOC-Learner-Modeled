import pickle

rds = None


def connect(redis_conn):
    global rds
    rds = redis_conn


def raw_save(key, value):
    rds.set(key, value)


def raw_load(key):
    return rds.get(key)


def save(key, obj):
    global rds
    rds.set(key, pickle.dumps(obj))
    return True


def load(key):
    global rds
    if not rds.get(key):
        return None
    return pickle.loads(rds.get(key))


def delete(key):
    global rds
    return rds.delete(key)