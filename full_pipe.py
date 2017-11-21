"""

"""
# /usr/bin/env python
from __future__ import print_function

import sys
import flask
import redis
import json

from testing.interface import testing_api
from training.interface import training_api
from data.interface import frames_api
from classifiers.interface import classifier_api
from config import config
from redis_io import io
from data.objects import DataPool, DataFrame
from classifiers.objects import ClassifierPool, ClassifierList

app = flask.Flask(__name__)
# Entering Flask debug mode
# app.config['DEBUG'] = True

app.register_blueprint(testing_api, url_prefix='/testing')
app.register_blueprint(training_api, url_prefix='/training')
app.register_blueprint(frames_api, url_prefix='/frames')
app.register_blueprint(classifier_api, url_prefix='/classifiers')

# open redis connection
rds = redis.StrictRedis(host='localhost', port=6379, db=1)


@app.route('/')
def index():
    html = flask.render_template(
        'index.html',
    )
    return html


if __name__ == "__main__":
    rds.flushall()
    io.connect(rds)

    if len(sys.argv) < 2:
        cfg = config.ConfigParser()
    else:
        cfg = config.ConfigParser(sys.argv[1])
    if not cfg.is_valid():
        sys.exit("Config file is invalid.")
    cfg_mysql = cfg.get_or_query_mysql()
    io.save('MLM::config::cfg_mysql', cfg_mysql)

    data_pool = DataPool()

    app.run(host='0.0.0.0')
