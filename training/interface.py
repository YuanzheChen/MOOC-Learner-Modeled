# /usr/bin/env python
import flask
# Note the blue print must be placed right after import flask
training_api = flask.Blueprint('training_api', __name__)

from data.objects import DataFrame, DataPool
from classifiers.objects import ClassifierList, ClassifierPool

from training import train
from util import initialize_form_dicts, process_form_input, \
    form_dicts_to_dict

from redis_io import io

data_pool = None
classifier_pool = None

MODELS = [
    'logistic_regression',
    'svm',
    'gaussian_process',
    'decision_tree',
    'random_forest',
    'neural_network'
]

FORM_DICT = dict(
    setting=[
        dict(name='data', type='select', value=None,
             item=[], default=0, desc="Data Frame"),
        dict(name='model', type='select', value=None,
             item=MODELS, default='logistic_regression', desc="Model Type"),
    ],
    config=[
        dict(name='min_nfeature', type='select', value=None, item=range(1, 5),
             default='3', desc="Minimum Number of Feature to Support"),
        dict(name='shall_normalize', type='checkbox', value=None,
             default=1, desc="Normalize Data"),
        dict(name='shall_train_all', type='checkbox', value=None,
             default=1, desc="Train on all previous data"),
        dict(name='week_ahead', type='select', value=None, item=range(1, 3),
             default=1, desc="Number of Weeks ahead to predict"),
    ]
)


@training_api.route("/")
def training():
    global data_pool
    global classifier_pool
    data_pool = DataPool()
    classifier_pool = ClassifierPool()
    setting = initialize_form_dicts(FORM_DICT['setting'])
    config = initialize_form_dicts(FORM_DICT['config'])
    io.save('MLM::training::setting', setting)
    io.save('MLM::training::config', config)

    setting = io.load('MLM::training::setting')
    config = io.load('MLM::training::config')
    if setting is None or config is None:
        raise ValueError("Cannot load forms from redis, cache missing")

    html = flask.render_template(
        'training/training.html',
        setting=setting,
        config=config,
    )
    return html


@training_api.route("/_get_data_frames")
def get_data_frames():
    return flask.jsonify(result=data_pool.publish())


@training_api.route("/_get_setting")
def get_data_setting():
    setting = io.load('MLM::training::setting')
    return flask.jsonify(result=setting)


@training_api.route("/_set_setting")
def set_data_setting():
    setting = io.load('MLM::training::setting')
    setting_input = flask.request.args
    setting = process_form_input('setting', setting, setting_input)
    print(setting)
    io.save('MLM::training::setting', setting)
    return flask.jsonify(result=True)


@training_api.route("/_get_config")
def get_config():
    config = io.load('MLM::training::config')
    return flask.jsonify(result=config)


@training_api.route("/_set_config")
def set_config():
    config = io.load('MLM::training::config')
    config_input = flask.request.args
    config = process_form_input('config', config, config_input)
    io.save('MLM::training::config', config)
    return flask.jsonify(result=True)


@training_api.route("/_request_training")
def request_training():
    setting = io.load('MLM::training::setting')
    config = io.load('MLM::training::config')
    setting = form_dicts_to_dict(setting)
    config = form_dicts_to_dict(config)
    if setting['data']['value'] == '' or setting['model']['value'] == '':
        return flask.jsonify(result=False)
    data_frame_ids = [int(_id) for _id in str(setting['data']['value']).split(",")]
    models = str(setting['model']['value']).split(",")
    data_frames = [data_pool.load_training(_id) for _id in data_frame_ids]
    for data_frame in data_frames:
        for model in models:
            train(data_frame, model, config)
    return flask.jsonify(result=True)
