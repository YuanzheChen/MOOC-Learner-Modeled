# /usr/bin/env python
import flask
# Note the blue print must be placed right after import flask
frames_api = flask.Blueprint('frames_api', __name__)

from data.objects import DataPool
from redis_io import io
from connector import Connector

data_pool = None
connector = None
feature_id_frame = None


@frames_api.route("/querier")
def querier():
    global data_pool
    data_pool = DataPool()
    html = flask.render_template(
        'data/querier.html',
    )
    return html


@frames_api.route("/_query_frame_pool")
def query_frame_poll():
    return flask.jsonify(result=data_pool.publish())


@frames_api.route("/_clear_all_frames")
def query_clear_all_frames():
    global data_pool
    data_pool.clear()
    return flask.jsonify(result=True)


@frames_api.route("/fetching")
def fetching():
    global connector
    cfg_mysql = io.load('MLM::config::cfg_mysql')
    if not cfg_mysql:
        return "No MySQL config dict provided"
    connector = Connector(cfg_mysql)
    connector.open_conn()
    html = flask.render_template(
        'data/fetching.html',
    )
    return html


@frames_api.route("/_get_features")
def get_features():
    io.save('selected_feature_table', 'user_long_feature')
    global feature_id_frame
    feature_id_frame = connector.get_feature_ids('user_long_feature')
    return flask.jsonify(result=feature_id_frame.publish_dicts())


@frames_api.route("/_set_selected_features")
def set_selected_features():
    selected_feature_id_ids = str(flask.request.args.get('selected_feature_id_ids', None)).split(',')
    selected_feature_id_ids = [int(selected_id) for selected_id in selected_feature_id_ids]
    selected_feature_ids = [feature_id_frame.publish_dict()[selected_id]['feature_id']
                            for selected_id in selected_feature_id_ids]
    selected_feature_names = [feature_id_frame.publish_dict()[selected_id]['feature_name']
                              for selected_id in selected_feature_id_ids]
    io.save('selected_feature_ids', selected_feature_ids)
    io.save('selected_feature_names', selected_feature_names)
    selected_feature_table = str(io.load('selected_feature_table'))
    return flask.jsonify(result=connector.save_feature_frame(selected_feature_table,
                                                             selected_feature_ids,
                                                             selected_feature_names))


@frames_api.route("/splitting")
def splitting():
    global data_pool
    data_pool = DataPool()
    html = flask.render_template(
        'data/splitting.html',
    )
    return html


@frames_api.route("/_request_splitting")
def request_splitting():
    selected_frame_id = int(flask.request.args.get('selected_frame_id', None))
    frac = float(flask.request.args.get('frac', None))
    global data_pool
    data_pool.split(selected_frame_id, frac)
    return flask.jsonify(result=True)