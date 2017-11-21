# /usr/bin/env python
import flask
# Note the blue print must be placed right after import flask
classifier_api = flask.Blueprint('classifier_api', __name__)

from objects import ClassifierList, ClassifierPool

data_pool = None


@classifier_api.route("/querier")
def querier():
    global data_pool
    data_pool = ClassifierPool()
    html = flask.render_template(
        'classifiers/querier.html',
    )
    return html


@classifier_api.route("/_query_frame_pool")
def query_frame_poll():
    return flask.jsonify(result=data_pool.publish())


@classifier_api.route("/_clear_all_frames")
def query_clear_all_frames():
    global data_pool
    data_pool.clear()
    return flask.jsonify(result=True)


@classifier_api.route("/transferring_by_week")
def transferring_by_week():
    html = flask.render_template(
        'classifiers/transferring_by_week.html',
    )
    return html