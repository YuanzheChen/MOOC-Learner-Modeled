# /usr/bin/env python
import flask
# Note the blue print must be placed right after import flask
testing_api = flask.Blueprint('testing_api', __name__)


@testing_api.route("/")
def interface():

    return ''
