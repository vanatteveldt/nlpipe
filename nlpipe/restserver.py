import datetime
import json
import os
import socket
import sys
from functools import wraps
import logging

import jwt
from flask import Flask, request, make_response, Response, abort, jsonify
from flask.templating import render_template
from flask_autodoc.autodoc import Autodoc

from nlpipe.client import FSClient
from nlpipe.module import UnknownModuleError, get_module, known_modules
from nlpipe.worker import run_workers

class LoginFailed(Exception):
    pass

def do_check_auth():
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        raise LoginFailed("No authentication supplied\n")
    if not auth_header.startswith("Token "):
        raise LoginFailed("Incorrectly formatted authorization header\n")
    token = auth_header[len("Token "):]
    try:
        jwt.decode(token, _secret_key())
    except jwt.DecodeError as e:
        raise LoginFailed("Invalid token") from e


# whill throw exception if not valid

def check_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if app.use_auth:
            try:
                do_check_auth()
            except LoginFailed as e:
                return "Login Failed: {e}\n".format(**locals()), 403
        return f(*args, **kwargs)
    return decorated_function

app = Flask('NLPipe', template_folder=os.path.dirname(__file__))


auto = Autodoc(app)

STATUS_CODES = {
    'UNKNOWN': 404,
    'PENDING': 202,
    'STARTED': 202,
    'DONE': 200,
    'ERROR': 500
}
ERROR_MIME = 'application/prs.error+text'

SECRET_KEY = None


def _secret_key():
    global SECRET_KEY
    if SECRET_KEY is None:
        hostid = os.popen("hostid").read().strip()
        hostname = socket.gethostname()
        SECRET_KEY = "__{hostid}_{hostname}"
    return SECRET_KEY


def get_token():
    payload = {
        'version': 1,
        'iat': datetime.datetime.utcnow(),

    }
    return jwt.encode(payload, _secret_key(), algorithm='HS256')


@app.route('/checktoken', methods=['HEAD', 'GET'])
@check_auth
def checktoken(token):
    return "Authentication {}\n".format("OK" if app.use_auth else "disabled"), 200


@app.route('/')
def index():
    fsdir = app.client.result_dir
    mods = sorted(known_modules(), key=lambda mod: mod.name)
    mods = {mod: dict(app.client.statistics(mod.name)) for mod in mods}
    return render_template('index.html', **locals())


@app.route('/apidoc')
def doc():
    return auto.html()


@app.route('/api/modules/<module>/', methods=['POST'])
@check_auth
@auto.doc()
def post_task(module):
    """
    POST a new task to the NLPipe server.
    Post body should contain the test to process.
    You can specify an explicit document id with ?id=<id>
    Response will be an empty HTTP 202 response with Location and ID headers

    :param module: The name of the module to process with
    """
    try:
        get_module(module)  # check if module exists
    except UnknownModuleError as e:
        return str(e), 404
    doc = request.get_data().decode('UTF-8')
    id = request.args.get("id")
    id = app.client.process(module, doc, id=id)
    resp = Response(id+"\n", status=202)
    resp.headers['Location'] = '/api/modules/{module}/{id}'.format(**locals())
    resp.headers['ID'] = id
    return resp


@app.route('/api/modules/<module>/<id>', methods=['HEAD'])
@check_auth
@auto.doc()
def task_status(module, id):
    """
    HEAD gets the status of a task as HTTP Status code.
    Response will also contain a status header.

    :param module: The module name
    :param id: ID of the task to get status for
    """
    status = app.client.status(module, id)
    resp = Response(status=STATUS_CODES[status])
    resp.headers['Status'] = status
    return resp


@app.route('/api/modules/<module>/<id>', methods=['GET'])
@check_auth
@auto.doc()
def result(module, id):
    """
    GET the processed result of a task.
    If processed OK, returns the result as document with HTTP 200
    If processing failed, returns HTTP 500 with a json document containing the exception
    If task is unknown or not yet processed, will return 404

    :param module: The module name
    :param id: ID of the task to get result for
    """
    format = request.args.get('format', None)
    try:
        result = app.client.result(module, id, format=format)
    except FileNotFoundError:
        return 'Error: Unknown document: {module}/{id}\n'.format(**locals()), 404
    except Exception as e:
        result = {"exception_class": type(e).__name__, "message": str(e)}
        return make_response(jsonify(result), 500)
    return result, 200


@app.route('/api/modules/<module>/', methods=['GET'])
@check_auth
@auto.doc()
def get_task(module):
    """
    GET a task to process.
    This is intended to be called by a worker and will set status of the task to STARTED.
    Returns the text to process with HTTP headers ID and Location

    :param module: Module name
    """
    id, doc = app.client.get_task(module)
    if doc is None:
        return 'Queue {module} empty!\n'.format(**locals()), 404
    resp = Response(doc, status=200)
    resp.headers['Location'] = '/api/modules/{module}/{id}'.format(**locals())
    resp.headers['ID'] = id
    return resp


@app.route('/api/modules/<module>/<id>', methods=['PUT'])
@check_auth
@auto.doc()
def put_results(module, id):
    """
    PUT the results of processing.
    If processing failed, use Content-type: prs.error+text and put the error message or diagnostics
    This is intended to be callsed by a worker and will set the status of the task to DONE or ERROR.

    :param module:
    :param id:
    :return:
    """
    doc = request.get_data().decode('UTF-8')
    if request.content_type == ERROR_MIME:
        app.client.store_error(module, id, doc)
    else:
        app.client.store_result(module, id, doc)
    return '', 204


@app.route('/api/modules/<module>/bulk/status', methods=['POST'])
@check_auth
@auto.doc()
def bulk_status(module):
    """
    Bulk method: POST a json list of IDs to get status information from.
    Returns a json dict of {id: status}

    :param module: The module name
    """
    try:
        ids = request.get_json(force=True)
        if not ids:
            raise ValueError("Empty request")
    except:
        return "Error: Please provive bulk IDs as a json list\nd ", 400
    statuses = {id: app.client.status(module, str(id)) for id in ids}
    return json.dumps(statuses, indent=4), 200


@app.route('/api/modules/<module>/bulk/result', methods=['POST'])
@check_auth
@auto.doc()
def bulk_result(module):
    """
    Bulk method: POST a json list of IDs to get results for.
    Returns a json dict of {id: result}

    :param module: The module name
    """
    try:
        ids = request.get_json(force=True)
        if not ids:
            raise ValueError("Empty request")
    except:
        return "Error: Please provive bulk IDs as a json list\nd ", 400
    format = request.args.get('format', None)
    results = app.client.bulk_result(module, ids, format=format)
    return jsonify(results)


@app.route('/api/modules/<module>/bulk/process', methods=['POST'])
@check_auth
@auto.doc()
def bulk_process(module):
    """
    Bulk method: POST a json list or {id: text} dict containing texts to process
    Returns a json list of ids

    :param module: The module name
    """
    reset_error = request.args.get('reset_error', False) in ('1', 'Y', 'True')
    reset_pending = request.args.get('reset_pending', False) in ('1', 'Y', 'True')
    try:
        docs = request.get_json(force=True)
        if not docs:
            raise ValueError("Empty request")
    except:
        logging.exception("bulk/process: Error parsing json {}".format(repr(request.data)[:20]))
        return "Error: Please provive bulk docs as a json list or {id:doc, } dict\n ", 400
    if isinstance(docs, list):
        docs, ids = docs, None
    else:
        docs, ids = docs.values(), docs.keys()
    ids = app.client.bulk_process(module, docs, ids=ids, reset_error=reset_error, reset_pending=reset_pending)
    return jsonify(ids)


if __name__ == '__main__':
    import argparse
    import tempfile

    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?",
                        help="Location of NLPipe storage directory (default: $NLPIPE_DIR or tempdir)")
    parser.add_argument("--workers", "-w", nargs="*", help="Run specified or all known worker modules")
    parser.add_argument("--port", "-p", type=int, default=5001,
                        help="Port number to listen to (default: $NLPIPE_PORT or 5001)")
    parser.add_argument("--host", "-H", help="Host address to listen on (default: $NLPIPE_HOST or localhost)")
    parser.add_argument("--debug", "-d", help="Set debug mode (implies -v)", action="store_true")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true")
    parser.add_argument("--disable-authentication", "-A", help="Disable authentication. Only use on firewalled servers",
                        action="store_true")
    parser.add_argument("--print-token", "-T", help="Print authentication token and exit", action="store_true")
    args = parser.parse_args()

    if args.print_token:
        print("Authentication token:\n{}".format(get_token().decode("ascii")))
        sys.exit()

    logging.basicConfig(level=logging.DEBUG if (args.debug or args.verbose) else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')

    host = args.host or os.environ.get("NLPIPE_HOST", "localhost")
    port = args.port or os.environ.get("NLPIPE_PORT", 5001)

    if not args.directory:
        if "NLPIPE_DIR" in os.environ:
            args.directory = os.environ["NLPIPE_DIR"]
        else:
            tempdir = tempfile.TemporaryDirectory(prefix="nlpipe_")
            args.directory = tempdir.name
    app.client = FSClient(args.directory)

    if args.workers is not None:
        module_names = args.workers or [m.name for m in known_modules()]
        logging.debug("Starting workers: {module_names}".format(**locals()))
        run_workers(app.client, module_names)

    logging.debug("Serving from {args.directory}".format(**locals()))
    app.use_auth = not args.disable_authentication
    if not app.use_auth:
        logging.warning("** Authentication disabled! **")

    app.run(port=port, host=host, debug=args.debug)
else:
    # configure server from defaults / environment
    if "NLPIPE_DIR" in os.environ:
        nlpipe_dir = os.environ["NLPIPE_DIR"]
        app.client = FSClient(nlpipe_dir)
        app.use_auth = True
        
    
    
