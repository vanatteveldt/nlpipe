import json
import os
import sys
from flask import Flask, request, make_response, Response, abort, jsonify
from nlpipe.client import FSClient
from nlpipe.module import UnknownModuleError, get_module, known_modules
from nlpipe.worker import run_workers
import logging

app = Flask('NLPipe')

STATUS_CODES = {
    'UNKNOWN': 404,
    'PENDING': 202,
    'STARTED': 202,
    'DONE': 200,
    'ERROR': 500
}


@app.route('/api/modules/<module>/', methods=['POST'])
def post_task(module):
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
def task_status(module, id):
    status = app.client.status(module, id)
    resp = Response(status=STATUS_CODES[status])
    resp.headers['Status'] = status
    return resp


@app.route('/api/modules/<module>/<id>', methods=['GET'])
def result(module, id):
    format = request.args.get('format', None)
    try:
        result = app.client.result(module, id, format=format)
    except FileNotFoundError:
        return 'Error: Unknown document: {module}/{id}\n'.format(**locals()), 404
    return result, 200


@app.route('/api/modules/<module>/', methods=['GET'])
def get_task(module):
    id, doc = app.client.get_task(module)
    if doc is None:
        return 'Queue {module} empty!\n'.format(**locals()), 404
    resp = Response(doc, status=200)
    resp.headers['Location'] = '/api/modules/{module}/{id}'.format(**locals())
    resp.headers['ID'] = id
    return resp


@app.route('/api/modules/<module>/<id>', methods=['PUT'])
def put_results(module, id):
    doc = request.get_data().decode('UTF-8')
    app.client.store_result(module, id, doc)
    return '', 204


@app.route('/api/modules/<module>/bulk/status', methods=['POST'])
def bulk_status(module):
    try:
        ids = request.get_json(force=True)
        if not ids:
            raise ValueError("Empty request")
    except:
        return "Error: Please provive bulk IDs as a json list\nd ", 400
    statuses = {id: app.client.status(module, str(id)) for id in ids}
    return json.dumps(statuses, indent=4), 200


@app.route('/api/modules/<module>/bulk/result', methods=['POST'])
def bulk_result(module):
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
def bulk_process(module):
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
    ids = app.client.bulk_process(module, docs, ids=ids)
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
    args = parser.parse_args()
    
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
    app.run(port=port, host=host, debug=args.debug)
