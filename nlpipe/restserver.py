import sys
from flask import Flask, request, make_response, Response, abort
from nlpipe.client import FSClient
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
    doc = request.get_data().decode('UTF-8')
    id = app.client.process(module, doc)
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
    try:
        result = app.client.result(module, id)
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

if __name__ == '__main__':
    import argparse
    import tempfile
    import shutil
    
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?", help="Location of NLPipe storage directory (default: tempdir)")
    parser.add_argument("--port", "-p", type=int, help="Port number to listen to (default: 5000)")
    parser.add_argument("--host", "-H", help="Host address to listen on (default: localhost)")
    parser.add_argument("--debug", "-d", help="Set debug mode (implies -v)", action="store_true")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG if (args.debug or args.verbose) else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')
                        
    kargs = {"debug": args.debug}
    if args.host: kargs['host'] = args.host
    if args.port: kargs['port'] = args.port

    if not args.directory:
        tempdir = tempfile.TemporaryDirectory(prefix="nlpipe_")
        args.directory = tempdir.name

    app.client = FSClient(args.directory)
    logging.debug("Serving from {args.directory}".format(**locals()))
    app.run(**kargs)
