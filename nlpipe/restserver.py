import sys
from flask import Flask, request, make_response, Response, abort
from nlpipe.client import FSClient

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
    print(id, doc)
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
    dir = sys.argv[1]
    app.client = FSClient(dir)
    app.run(debug=True)