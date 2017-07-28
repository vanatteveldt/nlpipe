import hashlib
import json
import time
import os.path
import errno
import logging
import subprocess

import itertools
from urllib.parse import urlencode

import requests

from nlpipe.module import Module, get_module, known_modules

# Status definitions and subdir names

STATUS = {"PENDING": "queue",
          "STARTED": "inprogress",
          "DONE": "results",
          "ERROR": "errors"}


def get_id(doc):
    """
    Calculate the id (hash) of the given document
    :param doc: The document (string)
    :return: a task id (hash)
    """
    if len(doc) == 34 and doc.startswith("0x"):
        # it sure looks like a hash
        return doc
    m = hashlib.md5()
    if isinstance(doc, str):
        doc = doc.encode("utf-8")
    m.update(doc)
    return "0x" + m.hexdigest()

class Client(object):
    """Abstract class for NLPipe client bindings"""

    def process(self, module, doc, id=None, reset_error=False, reset_pending=False):
        """Add a document to be processed by module, returning the task ID
        :param module: Module name
        :param doc: A document (string)
        :param id: An optional id for the task
        :param reset_error: Re-assign documents that have status 'ERROR'
        :param reset_pending: Re-assign documents that have status 'PENDING'
        :return: task ID
        :rtype: str
        """
        raise NotImplementedError()

    def status(self, module, id):
        """Get processing status
        :param module: Module name
        :param id: Task ID
        :return: any of 'UNKNOWN', 'PENDING', 'STARTED', 'DONE', 'ERROR'
        """
        raise NotImplementedError()


    def result(self, module, id, format=None):
        """Get processing result, optionally converted to a specified format.
        If the status is ERROR, the result will be raised as an exception

        :param module: Module name
        :param id: A document (string) or task ID
        :param format: (Optional) format to convert to, e.g. 'xml', 'csv', 'json'
        :return: The result of processing (string)
        """
        raise NotImplementedError()

    def process_inline(self, module, doc, format=None, id=None):
        """
        Process the given document, use cached version if possible, wait and return result
        :param module: Module name
        :param doc: A document (string)
        :return: The result of processing (string)
        """
        if id is None:
            id = get_id(doc)
        if self.status(module, id) == 'UNKNOWN':
            self.process(module, doc, id)
        while True:
            status = self.status(module, id)
            if status in ('DONE', 'ERROR'):
                return self.result(module, id, format=format)
            time.sleep(0.1)

    def get_task(self, module):
        """
        Get a document to process with the given module, marking the document as 'in progress'
        :param module: Name of the module
        :return: a pair (id, string) for the document to be processed
        """
        raise NotImplementedError()

    def get_tasks(self, module, n):
        """
        Get multiple documents to process
        :param module: Name of the module for processing
        :param n: Number of documents to retrieve
        :return: a sequence of (id, document string) pairs
        """
        for i in range(n):
            yield self.get_task(module)

    def store_result(self, module, id, result):
        """
        Store the given result
        :param module: Module name
        :param id: Document or task ID
        :param result: Result (string)
        """
        raise NotImplementedError()

    def store_error(self, module, id, result):
        """
        Store an error
        :param module: Module name
        :param id: Document or task ID
        :param result: Result (string) describing the error
        """
        raise NotImplementedError()


    def bulk_status(self, module, ids):
        """Get processing status of multiple ids
        :param module: Module name
        :param ids: Task IDs
        :return: a dict of {id: status}
        """
        return {id: self.status(module, id) for id in ids}

    def bulk_result(self, module, ids, format=None):
        """Get results for multiple ids
        :param module: Module name
        :param ids: Task IDs
        :return: a dict of {id: result}
        """
        return {id: self.result(module, id, format=format) for id in ids}

    def bulk_process(self, module, docs, ids=None, **kargs):
        """
        Add multiple documents to the processing queue
        :param module:  Module name
        :param docs: Documents to process
        :param ids: Optional sequence of explicit IDs corresponding to the documents
        :param kargs: Additional options to pass to process
        :return: a sequence of IDs
        """
        if ids is None:
            ids = itertools.repeat(None)
        return [self.process(module, doc, id=id, **kargs) for (doc, id) in zip(docs, ids)]


class FSClient(Client):
    """
    NLPipe client that relies on direct filesystem access (e.g. on local machine or over NFS)
    """

    def __init__(self, result_dir):
        self.result_dir = result_dir
        for module in known_modules():
            self._check_dirs(module.name)

    def _check_dirs(self, module: str):
        for subdir in STATUS.values():
            dirname = os.path.join(self.result_dir, module, subdir)
            try:
                os.makedirs(dirname)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    def _write(self, module, status, id, doc):
        self._check_dirs(module)
        fn = self._filename(module, status, id)
        open(fn, 'w', encoding="UTF-8").write(doc)
        return fn

    def _read(self, module, status, id):
        fn = self._filename(module, status, id)
        return open(fn, encoding="UTF-8").read()

    def _move(self, module, id, from_status, to_status):
        fn_from = self._filename(module, from_status, id)
        fn_to = self._filename(module, to_status, id)
        os.rename(fn_from, fn_to)

    def _delete(self, module, status, id):
        fn = self._filename(module, status, id)
        os.remove(fn)

    def _filename(self, module, status, id=None):
        dirname = os.path.join(self.result_dir, module, STATUS[status])
        if id is None:
            return dirname
        else:
            return os.path.join(dirname, str(id))

    def check(self, module):
        self._check_dirs(self, module)
        return module.check_status()
        
    def status(self, module, id):
        for status in STATUS.keys():
            if os.path.exists(self._filename(module, status, id)):
                return status
        return 'UNKNOWN'

    def process(self, module, doc, id=None, reset_error=False, reset_pending=False):
        if id is None:
            id = get_id(doc)
        status = self.status(module, id)
        if status == 'UNKNOWN':
            logging.debug("Assigning doc {id} to {module}".format(**locals()))
            self._write(module, 'PENDING', id, doc)
        elif (status == "ERROR" and reset_error) or (status == "STARTED" and reset_pending):
            logging.debug("Re-assigning doc {id} with status {status} to {module}".format(**locals()))
            self._delete(module, status, id)
            self._write(module, 'PENDING', id, doc)
        else:
            logging.debug("Document {id} had status {}".format(self.status(module, id), **locals()))
        return id

    def result(self, module, id, format=None):
        status = self.status(module, id)
        if status == 'DONE':
            result = self._read(module, 'DONE', id)
            if format is not None:
                try:
                    result = get_module(module).convert(id, result, format)
                except:
                    logging.exception("Error converting document {id} to {format}".format(**locals()))
                    raise
            return result
        if status == 'ERROR':
            raise Exception(self._read(module, 'ERROR', id))
        raise ValueError("Status of {id} is {status}".format(**locals()))

    def get_task(self, module):
        path = self._filename(module, 'PENDING')
        # I can't find a way to get newest file in python without iterating over all of them
        # So this seems more robust/faster than looping over python with .getctime for every entry
        cmd = "ls -rt {path} | head -1".format(**locals())
        fn = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
        if not fn: 
            return None, None  # no files to process
        try:
            self._move(module, fn, 'PENDING', 'STARTED')
        except FileNotFoundError:
            # file was removed between choosing it and now, so try again
            return self.get_task(module)
        return fn, self._read(module, 'STARTED', fn)

    def store_result(self, module, id, result):
        status = self.status(module, id)
        if status not in ('STARTED', 'DONE', 'ERROR'):
            raise ValueError("Cannot store result for task {id} with status {status}".format(**locals()))
        self._write(module, 'DONE', id, result)
        if status in ('STARTED', 'ERROR'):
            self._delete(module, status, id)

    def store_error(self, module, id, result):
        status = self.status(module, id)
        if status not in ('STARTED', 'DONE', 'ERROR'):
            raise ValueError("Cannot store error for task {id} with status {status}".format(**locals()))
        self._write(module, 'ERROR', id, result)
        if status in ('STARTED', 'DONE'):
            self._delete(module, status, id)

    def statistics(self, module):
        """Get number of docs for each status for this module"""
        for status in STATUS:
            path = self._filename(module, status)
            cmd = "ls {path} | wc -l".format(**locals())
            n = int(subprocess.check_output(cmd, shell=True).decode("utf-8"))
            yield status, n

class HTTPClient(Client):
    """
    NLPipe client that connects to the REST server
    """

    def __init__(self, server="http://localhost:5000", token=None):
        self.server = server
        self.token = token

    def request(self, method, url, headers=None, **kwargs):
        if headers is None:
            headers = {}
        if self.token:
            headers['Authorization'] = "Token {}".format(self.token)
        return requests.request(method, url, headers=headers, **kwargs)

    def head(self, url, **kwargs):
        return self.request('head', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('post', url, **kwargs)

    def get(self, url, **kwargs):
        return self.request('get', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('put', url, **kwargs)

    def status(self, module: str, id: str) -> str:
        url = "{self.server}/api/modules/{module}/{id}".format(**locals())
        res = self.head(url)
        if res.status_code == 403:
            raise Exception("403 Forbidden, please provide a token")
        if 'Status' in res.headers:
            return res.headers['Status']
        raise Exception("Cannot determine status for {module}/{id}; return code: {res.status_code}"
                        .format(**locals()))

    def process(self, module, doc, id=None):
        url = "{self.server}/api/modules/{module}/".format(**locals())
        if id is not None:
            url = "{url}?id={id}".format(**locals())
        res = self.post(url, data=doc.encode("utf-8"))
        if res.status_code != 202:
            raise Exception("Error on processing doc with {module}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.headers['ID']

    def result(self, module, id, format=None):
        url = "{self.server}/api/modules/{module}/{id}".format(**locals())
        if format is not None:
            url = "{url}?format={format}".format(**locals())
        res = self.get(url)
        if res.status_code != 200:
            raise Exception("Error on getting result for {module}/{id}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.text

    def get_task(self, module):
        url = "{self.server}/api/modules/{module}/".format(**locals())
        res = self.get(url)

        if res.status_code == 404:
            return None, None
        elif res.status_code != 200:
            raise Exception("Error on getting a task for {module}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.headers['ID'], res.text

    def store_result(self, module, id, result):
        url = "{self.server}/api/modules/{module}/{id}".format(**locals())
        data = result.encode("utf-8")
        res = self.put(url, data=data)

        if res.status_code != 204:
            raise Exception("Error on storing result for {module}:{id}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))


    def store_error(self, module, id, result):
        url = "{self.server}/api/modules/{module}/{id}".format(**locals())
        data = result.encode("utf-8")
        from nlpipe.restserver import ERROR_MIME
        headers = {'Content-type': ERROR_MIME}
        res = self.put(url, data=data, headers=headers)
        if res.status_code != 204:
            raise Exception("Error on storing error for {module}:{id}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))

    def bulk_status(self, module, ids):
        url = "{self.server}/api/modules/{module}/bulk/status".format(**locals())
        res = self.post(url, json=ids)
        if res.status_code != 200:
            raise Exception("Error on getting bulk status for {module}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.json()

    def bulk_result(self, module, ids, format=None):
        url = "{self.server}/api/modules/{module}/bulk/result".format(**locals())
        if format is not None:
            url = "{url}?format={format}".format(**locals())
        res = self.post(url, json=ids)
        if res.status_code != 200:
            raise Exception("Error on getting bulk results for {module}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.json()

    def bulk_process(self, module, docs, ids=None, reset_error=False, reset_pending=False):
        url = ("{self.server}/api/modules/{module}/bulk/process?reset_error={reset_error}&reset_pending={reset_pending}"\
               .format(**locals()))
        body = list(docs) if ids is None else dict(zip(ids, docs))
        res = self.post(url, json=body)
        if res.status_code != 200:
            raise Exception("Error on bulk processfor {module}; return code: {res.status_code}:\n{res.text}"
                            .format(**locals()))
        return res.json()

def get_client(servername, token=None):
    if servername.startswith("http:") or servername.startswith("https:"):
        logging.getLogger('requests').setLevel(logging.WARNING)
        if not token:
            token = os.environ.get('NLPIPE_TOKEN', None)
        logging.debug("Connecting to REST server at {servername} using token={}".format(bool(token), **locals()))
        return HTTPClient(servername, token=token)
    else:
        logging.debug("Connecting to local repository {servername}".format(**locals()))
        return FSClient(servername)
        
if __name__ == '__main__':
    import argparse
    import sys
    import nlpipe.modules

    parser = argparse.ArgumentParser()
    parser.add_argument("server", help="Server hostname or directory location")
    parser.add_argument("module", help="Module name")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true", default=False)
    parser.add_argument("--token", "-t", help="Provide auth token"
                        "(default reads ./.nlpipe_token or NLPIPE_TOKEN")
    action_parser = parser.add_subparsers(dest='action', title='Actions')
    action_parser.required = True

    actions = {name: action_parser.add_parser(name) 
               for name in ('status', 'result', 'check', 'process', 'process_inline',
                            'bulk_status', 'bulk_result', 'store_result', 'store_error')}
    for action in 'status', 'result', 'store_result', 'store_error':
        actions[action].add_argument('id', help="Task ID")

    for action in 'bulk_status', 'bulk_result':
        actions[action].add_argument('ids', nargs="+", help="Task IDs")
    for action in 'result', 'process_inline', 'bulk_result':
        actions[action].add_argument("--format", help="Optional output format to retrieve")
    for action in 'process', 'process_inline':
        actions[action].add_argument('doc', help="Document to process (use - to read from stdin")
        actions[action].add_argument('id', nargs="?", help="Optional explicit ID")
    for action in ('store_result', 'store_error'):
        actions[action].add_argument('result', help="Document to store (use - to read from stdin")
    
    args = vars(parser.parse_args())  # turn to dict so we can pop and pass the rest as kargs

    logging.basicConfig(level=logging.DEBUG if args.pop('verbose', False) else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')

    client = get_client(args.pop('server'), token=args.pop('token', None))
    
    for doc_arg in ('doc', 'result'):
        if args.get(doc_arg) == '-':
            args[doc_arg] = sys.stdin.read()

    action = args.pop('action')
    args = {k: v for (k, v) in args.items() if v}
    result = getattr(client, action)(**args)
    if action == "get_task":
        id, doc = result
        if id is not None:
            print(id, file=sys.stderr)
            print(doc)
    elif action in ("store_result", "store_error"):
        pass
    else:
        if result is not None:
            print(result)
