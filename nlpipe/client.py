import time
import os.path
from hashlib import md5

import errno

STATUS = {"queue": "PENDING", "inprogress": "STARTED", "result": "DONE", "error": "ERROR"}


def get_id(doc):
    """
    Calculate the id (hash) of the given document
    :param doc: The document (string)
    :return: a task id (hash)
    """
    if len(doc) == 34 and doc.startswith("0x"):
        # it sure looks like a hash
        return doc
    m = md5.new()
    m.update(doc)
    return "0x" + m.hexdigest()


class Client(object):
    """Abstract class for NLPipe client bindings"""

    def process(self, module, doc):
        """Add a document to be processed by module, returning the task ID"""
        raise NotImplementedError()

    def status(self, module, doc):
        """Query whether a document has been processed, by document or by task ID
        :param module: Module name
        :param doc: A document (string) or task ID
        :return: any of 'UNKNOWN', 'PENDING', 'STARTED', 'DONE', 'ERROR'
        """
        raise NotImplementedError()

    def result(self, module, doc):
        """Get processing result, by document or task ID
        :param module: Module name
        :param doc: A document (string) or task ID
        :return: The result of processing (string)
        """
        raise NotImplementedError()

    def process_inline(self, module, doc):
        """
        Process the given document, use cached version if possible, wait and return result
        :param module: Module name
        :param doc: A document (string) or task ID
        :return: The result of processing (string)
        """
        if self.status(module, doc) == 'UNKNOWN':
            self.process(module, doc)
        while True:
            status = self.status(module, doc)
            if status == 'DONE':
                return self.result(module, doc)
            if status == 'ERROR':
                raise Exception("Error on processing {doc} with {module}".format(**locals()))
            time.sleep(0.1)

    def get_task(self, module):
        """
        Get a document to process with the given module, marking the document as 'in progress'
        :param module: Name of the module
        :return: a document to be processed (string)
        """
        raise NotImplementedError()

    def get_tasks(self, module, n):
        """
        Get multiple documents to process
        :param module: Name of the module for processing
        :param n: Number of documents to retrieve
        :return: a sequence of documents (strings)
        """
        for i in range(n):
            yield self.get_task(module)

    def store_result(self, module, doc, result):
        """
        Store the given result
        :param module: Module name
        :param doc: Document or task ID
        :param result: Result (string)
        """
        raise NotImplementedError()


class FSClient(Client):
    """
    NLPipe client that relies on direct filesystem access (e.g. on local machine or over NFS)
    """

    def __init__(self, result_dir):
        self.result_dir = result_dir

    def _check_dirs(self, module):
        for subdir in STATUS.keys():
            dirname = os.path.join(self.result_dir, module, subdir)
            try:
                os.makedirs(dirname)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

    def _write(self, module, subdir, doc, id=None):
        self._check_dirs(module)
        if id is None:
            id = get_id(doc)
        fn = os.path.join(self.result_dir, module, subdir, id)
        open(fn, 'w').write(doc)
        return fn

    def _read(self, module, subdir, doc):
        id = get_id(doc)
        fn = os.path.join(self.result_dir, module, subdir, id)
        return open(fn).read()

    def status(self, module, doc):
        id = get_id(doc)
        for subdir, status in STATUS.items():
            if os.path.exists(os.path.join(self.result_dir, module, subdir, id)):
                return STATUS
        return 'UNKNOWN'

    def process(self, module, doc):
        id = get_id(doc)
        if self.status(module, id) == 'UNKNOWN':
            self._write(module, 'queue', doc, id)

    def result(self, module, doc):
        return self._read(module, 'results', doc)
