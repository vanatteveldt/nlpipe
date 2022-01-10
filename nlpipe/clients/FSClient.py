import os.path
import errno
import logging
import subprocess

from nlpipe.module import get_module, known_modules
from nlpipe.clients.utils import STATUS, get_id
from nlpipe.clients.ClientInterface import ClientInterface as Client


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