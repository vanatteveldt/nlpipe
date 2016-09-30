from tempfile import TemporaryDirectory

import time
from nose.tools import assert_equal, assert_true, assert_false

from nlpipe.client import FSClient
from nlpipe.worker import Worker


class TestWorker(Worker):
    module = "Test"
    executable = "tr '[:lower:]' '[:upper:]'"


def test_process():
    w = TestWorker(client=None)
    assert_equal(w.process("test"), "TEST")

def test_worker():
    with TemporaryDirectory() as dir:
        c = FSClient(dir)
        w = TestWorker(c)

        id = c.process(w.module, "test")
        assert_equal(c.status(w.module, id), "PENDING")

        w.start()
        time.sleep(0.2)

        assert_equal(c.status(w.module, id), "DONE")
        assert_equal(c.result(w.module, id), "TEST")

        w.terminate()