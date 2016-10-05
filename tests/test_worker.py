from tempfile import TemporaryDirectory

import time
from nose.tools import assert_equal, assert_true, assert_false

from nlpipe.client import FSClient
from nlpipe.worker import SystemWorker, FunctionWorker


SYSUPPER = "tr '[:lower:]' '[:upper:]'"
FNUPPER = lambda x: x.upper()

def test_process():
    w = SystemWorker(None, "test", SYSUPPER)
    assert_equal(w.process("test"), "TEST")
    w = FunctionWorker(None, "test", FNUPPER)
    assert_equal(w.process("test"), "TEST")

def test_worker():
    with TemporaryDirectory() as dir:
        c = FSClient(dir)
        w = SystemWorker(c, "test", SYSUPPER)

        id = c.process(w.module_name, "test")
        assert_equal(c.status(w.module_name, id), "PENDING")

        w.start()
        time.sleep(0.2)

        assert_equal(c.status(w.module_name, id), "DONE")
        assert_equal(c.result(w.module_name, id), "TEST")

        w.terminate()
