from tempfile import TemporaryDirectory

import time
from nose.tools import assert_equal, assert_true, assert_false

from nlpipe.client import FSClient
from nlpipe.worker import Worker
from nlpipe.modules.test_upper import TestUpper

SYSUPPER = "tr '[:lower:]' '[:upper:]'"
FNUPPER = lambda x: x.upper()

def test_worker():
    with TemporaryDirectory() as dir:
        c = FSClient(dir)
        m = TestUpper()
        w = Worker(c, m)

        id = c.process(m.name, "test")
        assert_equal(c.status(m.name, id), "PENDING")

        w.start()
        time.sleep(0.2)

        assert_equal(c.status(m.name, id), "DONE")
        assert_equal(c.result(m.name, id), "TEST")

        w.terminate()
