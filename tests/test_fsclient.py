from tempfile import TemporaryDirectory
import time
import os.path
from nose.tools import assert_equal, assert_true, assert_false

from nlpipe.client import FSClient, get_id


# noinspection PyProtectedMember
def test_pipeline():
    with TemporaryDirectory() as dir:
        c = FSClient(dir)
        m = "testmodule"
        txt1, txt2 = "This is a test", "This is another test"

        # Add a task to the queue
        id1 = c.process(m, txt1)
        assert_equal(c.status(m, id1), "PENDING")
        assert_equal(id1, get_id(txt1))
        assert_equal(c._read(m, "PENDING", id1), txt1)
        assert_true(os.path.exists(c._filename(m, 'PENDING', id1)))
        assert_false(os.path.exists(c._filename(m, 'STARTED', id1)))

        # Add another task
        time.sleep(0.01) # force small time difference between files
        id2 = c.process(m, txt2)

        # test getting tasks from the queue
        assert_equal(c.get_task(m), (id1, txt1))  # fifo
        assert_equal(c.status(m, id1), "STARTED")
        assert_false(os.path.exists(c._filename(m, 'PENDING', id1)))
        assert_true(os.path.exists(c._filename(m, 'STARTED', id1)))
        assert_equal(c.get_task(m), (id2, txt2))  # fifo
        assert_equal(c.get_task(m), (None, None))  # done!

        # Process a task
        c.store_result(m, id1, txt1.upper())
        assert_equal(c.status(m, id1), "DONE")
        assert_false(os.path.exists(c._filename(m, 'STARTED', id1)))
        assert_true(os.path.exists(c._filename(m, 'DONE', id1)))

        # Retrieve results
        assert_equal(c.result(m, id1), txt1.upper())
