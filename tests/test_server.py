import json
from tempfile import TemporaryDirectory

from nlpipe.client import FSClient, get_id
from nlpipe.restserver import app
from nose.tools import assert_equal

from nlpipe.modules.test_upper import TestUpper

def test_server():
    with TemporaryDirectory() as root:
        app.client = FSClient(root)
        client = app.test_client()

        # unknown task
        x = client.head('/api/modules/test/12345')
        assert_equal(x.status_code, 404)

        # unknown module
        x = client.post('/api/modules/test-doesnotexist/', data="")
        assert_equal(x.status_code, 404)
        
        # client: add task for processing
        txt = 'this is a test'
        m = TestUpper()
        url = "/api/modules/{m.name}/".format(**locals())
        x = client.post(url, data=txt)
        id = x.headers.get('ID')
        task_url = x.headers.get('Location')
        assert_equal(id.strip(), get_id(txt))

        x = client.head(task_url)
        assert_equal(x.status_code, 202)
        assert_equal(x.headers.get('Status'), 'PENDING')

        # worker: retrieve task
        x = client.get(url)
        task_url2 = x.headers.get('Location')
        assert_equal(task_url, task_url2)
        assert_equal(x.data.decode('UTF-8'), txt)

        # client: check task status
        x = client.head(task_url)
        assert_equal(x.status_code, 202)
        assert_equal(x.headers.get('Status'), 'STARTED')


        # worker: put results
        x = client.put(task_url, data=m.process(txt))

        # client: retrieve results
        x = client.head(task_url)
        assert_equal(x.status_code, 200)
        assert_equal(x.headers.get('Status'), 'DONE')

        x = client.get(task_url)
        assert_equal(x.data.decode('UTF-8'), txt.upper())

        # client: retrieve formatted results
        x = client.get(task_url+"?format=json")
        assert_equal(json.loads(x.data.decode('UTF-8')),
                     {'result': 'THIS IS A TEST', 'status': 'OK'})
        
