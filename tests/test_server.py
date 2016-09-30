from tempfile import TemporaryDirectory

from nlpipe.client import FSClient, get_id
from nlpipe.restserver import app
from nose.tools import assert_equal


def test_server():
    with TemporaryDirectory() as root:
        app.client = FSClient(root)
        client = app.test_client()

        # unknown task
        x = client.head('/api/modules/test/12345')
        assert_equal(x.status_code, 404)

        # client: add task for processing
        txt = 'THIS IS A TEST'
        x = client.post('/api/modules/test/', data=txt)
        id = x.headers.get('ID')
        url = x.headers.get('Location')
        assert_equal(id.strip(), get_id(txt))

        x = client.head(url)
        assert_equal(x.status_code, 202)
        assert_equal(x.headers.get('Status'), 'PENDING')

        # worker: retrieve task
        x = client.get('/api/modules/test/')
        url2 = x.headers.get('Location')
        assert_equal(url, url2)
        assert_equal(x.data.decode('UTF-8'), txt)

        # client: check task status
        x = client.head(url)
        assert_equal(x.status_code, 202)
        assert_equal(x.headers.get('Status'), 'STARTED')


        # worker: put results
        x = client.put(url, data=txt.lower())

        # client: retrieve results
        x = client.head(url)
        assert_equal(x.status_code, 200)
        assert_equal(x.headers.get('Status'), 'DONE')

        x = client.get(url)
        assert_equal(x.data.decode('UTF-8'), txt.lower())