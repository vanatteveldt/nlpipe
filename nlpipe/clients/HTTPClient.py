import requests
from nlpipe.clients.ClientInterface import ClientInterface as Client


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

    def process(self, module, doc, id=None, **kwargs):
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
        from nlpipe.servers.utils import ERROR_MIME
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