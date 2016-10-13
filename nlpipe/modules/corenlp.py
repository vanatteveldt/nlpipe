"""
Wrapper around the CoreNLP server (http://nlp.stanford.edu/software/corenlp.shtml)

Assumes a CoreNLP server is listening at CORENLP_HOST (default localhost:9000)
E.g. you can run:
docker run -dp 9000:9000 chilland/corenlp-docker
"""

from nlpipe.module import Module
from urllib.parse import urlencode
import requests
import json
import os

class CoreNLPLemmatizer(Module):
    name = "corenlp_lemmatize"
    properties = {"annotators": "tokenize,ssplit,pos,lemma,ner", "outputFormat": "xml"}

    def __init__(self, server=None):
        if server is None:
            server = os.getenv('CORENLP_HOST', 'http://localhost:9000')
        self.server = server

    def status(self):
        res = requests.get(self.server)
        if "http://nlp.stanford.edu/software/corenlp.shtml" not in res.text:
            raise Exception("Unexpected answer at {self.server}".format(**locals()))
        
    def process(self, text):
        query = urlencode({"properties": json.dumps(self.properties)})
        url = "{self.server}/?{query}".format(**locals())
        res = requests.post(url, data=text)
        if res.status_code != 200:
            raise Exception("Error calling corenlp at {url}: {res.status_code}\n{res.content}".format(**locals()))
        return res.content.decode("utf-8")

CoreNLPLemmatizer.register()
