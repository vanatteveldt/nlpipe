import json
import os
import requests
from nlpipe.module import Module

class ParzuClient(Module):
    name = "parzu"

    def check_status(self):
        parzu_server = os.environ.get('PARZU_SERVER', 'http://localhost:5003')
        r = requests.get(parzu_server)
        if r.status_code != 200:
            raise Exception("No server found at {parzu_server}".format(**locals()))

    def process(self, text):
        parzu_server = os.environ.get('PARZU_SERVER', 'http://localhost:5003')
        url = "{parzu_server}/parse/".format(**locals())
        data = {"text": text}
        r = requests.post(url, data=json.dumps(data))
        r.raise_for_status()
        return r.content.decode("utf-8")

    def convert(self, id, result, format):
        assert format in ["csv"]
        header = "id, word, lemma, pos, pos2, features, parent, relation, extra1, extra2\n"
        return header + result

ParzuClient.register()