"""
Wrapper around the RUG Alpino Dependency parser using NAF
The module expects either ALPINO_HOME to point at the alpino installation dir
or an alpino server to be running at ALPINO_SERVER (default: localhost:5002)

You can use the following command to get the server running: (see github.com/vanatteveldt/alpinoserver)
docker run -dp 5002:5002 vanatteveldt/alpino-server

If running alpino locally, note that the module needs the dependencies end_hook, which seems to be missing in
some builds. See: http://www.let.rug.nl/vannoord/alp/Alpino
"""
import csv
import logging
import os
from io import StringIO, BytesIO

import requests
from KafNafParserPy import KafNafParser

from nlpipe.module import Module
from .alpino import POSMAP

log = logging.getLogger(__name__)


class AlpinoClient(object):
    def check_status(self):
        alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')
        r = requests.get(alpino_server)
        if r.status_code != 200:
            raise Exception("No server found at {alpino_server}".format(**locals()))

    def process(self, text):
        modules = ",".join(self.modules)
        alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')
        url = "{alpino_server}/parse/{modules}".format(**locals())
        r = requests.post(url, text.encode("utf-8"))
        r.raise_for_status()
        return r.content.decode("utf-8")

    def convert(self, id, result, format):
        assert format == "csv"
        def _int(x):
            return None if x is None else int(x)
        naf = KafNafParser(BytesIO(result.encode("utf-8")))

        deps = {dep.get_to(): (dep.get_function(), dep.get_from())
                for dep in naf.get_dependencies()}
        tokendict = {token.get_id(): token for token in naf.get_tokens()}

        s = StringIO()
        w = csv.writer(s)
        w.writerow(["id", "token_id", "offset", "sentence", "para", "word", "term_id",
                    "lemma", "pos", "pos1", "parent", "relation"])
        for term in naf.get_terms():
            tokens = [tokendict[id] for id in term.get_span().get_span_ids()]
            for token in tokens:
                tid = term.get_id()
                pos = term.get_pos()
                pos1 = POSMAP[pos]
                row = [id,  token.get_id(), _int(token.get_offset()), _int(token.get_para()), token.get_text(),
                       tid, term.get_lemma(), pos, pos1]
                if tid in deps:
                    rel, parent = deps[tid]
                    row += [parent, rel.split("/")[-1]]
                else:
                    row += [None, None]
                w.writerow(row)
        return s.getvalue()


class AlpinoNERCParser(AlpinoClient, Module):
    name = "alpinonerc"
    modules = ["alpino", "nerc"]
AlpinoNERCParser.register()


class AlpinoCorefPipe(AlpinoClient, Module):
    name = "alpinocoref"
    modules = ["alpino", "nerc", "coref"]
AlpinoCorefPipe.register()


class AlpinoCoref(AlpinoClient, Module):
    name = "corefnl"
    modules = ["coref"]
AlpinoCoref.register()
