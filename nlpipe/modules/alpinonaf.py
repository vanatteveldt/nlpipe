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

    def _csv_header(self):
        return ["doc_id", "token_id", "para", "sentence", "offset", "token",
                "lemma", "POS", "pos1", "parent", "relation"]

    def _csv_row(self, memo, term, token):
        def _int(x):
            return None if x is None else int(x)
        def _id_int(x):
            """Convert term ids ('t_12') into integer values (12)"""
            return int(x.replace("t_", ""))
        pos = term.get_pos()
        tid = term.get_id()
        yield _id_int(tid)
        yield from (_int(x) for x in (token.get_para(), token.get_sent(), token.get_offset()))
        yield from (token.get_text(), term.get_lemma(), pos, POSMAP[pos])
        if tid in memo['deps']:
            rel, parent = memo['deps'][tid]
            yield from [_id_int(parent), rel.split("/")[-1]]
        else:
            yield from [None, None]

    def _csv_memo(self, naf):
        return dict(deps={dep.get_to(): (dep.get_function(), dep.get_from())
                          for dep in naf.get_dependencies()})

    def convert(self, id, result, format):
        assert format == "csv"
        naf = KafNafParser(BytesIO(result.encode("utf-8")))
        memo = self._csv_memo(naf)
        tokendict = {token.get_id(): token for token in naf.get_tokens()}
        s = StringIO()
        w = csv.writer(s)
        w.writerow(self._csv_header())
        for term in naf.get_terms():
            tokens = [tokendict[id] for id in term.get_span().get_span_ids()]
            for token in tokens:
                tid = term.get_id()
                pos = term.get_pos()
                pos1 = POSMAP[pos]
                row = [id] + list(self._csv_row(memo, term, token))
                w.writerow(row)
        return s.getvalue()


class AlpinoNERCParser(AlpinoClient, Module):
    name = "alpinonerc"
    modules = ["alpino", "nerc"]
AlpinoNERCParser.register()


class AlpinoCorefPipe(AlpinoClient, Module):
    name = "alpinocoref"
    modules = ["alpino", "nerc", "coref"]

    def _csv_header(self):
        return super()._csv_header() + ["ner_id", "NER", "coref_id"]

    def _csv_memo(self, naf):
        memo = super()._csv_memo(naf)
        # NER data
        memo['entities'] = {} # term_id : entity
        for ent in naf.get_entities():
            for ref in ent.get_references():
                for span in ref:
                    for target in span:
                        memo['entities'][target.get_id()] = ent
        # COREF data
        memo['coref'] = {} # term_id : coref
        for coref in naf.get_corefs():
            for span in coref.get_spans():
                memo['coref'][span.get_id_head()] = coref
        return memo

    def _csv_row(self, memo, term, token):
        row = list(super()._csv_row(memo, term, token))
        # NER data
        ent = memo['entities'].get(term.get_id())
        if ent:
            row += [ent.get_id(), ent.get_type()]
        else:
            row += [None, None]
        coref = memo['coref'].get(term.get_id())
        if coref:
            row += [coref.get_id()]
        else:
            row += [None]

        return row


AlpinoCorefPipe.register()


class AlpinoCoref(AlpinoClient, Module):
    name = "corefnl"
    modules = ["coref"]
AlpinoCoref.register()
