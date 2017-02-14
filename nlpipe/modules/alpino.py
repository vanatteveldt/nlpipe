"""
Wrapper around the RUG Alpino Dependency parser
The module expects either ALPINO_HOME to point at the alpino installation dir
or an alpino server to be running at ALPINO_SERVER (default: localhost:5002)

You can use the following command to get the server running: (see github.com/vanatteveldt/alpinoserver)
docker run -dp 5002:5002 vanatteveldt/alpino-server

If running alpino locally, note that the module needs the dependencies end_hook, which seems to be missing in
some builds. See: http://www.let.rug.nl/vannoord/alp/Alpino
"""
import csv
import datetime
import json
import logging
import os
import subprocess
import requests

import itertools
import tempfile
from io import StringIO

from nlpipe.module import Module

log = logging.getLogger(__name__)

CMD_PARSE = ["bin/Alpino", "end_hook=dependencies", "-parse"]
CMD_TOKENIZE = ["Tokenization/tok"]


class AlpinoParser(Module):
    name = "alpino"

    def check_status(self):
        if 'ALPINO_HOME' in os.environ:
            alpino_home = os.environ['ALPINO_HOME']
            if not os.path.exists(alpino_home):
                raise Exception("Alpino not found at ALPINO_HOME={alpino_home}".format(**locals()))
        else:
            alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')
            r = requests.get(alpino_server)
            if r.status_code != 200:
                raise Exception("No server found at {alpino_server} and ALPINO_HOME not set".format(**locals()))

    def process(self, text):
        if 'ALPINO_HOME' in os.environ:
            tokens = tokenize(text)
            return parse_raw(tokens)
        else:
            alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')
            url = "{alpino_server}/parse".format(**locals())
            body = {"text": text, "output": "dependencies"}
            r = requests.post(url, json=body)
            if r.status_code != 200:
                raise Exception("Error calling Alpino at {alpino_server}: {r.status_code}:\n{r.content!r}"
                                .format(**locals()))
            return r.text

    def convert(self, id, result, format):
        assert format in ["csv"]
        s = StringIO()
        w = csv.writer(s)
        w.writerow(["doc", "id", "sentence", "offset", "word", "lemma", "pos", "rel", "parent"])
        for line in interpret_parse(result):
            w.writerow((id,) + line)
        return s.getvalue()


AlpinoParser.register()


def _call_alpino(command, input):
    alpino_home = os.environ['ALPINO_HOME']
    p = subprocess.Popen(command, shell=False, stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         cwd=alpino_home)
    out, err = [x.decode("utf-8") for x in p.communicate(input.encode("utf-8"))]
    if not out:
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="wb") as f:
            f.write(input.encode("utf-8"))
            logging.exception("Error calling Alpino, input file written to {f.name}, command was {command}"
                              .format(**locals()))
        raise Exception("Problem calling {command}, output was empty. Error: {err!r}".format(**locals()))
    return out


def tokenize(text: str) -> str:
    return _call_alpino(CMD_TOKENIZE, text).replace("|", "")


def parse_raw(tokens):
    return _call_alpino(CMD_PARSE, tokens)


def get_fields(parse):
    if parse.strip().startswith("{"):
        parse = json.loads(parse)
        for sid in parse:
            for row in parse[sid]['triples']:
                yield row + [sid]
    else:
        for line in parse.split("\n"):
            if line.strip():
                yield line.strip().split("|")


def interpret_parse(parse):
    rels = {}  # child: (rel, parent)
    for line in get_fields(parse):
        assert len(line) == 16
        sid = int(line[-1])
        func, rel = line[7].split("/")
        child = interpret_token(sid, *line[8:15])
        if func == "top":
            parent = None
        else:
            parent = interpret_token(sid, *line[:7])
        rels[child] = (rel, parent)

    # get tokenid for each token, preserving order
    tokens = sorted(rels.keys(), key=lambda token: token[:2])
    tokenids = {token: i for (i, token) in enumerate(tokens)}

    for token in tokens:
        (rel, parent) = rels[token]
        tokenid = tokenids[token]
        parentid = tokenids[parent] if parent is not None else None
        yield (tokenid, ) + token + (rel, parentid)


def interpret_token(sid, lemma, word, begin, _end, major_pos, _pos, full_pos):
    """Convert to raw alpino token into a (word, lemma, begin, pos1) tuple"""
    if major_pos not in POSMAP:
        logging.warn("UNKNOWN POS: {major_pos}".format(**locals()))
    pos1 = POSMAP.get(major_pos, '?')
    return sid, int(begin), word, lemma, pos1



POSMAP = {"pronoun": 'O', "pron": 'O',
          "verb": 'V',
          "noun": 'N',
          "preposition": 'P', "prep": 'P',
          "determiner": "D",  "det": "D",
          "comparative": "C",  "comp": "C",
          "adverb": "B",
          'adv': 'B',
          "adj": "A",
          "complementizer": "C",
          "punct": ".",
          "conj": "C",
          "vg": 'C', "prefix": 'C',  # not quite sure what vg stands for, sorry
          "tag": "?",
          "particle": "R",  "fixed": 'R',
          "name": "M",
          "part": "R",
          "intensifier": "B",
          "number": "Q", "num": 'Q',
          "cat": "Q",
          "n": "Q",
          "reflexive":  'O',
          "conjunct": 'C',
          "pp": 'P',
          'anders': '?',
          'etc': '?',
          'enumeration': '?',
          'np': 'N',
          'p': 'P',
          'quant': 'Q',
          'sg': '?',
          'zo': '?',
          'max': '?',
          'mogelijk': '?',
          'sbar': '?',
          '--': '?',
          }

