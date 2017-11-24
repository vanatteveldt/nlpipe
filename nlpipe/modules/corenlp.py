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
from io import StringIO
import csv
import logging

from corenlp_xml.document import Document

class CoreNLPBase(Module):


    def __init__(self, server=None):
        if server is None:
            server = os.getenv('CORENLP_HOST', 'http://localhost:9000')
        self.server = server

    def check_status(self):
        res = requests.get(self.server)
        if "http://nlp.stanford.edu/software/corenlp.shtml" not in res.text:
            raise Exception("Unexpected answer at {self.server}".format(**locals()))

    def process(self, text):
        query = urlencode({"properties": json.dumps(self.properties)})
        url = "{self.server}/?{query}".format(**locals())
        res = requests.post(url, data=text.encode("utf-8"))
        if res.status_code != 200:
            raise Exception("Error calling corenlp at {url}: {res.status_code}\n{res.content}".format(**locals()))
        return res.content.decode("utf-8")

class CoreNLPParser(CoreNLPBase):
    name = "corenlp_parse"
    properties = {"annotators": "tokenize,ssplit,pos,lemma,ner,parse,dcoref", "outputFormat": "xml"}

    def convert(self, id, result, format):
        assert format in ["csv"]
        try:
            doc = Document(result.encode("utf-8"))
        except:
            logging.exception("Error on parsing xml")
            raise

        s = StringIO()
        w = csv.writer(s)
        w.writerow(["doc_id", "sentence", "token_id", "offset", "token", "lemma", "POS", "pos1", "NER",
                    "relation", "parent"])

        parents = {}  # sentence, child.id : (rel, parent.id)
        for sent in doc.sentences:
            if sent.collapsed_ccprocessed_dependencies:
                for dep in sent.collapsed_ccprocessed_dependencies.links:
                    if dep.type != 'root':
                        parents[sent.id, dep.dependent.idx] = (dep.type, dep.governor.idx)

        for sent in doc.sentences:
            for t in sent.tokens:
                rel, parent = parents.get((sent.id, t.id), (None, None))
                w.writerow([id, sent.id, t.id, t.character_offset_begin, t.word, t.lemma,
                            t.pos, POSMAP[t.pos], t.ner, rel, parent])

        return s.getvalue()


class CoreNLPLemmatizer(CoreNLPBase):
    name = "corenlp_lemmatize"
    properties = {"annotators": "tokenize,ssplit,pos,lemma,ner", "outputFormat": "xml"}

    def convert(self, id, result, format):
        assert format in ["csv"]
        try:
            doc = Document(result.encode("utf-8"))
        except:
            logging.exception("Error on parsing xml")
            raise

        s = StringIO()
        w = csv.writer(s)
        w.writerow(["id", "sentence", "offset", "word", "lemma", "POS", "pos1", "ner"])
        
        for sent in doc.sentences:
            for t in sent.tokens:
                w.writerow([id, sent.id, t.character_offset_begin, t.word, t.lemma,
                            t.pos, POSMAP[t.pos], t.ner])
        return s.getvalue()
    
CoreNLPLemmatizer.register()
CoreNLPParser.register()


POSMAP = { # Penn treebank POS -> simple POS
    # P preposition
    'IN': 'P',
    # G adjective
    'JJ': 'G',
    'JJR': 'G',
    'JJS': 'G',
    'WRB': 'G',
    # C conjunction
    'LS': 'C',
    # V verb
    'MD': 'V',
    'VB': 'V',
    'VBD': 'V',
    'VBG': 'V',
    'VBN': 'V',
    'VBP': 'V',
    'VBZ': 'V',
    # N noun
    'NN': 'N',
    'NNS': 'N',
    'FW': 'N',
    # R proper noun 
    'NNP': 'R',
    'NNPS': 'R',
    # D determiner
    'PDT': 'D',
    'DT': 'D',
    'WDT': 'D',
    # A adverb
    'RB': 'A',
    'RBR': 'A',
    'RBS': 'A',
    # O other
    'CC': 'O',
    'CD': 'O',
    'POS': 'O',
    'PRP': 'O',
    'PRP$': 'O',
    'EX': 'O',
    'RP': 'O',
    'SYM': 'O',
    'TO': 'O',
    'UH': 'O',
    'WP': 'O',
    'WP$': 'O',
    ',': 'O',
    '.': 'O',
    ':': 'O',
    '``': 'O',
    '$': 'O',
    "''": 'O',
    "#": 'O',
    '-LRB-': 'O',
    '-RRB-': 'O',
}
