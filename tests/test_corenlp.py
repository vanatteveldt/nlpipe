from nose.tools import assert_in, assert_equal
from nlpipe.modules.corenlp import CoreNLPLemmatizer
from tests.tools import _check_status
from io import StringIO
import csv

def test_process():
    """
    Test CoreNLP processing
    Make sure a corenlp server is listening at port 9000, e.g.:
    docker run -dp 9000:9000 chilland/corenlp-docker
    """
    c = CoreNLPLemmatizer()
    _check_status(c)
    result = c.process("two words")
    assert_in("<lemma>word</lemma>", result)
    
    tokens = list(csv.DictReader(StringIO(c.convert(result, format="csv"))))
    assert_equal(len(tokens), 2)
    assert_equal(tokens[1]['lemma'], "word")
    
