from nose.tools import assert_in
from nlpipe.modules.corenlp import CoreNLPLemmatizer
from tests.tools import _check_status

def test_process():
    """
    Test CoreNLP processing
    Make sure a corenlp server is listening at port 9000, e.g.:
    docker run -dp 9000:9000 chilland/corenlp-docker
    """
    c = CoreNLPLemmatizer()
    _check_status(c)
    result = c.process("a test")
    assert_in("<lemma>test</lemma>", result)
    
