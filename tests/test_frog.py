import csv
from io import StringIO
import logging
from nose.tools import assert_equal
from unittest import SkipTest
from nlpipe.modules.frog import FrogLemmatizer
from tests.tools import check_status


def test_process():
    """
    Test Frog lemmatizing
    Make sure a frog server is listening at port 9000, e.g.:
    sudo docker run -dp 9887:9887 proycon/lamachine frog -S 9887 --skip=pm
    """
    c = FrogLemmatizer()
    check_status(c)
    result = c.process("Nederlandse woordjes")
    print(result)
    r = list(csv.DictReader(StringIO(result)))
    assert_equal(len(r), 2)
    assert_equal(r[0]["lemma"], "nederlands")
    assert_equal(r[0]["ner"], "B-LOC")

    
