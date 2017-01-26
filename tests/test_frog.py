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


def test_csv():
    """
    Test whether csv format correctly adds id and simplified POS tag
    """
    c = FrogLemmatizer()
    result = ('sentence,offset,word,lemma,morphofeat,ner,chunk\n'
              '1,0,dit,dit,"VNW(aanw,pron,stan,vol,3o,ev)",O,B-NP\n'
              '1,3,is,zijn,"WW(pv,tgw,ev)",O,B-VP\n')

    result = c.convert(123, result, "csv")
    r = list(csv.DictReader(StringIO(result)))
    assert_equal(len(r), 2)
    assert_equal(set(r[0].keys()), {"id", "sentence", "offset", "word", "lemma", "morphofeat", "ner", "chunk", "pos"})
    assert_equal(r[0]["id"], "123")
    assert_equal(r[0]["pos"], "O")
    assert_equal(r[1]["pos"], "V")

