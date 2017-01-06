"""
Test the Alpino module
"""
import csv
import os
import os.path
from io import StringIO
from unittest import SkipTest

from nose.tools import assert_equal
from nlpipe.modules.alpino import AlpinoParser, tokenize, parse_raw, interpret_token, interpret_parse
from tests.tools import check_status

_SENT = "Toob is dik"
_TOK_IS = 'ben|is|1|2|verb|verb(copula)|[stype=declarative]:verb(unacc,sg_heeft,copula)'
_TOK_DIK = 'dik|dik|2|3|adj|adj|[]:adjective(no_e(adv))'
_TOK_TOP = 'top|top|0|0|top|top|top'
_TOK_TOOB = 'Toob|Toob|0|1|name|name(PER)|[rnum=sg]:proper_name(sg,PER)'
_PARSE = ("{ben}|hd/predc|{dik}|1\n{top}|top/hd|{ben}|1\n{ben}|hd/su|{toob}|1"
          .format(ben=_TOK_IS, dik=_TOK_DIK, toob=_TOK_TOOB, top=_TOK_TOP))


def test_tokenize():
    check_status(AlpinoParser())
    text = u"D\xedt is een zin, met komma |nietwaar|? En nog 'n zin"
    expected = u"D\xedt is een zin , met komma nietwaar ?\nEn nog 'n zin\n"
    assert_equal(tokenize(text), expected)


def test_parse_raw():
    check_status(AlpinoParser())
    deps = parse_raw(_SENT)
    assert_equal({dep for dep in deps.split("\n") if dep},
                 {dep for dep in _PARSE.split("\n") if dep})


def test_parse():
    p = AlpinoParser()
    check_status(p)
    deps = p.process(_SENT)
    assert_equal({dep for dep in deps.split("\n") if dep},
                 {dep for dep in _PARSE.split("\n") if dep})

def test_interpret_parse():
    tokens = {token[0]: token for token in interpret_parse(_PARSE)}
    rels = {token[4]: (token[-2], tokens[token[-1]][4]) for token in tokens.values() if token[-1]}
    print(rels)
    assert_equal(rels, {'Toob': ('su', 'ben'), 'dik': ('predc', 'ben')})
    assert_equal(tokens[1], (1, 1, 1, 'is', 'ben', 'V', 'hd', None))


def test_convert():
    p = AlpinoParser()
    check_status(p)
    s = p.convert(123, p.process(_SENT), "csv")
    tokens = list(csv.DictReader(StringIO(s)))
    print(tokens)
    assert_equal(len(tokens), 3)
    assert_equal(tokens[0]['doc'], '123')

    assert_equal(tokens[0]['lemma'], 'Toob')
    assert_equal(tokens[0]['parent'], tokens[1]['id'])


def test_interpret_token():
    actual = interpret_token(1, *_TOK_TOOB.split("|"))
    expected = (1, 0, "Toob", "Toob", 'M')
    assert_equal(actual, expected)


def test_alpino_unicode():
    "Test what happens with non-ascii characters in input"
    check_status(AlpinoParser())
    text = "Bjarnfre\xf0arson leeft"
    # tokenize should convery to utf-8 and only add final line break
    assert_equal(tokenize(text), text + "\n")
