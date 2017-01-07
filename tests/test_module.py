import json

from nose.tools import assert_equal, assert_raises

from nlpipe.module import Module, get_module
from nlpipe.modules.test_upper import TestUpper

def test_get_module():
    m = get_module(TestUpper.name)
    assert_equal(m.__class__, TestUpper)

def test_proces():
    assert_equal(TestUpper().process("test"), "TEST")

def test_convert():
    assert_equal(json.loads(TestUpper().convert(1, "TEXT", "json")), {'result': 'TEXT', 'status': 'OK'})
    assert_raises(Exception, TestUpper().convert, 1, "TEXT", "unknown-format")
    
    
