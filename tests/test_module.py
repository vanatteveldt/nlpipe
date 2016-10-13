import json

from nose.tools import assert_equal, assert_raises

from nlpipe.module import Module
from nlpipe.modules.test_upper import TestUpper

def test_get_module():
    m = Module.get_module(TestUpper.name)
    assert_equal(m.__class__, TestUpper)

def test_proces():
    assert_equal(TestUpper().process("test"), "TEST")

def test_convert():
    assert_equal(json.loads(TestUpper().convert("TEXT", "json")), {'result': 'TEXT', 'status': 'OK'})
    assert_raises(Exception, TestUpper().convert, "TEXT", "unknown-format")
    
    
