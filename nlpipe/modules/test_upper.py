"""
Trivial test module that converts to upper case
"""

from nlpipe.module import Module

class TestUpper(Module):
    name= "test_upper"
    def check_status(self):
        pass
    def process(self, text):
        return text.upper()
    def convert(self, result, format):
        if format=="json":
            result = {"status": "OK", "result": result}
        super().convert(result, format)
