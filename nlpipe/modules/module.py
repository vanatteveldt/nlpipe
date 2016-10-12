class Module(object):
    """Abstract base class for NLPipe modules"""
    name = None
    
    def check_status(self):
        """Check the status of this module and return an error if not available (e.g. service or tool not found)"""
        raise NotImplementedError()

    def process(self, text):
        """Process the given text and return the result"""
        raise NotImplementedError()

    def convert(self, result, format):
        """Convert the given result to the given format (e.g. 'xml'), if possible or raise an exception if not"""
        raise ValueError("Module {self.name} results cannot be converted to {format}".format(**locals()))


class TestUpper(Module):
    def check_status(self):
        pass
    def process(self, text):
        return text.upper()
    def convert(self, result, format):
        if format=="json":
            result = {"status": "OK", "result": result}
        super().convert(result, format)
