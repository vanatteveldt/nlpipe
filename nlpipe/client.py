import time

#TODO: how do we want to signal failure? In the document?


class Client(object):
    """Abstract class for NLPipe client bindings"""

    def process(self, module, doc):
        """Add a document to be processed by module, returning the task ID"""
        raise NotImplementedError()

    def done(self, module, doc):
        """Query whether a document has been processed, by document or by task ID
        :param module: Module name
        :param doc: A document (string) or task ID
        :return: Boolean indicating whether processing is done
        """
        raise NotImplementedError()

    def result(self, module, doc):
        """Get processing result, by document or task ID
        :param module: Module name
        :param doc: A document (string) or task ID
        :return: The result of processing (string)
        """
        raise NotImplementedError()

    def process_inline(self, module, doc):
        """
        Process the given document, use cached version if possible, wait and return result
        :param module: Module name
        :param doc: A document (string) or task ID
        :return: The result of processing (string)
        """
        if not self.done(module, doc):
            self.process(module, doc)
        while not self.done(module, doc):
            time.sleep(0.1)
        return self.result(module, doc)

    def get_task(self, module):
        """
        Get a document to process with the given module, marking the document as 'in progress'
        :param module: Name of the module
        :return: a document to be processed (string)
        """
        raise NotImplementedError()

    def get_tasks(self, module, n):
        """
        Get multiple documents to process
        :param module: Name of the module for processing
        :param n: Number of documents to retrieve
        :return: a sequence of documents (strings)
        """
        for i in range(n):
            yield self.get_task(module)

    def store_result(self, module, doc, result):
        """
        Store the given result
        :param module: Module name
        :param doc: Document or task ID
        :param result: Result (string)
        """
        raise NotImplementedError()

class FSClient(Client):
    """
    NLPipe client that relies on direct filesystem access (e.g. on local machine or over NFS)
    """

    def __init__(self, result_dir):
        self.result_dir = result_dir

    def process(self, module, doc):
        self._check_dirs(module)






