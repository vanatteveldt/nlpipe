import time

import subprocess

from multiprocessing import Process


class Worker(Process):
    """
    Base class for NLP workers.
    """

    module = None  # Module name for this worker
    executable = None  # External program to run in process() if not overridden
    encoding = 'UTF-8'
    sleep_timeout = 0.1

    def __init__(self, client):
        """
        :param client: a Client object to connect to the NLP Server
        """
        super().__init__()
        self.client = client

    def run(self):
        while True:
            id, doc = self.client.get_task(self.module)
            if id is None:
                time.sleep(self.sleep_timeout)
                continue
            result = self.process(doc)
            self.client.store_result(self.module, id, result)

    def process(self, doc):
        """
        Process the given document.
        Subclasses should override this or set the executable instance variable.
        :param doc: a document
        :return: the processing result
        """
        result = subprocess.check_output(self.executable, input=doc.encode(self.encoding), shell=True)
        return result.decode(self.encoding)


