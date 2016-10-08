import time
import sys
import subprocess
import logging

from nlpipe import client

from multiprocessing import Process
from configparser import SafeConfigParser
from pydoc import locate

class Worker(Process):
    """
    Base class for NLP workers.
    """

    sleep_timeout = 1

    def __init__(self, client, module_name):
        """
        :param client: a Client object to connect to the NLP Server
        """
        super().__init__()
        self.module_name = module_name
        self.client = client

    def run(self):
        while True:
            id, doc = self.client.get_task(self.module_name)
            if id is None:
                time.sleep(self.sleep_timeout)
                continue
            logging.info("Received task {self.module_name}/{id} ({n} bytes)".format(n=len(doc), **locals()))
            try:
                result = self.process(doc)
                self.client.store_result(self.module_name, id, result)
                logging.debug("Succesfully completed task {self.module_name}/{id} ({n} bytes)"
                              .format(n=len(result), **locals()))
            except Exception as e:
                logging.exception("Exception on parsing {self.module_name}/{id}"
                              .format(**locals()))
                self.client.store_error(self.module_name, id, str(e))

    def process(self, doc):
        """
        Process the given document.
        Subclasses should override this or set the executable instance variable.
        :param doc: a document
        :return: the processing result
        """
        raise NotImplementedError()


class SystemWorker(Worker):
    def __init__(self, client, module_name, executable, encoding="UTF-8", **options):
        super().__init__(client, module_name, **options)
        self.executable = executable
        self.encoding = encoding

    def process(self, doc):
        result = subprocess.check_output(self.executable, input=doc.encode(self.encoding), shell=True)
        return result.decode(self.encoding)

    
class FunctionWorker(Worker):
    def __init__(self, client, module_name, function, **options):
        super().__init__(client, module_name, **options)
        if isinstance(function, str):
            function = _import(function)
        self.function = function

    def process(self, doc):
        return self.function(doc)

def _import(name):
    result = locate(name)
    if result is None:
        raise ValueError("Cannot import {name!r}".format(**locals()))
    return result
    
def run_config(client, config_filename, modules=None):    
    config = SafeConfigParser()
    config.read(config_filename)

    if modules is None:
        modules = set(config) - {'DEFAULT'} # all workers

    # create and start workers
    for module in modules:
        worker_class = _import(config[module].pop('worker_class'))
        worker = worker_class(client, module, **config[module])
        logging.debug("Starting worker {module}".format(**locals()))
        worker.start()

    logging.info("Workers active and waiting for input")
    
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("server", help="Server hostname or directory location")
    parser.add_argument("config", help="Configuration file name")
    parser.add_argument("workers", nargs="*", help="Run only the specified workers")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true", default=False)

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')
    
    client = client._get_client(args.server)

    print(args)
    run_config(client, args.config, args.workers or None)
