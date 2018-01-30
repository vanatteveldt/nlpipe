import time
import sys
import subprocess
import logging
from typing import Iterable

from nlpipe import client
from nlpipe.client import Client
from nlpipe.module import get_module

from multiprocessing import Process
from configparser import SafeConfigParser
from pydoc import locate

class Worker(Process):
    """
    Base class for NLP workers.
    """

    sleep_timeout = 1

    def __init__(self, client, module, quit=False):
        """
        :param client: a Client object to connect to the NLP Server
        :param module: The module to perform work on
        :param quit: if True, quit if no jobs are found; if False, poll server every second.
        """
        super().__init__()
        self.client = client
        self.module = module
        self.quit = quit

    def run(self):
        while True:
            id, doc = self.client.get_task(self.module.name)
            if id is None:
                if self.quit:
                    logging.info("No jobs for {self.module.name}, quitting!".format(**locals()))
                    break
                time.sleep(self.sleep_timeout)
                continue
            logging.info("Received task {self.module.name}/{id} ({n} bytes)".format(n=len(doc), **locals()))
            try:
                result = self.module.process(doc)
                self.client.store_result(self.module.name, id, result)
                logging.debug("Succesfully completed task {self.module.name}/{id} ({n} bytes)"
                              .format(n=len(result), **locals()))
            except Exception as e:
                logging.exception("Exception on parsing {self.module.name}/{id}"
                              .format(**locals()))
                try:
                    self.client.store_error(self.module.name, id, str(e))
                except:
                    logging.exception("Exception on storing error for {self.module.name}/{id}"
                                      .format(**locals()))


def _import(name):
    result = locate(name)
    if result is None:
        raise ValueError("Cannot import {name!r}".format(**locals()))
    return result


def run_workers(client: Client, modules: Iterable[str], nprocesses:int=1, quit:bool=False) -> Iterable[Worker]:
    """
    Run the given workers as separate processes
    :param client: a nlpipe.client.Client object
    :param modules: names of the modules (module name or fully qualified class name)
    :param nprocesses: Number of processes per module
    :param quit: If True, workers stop when no jobs are present; if False, they poll the server every second.
    """
    # import built-in workers
    import nlpipe.modules
    # create and start workers
    result = []  # don't yield, result can be ignored silently
    for module_class in modules:
        if "." in module_class:
            module = _import(module_class)()
        else:
            module = get_module(module_class)
        for i in range(1, nprocesses+1):
            logging.debug("[{i}/{nprocesses}] Starting worker {module}".format(**locals()))
            Worker(client=client, module=module, quit=quit).start()
        result.append(module)

    logging.info("Workers active and waiting for input")
    return result
    
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("server", help="Server hostname or directory location")
    parser.add_argument("modules", nargs="+", help="Class names of module(s) to run")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true", default=False)
    parser.add_argument("--processes", "-p", help="Number of processes per worker", type=int, default=1)
    parser.add_argument("--quit", "-q", help="Quit if no jobs are available", action="store_true", default=False)
    parser.add_argument("--token", "-t", help="Provide auth token"
                        "(default reads ./.nlpipe_token or NLPIPE_TOKEN")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')
    
    client = client.get_client(args.server, token=args.token)
    run_workers(client, args.modules, nprocesses=args.processes, quit=args.quit)
