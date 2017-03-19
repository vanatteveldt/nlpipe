"""
Wrapper around the RUG Alpino Dependency parser using NAF
The module expects either ALPINO_HOME to point at the alpino installation dir
or an alpino server to be running at ALPINO_SERVER (default: localhost:5002)

You can use the following command to get the server running: (see github.com/vanatteveldt/alpinoserver)
docker run -dp 5002:5002 vanatteveldt/alpino-server

If running alpino locally, note that the module needs the dependencies end_hook, which seems to be missing in
some builds. See: http://www.let.rug.nl/vannoord/alp/Alpino
"""
import csv
import datetime
import json
import logging
import os
import subprocess
import requests

import itertools
import tempfile
from io import StringIO, BytesIO

from nlpipe.module import Module

log = logging.getLogger(__name__)


class AlpinoNERCParser(Module):
    name = "alpinonerc"

    def check_status(self):
        alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')
        r = requests.get(alpino_server)
        if r.status_code != 200:
            raise Exception("No server found at {alpino_server}".format(**locals()))

    def process(self, text):
        alpino_server = os.environ.get('ALPINO_SERVER', 'http://localhost:5002')
        url = "{alpino_server}/parse/nerc".format(**locals())
        r = requests.post(url, text)
        r.raise_for_status()
        return r.content.decode("utf-8")


AlpinoNERCParser.register()
