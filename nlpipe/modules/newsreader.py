"""
Wrapper around the RUG Alpino Dependency parser
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
from io import StringIO

from nlpipe.module import Module

log = logging.getLogger(__name__)

class Newsreader(Module):
    name = "newsreader"

    def check_status(self):
        newsreader_server = os.environ.get('NEWSREADER_SERVER', 'http://localhost:5002')
        r = requests.get(newsreader_server)
        if r.status_code != 200:
            raise Exception("No newsreader server found at {newsreader_server}".format(**locals()))

    def process(self, text):
        newsreader_server = os.environ.get('NEWSREADER_SERVER', 'http://localhost:5002')
        url = "{newsreader_server}/newsreader".format(**locals())
        body = {"text": text}
        r = requests.post(url, json=body)
        if r.status_code != 200:
            raise Exception("Error calling Newsreader at {newsreader_server}: {r.status_code}:\n{r.content!r}"
                            .format(**locals()))
        return r.text


Newsreader.register()