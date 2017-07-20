"""
Wrapper around antske/coref_draft.
Input should be NAF files parsed by alpino (e.g. alpinonerc)
"""

import logging
from nlpipe.module import Module
import subprocess
import os

log = logging.getLogger(__name__)


class CorefNL(Module):
    name = "corefnl"

    def check_status(self):
        if 'COREF_HOME' not in os.environ:
            raise Exception("COREF_HOME not set!")
        coref_home = os.environ['COREF_HOME']
        if not os.path.exists(coref_home):
            raise Exception("Coref not found at COREF_HOME={coref_home}".format(**locals()))
            
    def process(self, text):
        coref_home = os.environ['COREF_HOME']
        command = [os.path.join(coref_home, "env/bin/python"),
                   "-m", "multisieve_coreference.resolve_coreference"]

        p = subprocess.Popen(command, shell=False, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = [x.decode("utf-8") for x in p.communicate(text.encode("utf-8"))]
        if err:
            raise Exception(err)
        if not out:
            raise Exception("No output from coreference and no error message")

        return out
        
CorefNL.register()
