"""
Wrapper around antske/coref_draft.
Input should be NAF files parsed by alpino (e.g. alpinonerc)
"""

from KafNafParserPy import KafNafParser
from multisieve_coreference import process_coreference
from io import BytesIO
import logging
from nlpipe.module import Module

log = logging.getLogger(__name__)


class CorefNL(Module):
    name = "corefnl"

    def process(self, text):
        inb = BytesIO(text.encode("utf-8"))
        naf = KafNafParser(inb)
        naf = process_coreference(naf)
        b = BytesIO()
        naf.dump(b)
        return b.getvalue().decode("utf-8")

CorefNL.register()
