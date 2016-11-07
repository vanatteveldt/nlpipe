from unittest import SkipTest
import logging

def _check_status(module):
    try:
        module.check_status()
    except Exception as e:
        logging.exception("Module offline: {module}".format(**locals()))
        raise SkipTest(e)
