import json
import os
import sys
import logging

from nlpipe.clients.FSClient import FSClient
from nlpipe.module import known_modules
from nlpipe.worker import run_workers
from nlpipe.servers.utils import get_token
from flask import Flask
from flask_cors import CORS

from nlpipe.servers.RESTServer import app_restServer

app = Flask('NLPipe', template_folder=os.path.dirname(__file__))
CORS(app)
app.register_blueprint(app_restServer)

if __name__ == '__main__':
    import argparse
    import tempfile

    # arguments for restserver
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?",
                        help="Location of NLPipe storage directory (default: $NLPIPE_DIR or tempdir)")
    parser.add_argument("--workers", "-w", nargs="*", help="Run specified or all known worker modules")
    parser.add_argument("--port", "-p", type=int, default=5001,
                        help="Port number to listen to (default: $NLPIPE_PORT or 5001)")
    parser.add_argument("--host", "-H", help="Host address to listen on (default: $NLPIPE_HOST or localhost)")
    parser.add_argument("--debug", "-d", help="Set debug mode (implies -v)", action="store_true")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true")
    parser.add_argument("--disable-authentication", "-A", help="Disable authentication. Only use on firewalled servers",
                        action="store_true")
    parser.add_argument("--print-token", "-T", help="Print authentication token and exit", action="store_true")
    args = parser.parse_args()  # read the arguments from the commandline

    if args.print_token:
        print("Authentication token:\n{}".format(get_token().decode("ascii")))
        sys.exit()

    logging.basicConfig(level=logging.DEBUG if (args.debug or args.verbose) else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')

    host = args.host or os.environ.get("NLPIPE_HOST", "localhost")
    port = args.port or os.environ.get("NLPIPE_PORT", 5001)

    if not args.directory:
        if "NLPIPE_DIR" in os.environ:
            args.directory = os.environ["NLPIPE_DIR"]
        else:
            tempdir = tempfile.TemporaryDirectory(prefix="nlpipe_")
            args.directory = tempdir.name
    app.client = FSClient(args.directory)  # add client to the Flask application
    app_restServer.client = FSClient(args.directory)  # add client to the Flask application

    if args.workers is not None:
        module_names = args.workers or [m.name for m in known_modules()]
        logging.debug("Starting workers: {module_names}".format(**locals()))
        run_workers(app.client, module_names)  # run the workers

    logging.debug("Serving from {args.directory}".format(**locals()))
    app.use_auth = not args.disable_authentication
    # not sure if the line below is correct
    app_restServer.use_auth = not args.disable_authentication
    if not app.use_auth:
        logging.warning("** Authentication disabled! **")

    app.run(port=port, host=host, debug=args.debug)  # run the Flask app
else:
    # configure server from defaults / environment
    if "NLPIPE_DIR" in os.environ:
        nlpipe_dir = os.environ["NLPIPE_DIR"]
        app.client = FSClient(nlpipe_dir)
        app.use_auth = True
        
    
    
