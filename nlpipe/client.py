import logging
import os

from nlpipe.clients.HTTPClient import HTTPClient
from nlpipe.clients.FSClient import FSClient


def get_client(servername, token=None):
    if servername.startswith("http:") or servername.startswith("https:"):
        logging.getLogger('requests').setLevel(logging.WARNING)
        if not token:
            token = os.environ.get('NLPIPE_TOKEN', None)
        logging.debug("Connecting to REST server at {servername} using token={}".format(bool(token), **locals()))
        return HTTPClient(servername, token=token)
    else:
        logging.debug("Connecting to local repository {servername}".format(**locals()))
        return FSClient(servername)


if __name__ == '__main__':
    import argparse
    import sys
    import nlpipe.modules

    parser = argparse.ArgumentParser()
    parser.add_argument("server", help="Server hostname or directory location")
    parser.add_argument("module", help="Module name")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true", default=False)
    parser.add_argument("--token", "-t", help="Provide auth token"
                        "(default reads ./.nlpipe_token or NLPIPE_TOKEN")

    action_parser = parser.add_subparsers(dest='action', title='Actions')
    action_parser.required = True

    actions = {name: action_parser.add_parser(name) 
               for name in ('status', 'result', 'check', 'process', 'process_inline',
                            'bulk_status', 'bulk_result', 'store_result', 'store_error')}

    for action in 'status', 'result', 'store_result', 'store_error':
        actions[action].add_argument('id', help="Task ID")
    for action in 'bulk_status', 'bulk_result':
        actions[action].add_argument('ids', nargs="+", help="Task IDs")
    for action in 'result', 'process_inline', 'bulk_result':
        actions[action].add_argument("--format", help="Optional output format to retrieve")
    for action in 'process', 'process_inline':
        actions[action].add_argument('doc', help="Document to process (use - to read from stdin")
        actions[action].add_argument('id', nargs="?", help="Optional explicit ID")
    for action in ('store_result', 'store_error'):
        actions[action].add_argument('result', help="Document to store (use - to read from stdin")
    
    args = vars(parser.parse_args())  # turn to dict so we can pop and pass the rest as kargs

    logging.basicConfig(level=logging.DEBUG if args.pop('verbose', False) else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')

    client = get_client(args.pop('server'), token=args.pop('token', None))
    
    for doc_arg in ('doc', 'result'):
        if args.get(doc_arg) == '-':
            args[doc_arg] = sys.stdin.read()

    action = args.pop('action')
    args = {k: v for (k, v) in args.items() if v}
    result = getattr(client, action)(**args)
    if action == "get_task":
        id, doc = result
        if id is not None:
            print(id, file=sys.stderr)
            print(doc)
    elif action in ("store_result", "store_error"):
        pass
    else:
        if result is not None:
            print(result)
