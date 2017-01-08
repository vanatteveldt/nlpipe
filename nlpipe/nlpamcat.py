import argparse

from amcatclient import amcatclient
from nlpipe.client import get_client
import logging

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("amcatserver", help="AmCAT Server hostname")
    parser.add_argument("project", help="AmCAT Project ID")
    parser.add_argument("articleset", help="AmCAT Article Set ID")
    parser.add_argument("nlpipeserver", help="NLPipe Server hostname or directory location")
    parser.add_argument("module", help="Module name")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    print(dir(args))
    # connect to AmCAT and NLPipe
    logging.debug("Getting article IDS from {args.amcatserver} set {args.project}:{args.articleset}".format(**locals()))
    a = amcatclient.AmcatAPI(args.amcatserver)
    ids = [x['id'] for x in a.get_articles(args.project, args.articleset, columns=['id'])]
    logging.debug("Got {} IDS".format(len(ids)))

    c = get_client(args.nlpipeserver)
    status = {id: c.status(args.module, str(id)) for id in ids}
    todo = [id for (id, status) in status.items() if status == "UNKNOWN"]
    logging.info("Assigning {} articles from {args.amcatserver} set {args.project}:{args.articleset}"
                 .format(len(todo), **locals()))

    for art in a.get_articles_by_id(articles=todo, columns='headline,text', page_size=100):
        text = "{headline}\n\n{text}".format(**art)
        c.process(args.module, text, id=str(art['id']))

    logging.info("Done! Assigned {} articles".format(len(todo)))
