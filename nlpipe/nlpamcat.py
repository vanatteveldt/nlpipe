import argparse
import json
from collections import Counter
import re
from io import BytesIO

from amcatclient import amcatclient
from nlpipe.client import get_client
import logging
from KafNafParserPy import KafNafParser, CfileDesc, Cpublic, CHeader


def normalize(txt):
    pars = []
    for par in txt.split("\n\n"):
        par = par.replace("\n", " ")
        par = re.sub("\s+", " ", par)
        par = par.strip()
        pars.append(par)
    return "\n\n".join(pars)

def get_text(a, to_naf=False, lang='nl'):
    result = "\n\n".join([normalize(a[x]) for x in ('headline', 'text')])
    if to_naf:
        naf = KafNafParser(type="NAF")
        naf.header = CHeader(type=naf.type)
        naf.root.insert(0, naf.header.get_node())

        naf.set_language(lang)
        naf.set_raw(result)
        naf.set_version("3.0")
        
        fd = CfileDesc()
        if 'author' in a:
            fd.set_author(a['author'])
        if 'headline' in a:
            fd.set_title(a['headline'])
        if 'date' in a:
            fd.set_creationtime(a['date'])
        if 'medium' in a:
            fd.set_magazine(a['medium'])
        if 'page' in a:
            fd.set_pages(str(a['page']))
        if 'section' in a:
            fd.set_section(a['section'])
        naf.header.set_fileDesc(fd)

        pub = Cpublic()
        if 'url' in a:
            pub.set_uri(a['url'])
        if 'uuid' in a:
            pub.set_publicid(a['uuid'])
        naf.header.set_publicId(pub)
        b = BytesIO()
        naf.dump(b)
        result = b.getvalue().decode("utf-8")
    return result





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("amcatserver", help="AmCAT Server hostname")
    parser.add_argument("project", help="AmCAT Project ID", type=int)
    parser.add_argument("articleset", help="AmCAT Article Set ID", type=int)
    parser.add_argument("nlpipeserver", help="NLPipe Server hostname or directory location")
    parser.add_argument("module", help="Module name")
    parser.add_argument("action", help="NLPipe action", choices=["process", "status", "result"])
    parser.add_argument("--naf", help="Use NAF input format (for action=process)", action="store_true")
    parser.add_argument("--format", "-f", help="Result format")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true")
    parser.add_argument("--reset-error", "-e", help="Reset errored documents (for action=process)", action="store_true")
    parser.add_argument("--reset-pending", "-p", help="Reset pending documents (for action=process)", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)

    logging.debug("Will {args.action} at nlpipe {args.nlpipeserver} all articles "
                  "from {args.amcatserver} set {args.articleset}".format(**locals()))

    # connect to AmCAT and NLPipe
    logging.debug("Getting article IDS from {args.amcatserver} set {args.project}:{args.articleset}".format(**locals()))
    a = amcatclient.AmcatAPI(args.amcatserver)
    ids = [str(x['id']) for x in a.get_articles(args.project, args.articleset, columns=['id'])]

    # Get status of articles
    logging.debug("Getting of {} IDS".format(len(ids)))
    c = get_client(args.nlpipeserver)
    status = c.bulk_status(args.module, ids)

    if args.action == "process":
        todo = [id for (id, status) in status.items() if status == "UNKNOWN"]
        if args.reset_error:
            todo += [id for (id, status) in status.items() if status == "ERROR"]
        if args.reset_pending:
            todo += [id for (id, status) in status.items() if status == "PENDING"]

        if todo:
            logging.info("Assigning {} articles from {args.amcatserver} set {args.project}:{args.articleset}"
                         .format(len(todo), **locals()))
            columns = 'headline,text,creator,date,url,uuid,medium,section,page' if args.naf else 'headline,text'
            for arts in a.get_articles_by_id(articles=todo, columns=columns, page_size=100, yield_pages=True):
                ids = [a['id'] for a in arts]
                texts = [get_text(a, to_naf=args.naf) for a in arts]

                logging.debug("Assigning {} articles".format(len(ids)))
                c.bulk_process(args.module, texts, ids=ids, reset_error=args.reset_error,
                               reset_pending=args.reset_pending)

        logging.info("Done! Assigned {} articles".format(len(todo)))
    if args.action == "status":
        for k, v in Counter(status.values()).items():
            print("{k}: {v}".format(**locals()))
    if args.action == 'result':
        toget = [id for (id, status) in status.items() if status == "DONE"]
        kargs = {'format': args.format} if args.format else {}
        results = c.bulk_result(args.module, toget, **kargs)
        if args.format == "csv":
            for i, csv_bytes in enumerate(results.values()):
                if i != 0 and "\n" in csv_bytes:
                    csv_bytes = csv_bytes.split("\n", 1)[1]
                print(csv_bytes.strip())
        else:
            print(json.dumps(results, indent=4))

