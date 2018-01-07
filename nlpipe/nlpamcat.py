import argparse
import json
import os
from collections import Counter
import re
from io import BytesIO
from typing import Union, Iterable, Mapping
import itertools

from amcatclient import AmcatAPI
from nlpipe.client import get_client, Client
import logging
from KafNafParserPy import KafNafParser, CfileDesc, Cpublic, CHeader



def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks."""
    if n < 1:
        raise ValueError("Size of {} invalid for grouper() / splitlist().".format(n))
    return itertools.zip_longest(fillvalue=fillvalue, *([iter(iterable)] * n))


def splitlist(iterable, itemsperbatch=100):
    """Split a list into smaller lists. Uses no fillvalue, as opposed to grouper()."""
    _fillvalue = object()
    for group in grouper(iterable, itemsperbatch, _fillvalue):
        yield [e for e in group if e is not _fillvalue]


def get_ids(amcat_server: AmcatAPI, project: int, articleset: int) -> Iterable[int]:
    """
    Get the article ids for this articleset

    :param amcat_server: Amcat server
    :param project: AmCAT project ID
    :param articleset: AmCAT Articleset ID
    :return: sequence of AmCAT article IDs
    """
    return (x['id'] for x in
            amcat_server.get_articles(project, articleset, columns=['id']))


def get_status(amcat_server: AmcatAPI, project: int, articleset: int,
               nlpipe_server: Client, module: str) -> Mapping[int, str]:
    """
    Get the status for each article in this article set

    :param amcat_server: Amcat server (url str or AmCATAPI object)
    :param project: AmCAT project ID (int)
    :param articleset: AmCAT Articleset ID (int)
    :param nlpipe_server: NLPipe server (url/dirname str or nlpipe.Client object)
    :param module: NLPipe module name (str)
    :return: a dict of {id: status} (int: str)
    """
    ids = list(get_ids(amcat_server, project, articleset))
    return {int(id): status
            for (id, status) in nlpipe_server.bulk_status(module, ids).items()}


def process_pipe(amcat_server: AmcatAPI, project: int, articleset: int,
                 nlpipe_server: Client, module: str, previous_module: str) -> None:
    
    status = get_status(amcat_server, project, articleset, nlpipe_server, module)

    todo = {id for (id, status) in status.items() if status in "UNKNOWN"}

    if not todo:
        logging.info("All {} documents to process with {module} are done".format(len(status), **locals()))
        return

    logging.info("{} out of {} documents to process with {module}, checking {previous_module} status"
                 .format(len(todo), len(status),  **locals()))
                 
    previous_status = get_status(amcat_server, project, articleset, nlpipe_server, previous_module)
    cando = {id for (id, status) in previous_status.items() if status in "DONE"}

    if todo - cando:
        logging.warning("{} out of {} documents cannot be processed as {previous_module} is not done"
                        .format(len(todo-cando), len(todo), **locals()))

    todo = todo & cando
    if todo:
        logging.info("Assigning {} articles from {amcat_server} set {project}:{articleset}"
                     .format(len(todo), **locals()))
        for ids in splitlist(todo, itemsperbatch=100):
            ids = [str(id) for id in ids]
            logging.debug("Assigning {} articles...".format(len(ids)))
            input_files = nlpipe_server.bulk_result(previous_module, ids)
            input_files = [input_files[str(id)] for id in ids]
            nlpipe_server.bulk_process(module, input_files, ids=ids)


def process(amcat_server: AmcatAPI, project: int, articleset: int,
            nlpipe_server: Client, module: str,
            reset_error: bool=False, reset_started: bool=False, to_naf: bool=False) -> None:
    """
    Process the given documents

    :param amcat_server: Amcat server (url str or AmCATAPI object)
    :param project: AmCAT project ID (int)
    :param articleset: AmCAT Articleset ID (int)
    :param nlpipe_server: NLPipe server (url/dirname str or nlpipe.Client object)
    :param module: NLPipe module name (str)
    :param reset_started: Re-set started documents to pending
    :param reset_error: Re-assign documents with errors
    :param to_naf: Assign as NAF documents with metadata (otherwise, assign as plain text)
    :param token: Token to use for authentication
    """
    status = get_status(amcat_server, project, articleset, nlpipe_server, module)
    accept_status = {"UNKNOWN"}
    if reset_error:
        accept_status |= {"ERROR"}
    if reset_started:
        accept_status |= {"STARTED"}

    todo = [id for (id, status) in status.items() if status in accept_status]
    if todo:
        logging.info("Assigning {} articles from {amcat_server} set {project}:{articleset}"
                     .format(len(todo), **locals()))
        columns = 'headline,text,creator,date,url,uuid,medium,section,page' if args.naf else 'headline,text'
        for page in amcat_server.get_articles_by_id(articles=todo, columns=columns,
                                                            page_size=100, yield_pages=True):
            args = page['results']
            ids = [a['id'] for a in arts]
            texts = [_get_text(a, to_naf=args.naf) for a in arts]
            logging.debug("Assigning {} articles".format(len(ids)))
            nlpipe_server.bulk_process(args.module, texts, ids=ids, reset_error=reset_error,
                                                reset_pending=reset_started)
    logging.info("Done! Assigned {} articles".format(len(todo)))


def get_results(amcat_server: AmcatAPI, project: int, articleset: int,
                nlpipe_server: Client, module: str, format: str=None):
    status = get_status(amcat_server, project, articleset, nlpipe_server, module)
    toget = [id for (id, status) in status.items() if status == "DONE"]
    kargs = {'format': format} if format else {}
    for batch in splitlist(toget):
        yield from nlpipe_server.bulk_result(module, batch, **kargs).items()


def _normalize(txt):
    pars = []
    for par in txt.split("\n\n"):
        par = par.replace("\n", " ")
        par = re.sub("\s+", " ", par)
        par = par.strip()
        pars.append(par)
    return "\n\n".join(pars)


def _get_text(a, to_naf=False, lang='nl'):
    result = "\n\n".join([_normalize(a[x]) for x in ('headline', 'text')])
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

        naf.header.set_publicId(a['uuid'])
        #if 'url' in a:
        #    naf.header.set_uri(a['url'])
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
    parser.add_argument("action", help="NLPipe action", choices=["process", "process_pipe", "status", "result"])
    parser.add_argument("--naf", help="Use NAF input format (action=process)", action="store_true")
    parser.add_argument("--format", "-f", help="Result format (action=result)")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true")
    parser.add_argument("--reset-error", "-e", help="Reset errored documents (action=process)", action="store_true")
    parser.add_argument("--reset-started", "-p", help="Reset started documents (action=process)", action="store_true")
    parser.add_argument("--result-folder", "-o", help="Folder for storing results (one file per document)")
    parser.add_argument("--token", "-t", help="Provide auth token"
                        "(default reads ./.nlpipe_token or NLPIPE_TOKEN")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("amcatclient").setLevel(logging.INFO)

    logging.debug("Will {args.action} at nlpipe {args.nlpipeserver} all articles "
                  "from {args.amcatserver} set {args.articleset}".format(**locals()))

    amcatserver =AmcatAPI(args.amcatserver)
    nlpipeserver = get_client(args.nlpipeserver, args.token)
    
    if args.action == "process":
        process(amcatserver, args.project, args.articleset, nlpipeserver, args.module,
                args.reset_error, args.reset_started)
    if args.action == "process_pipe":
        process_pipe(amcatserver, args.project, args.articleset, nlpipeserver, args.module, "alpinonerc")
    if args.action == "status":
        status = get_status(amcatserver, args.project, args.articleset, nlpipeserver, args.module)
        for k, v in Counter(status.values()).items():
            print("{k}: {v}".format(**locals()))
    if args.action == 'result':
        results = get_results(amcatserver, args.project, args.articleset, nlpipeserver, args.module,
                              format=args.format)
        if args.format == "csv":
            for i, (id, csv_bytes) in enumerate(results):
                if i != 0 and "\n" in csv_bytes:
                    csv_bytes = csv_bytes.split("\n", 1)[1]
                print(csv_bytes.strip())
        else:
            if args.result_folder:
                for id, result in results:
                    fn = os.path.join(args.result_folder, str(id))
                    logging.debug(fn)
                    open(fn, 'w').write(result)
            else:
                print(json.dumps(results, indent=4))

