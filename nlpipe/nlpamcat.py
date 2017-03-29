import argparse
import json
from collections import Counter
import re
from io import BytesIO
from typing import Union, Iterable, Mapping

from amcatclient import AmcatAPI
from nlpipe.client import get_client, Client
import logging
from KafNafParserPy import KafNafParser, CfileDesc, Cpublic, CHeader


def _amcat(amcat_server: Union[str, AmcatAPI]) -> AmcatAPI:
    return AmcatAPI(amcat_server) if isinstance(amcat_server, str) else amcat_server


def _nlpipe(nlpipe_server: Union[str, Client]) -> Client:
    return get_client(nlpipe_server) if isinstance(nlpipe_server, str) else nlpipe_server


def get_ids(amcat_server: Union[str, AmcatAPI], project: int, articleset: int) -> Iterable[int]:
    """
    Get the article ids for this articleset

    :param amcat_server: Amcat server
    :param project: AmCAT project ID
    :param articleset: AmCAT Articleset ID
    :return: sequence of AmCAT article IDs
    """
    return (x['id'] for x in
            _amcat(amcat_server).get_articles(project, articleset, columns=['id']))


def get_status(amcat_server: Union[str, AmcatAPI], project: int, articleset: int,
               nlpipe_server: Union[str, Client], module: str) -> Mapping[int, str]:
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
            for (id, status) in _nlpipe(nlpipe_server).bulk_status(module, ids).items()}


def process(amcat_server: Union[str, AmcatAPI], project: int, articleset: int,
            nlpipe_server: Union[str, Client], module: str,
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
        for arts in _amcat(amcat_server).get_articles_by_id(articles=todo, columns=columns,
                                                            page_size=100, yield_pages=True):
            ids = [a['id'] for a in arts]
            texts = [_get_text(a, to_naf=args.naf) for a in arts]
            logging.debug("Assigning {} articles".format(len(ids)))
            _nlpipe(nlpipe_server).bulk_process(args.module, texts, ids=ids, reset_error=reset_error,
                                                reset_pending=reset_started)
    logging.info("Done! Assigned {} articles".format(len(todo)))


def get_results(amcat_server: Union[str, AmcatAPI], project: int, articleset: int,
            nlpipe_server: Union[str, Client], module: str, format: str=None):
    status = get_status(amcat_server, project, articleset, nlpipe_server, module)
    toget = [id for (id, status) in status.items() if status == "DONE"]
    kargs = {'format': format} if format else {}
    return _nlpipe(nlpipe_server).bulk_result(module, toget, **kargs)


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
    parser.add_argument("--naf", help="Use NAF input format (action=process)", action="store_true")
    parser.add_argument("--format", "-f", help="Result format (action=result)")
    parser.add_argument("--verbose", "-v", help="Verbose (debug) output", action="store_true")
    parser.add_argument("--reset-error", "-e", help="Reset errored documents (action=process)", action="store_true")
    parser.add_argument("--reset-started", "-p", help="Reset started documents (action=process)", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format='[%(asctime)s %(name)-12s %(levelname)-5s] %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("amcatclient").setLevel(logging.INFO)

    logging.debug("Will {args.action} at nlpipe {args.nlpipeserver} all articles "
                  "from {args.amcatserver} set {args.articleset}".format(**locals()))

    if args.action == "process":
        process(args.amcatserver, args.project, args.articleset, args.nlpipeserver, args.module,
                args.reset_error, args.reset_started)
    if args.action == "status":
        status = get_status(args.amcatserver, args.project, args.articleset, args.nlpipeserver, args.module)
        for k, v in Counter(status.values()).items():
            print("{k}: {v}".format(**locals()))
    if args.action == 'result':
        results = get_results(args.amcatserver, args.project, args.articleset, args.nlpipeserver, args.module,
                              format=args.format)
        if args.format == "csv":
            for i, csv_bytes in enumerate(results.values()):
                if i != 0 and "\n" in csv_bytes:
                    csv_bytes = csv_bytes.split("\n", 1)[1]
                print(csv_bytes.strip())
        else:
            print(json.dumps(results, indent=4))

