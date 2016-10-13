# This is a comment
FROM python:3.5
MAINTAINER Wouter van Atteveldt (wouter@vanatteveldt.com)
EXPOSE 5001

RUN pip3 install -e git+git://github.com/vanatteveldt/nlpipe.git#egg=nlpipe #redo
CMD ["python", "-m", "nlpipe.restserver", "-p", "5001", "-H", "0.0.0.0", "/tmp/nlpipe-data", "corenlp_lemmatize", "test_upper"]

