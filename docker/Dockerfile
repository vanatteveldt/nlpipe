# This is a comment
FROM python:3.5
MAINTAINER Wouter van Atteveldt (wouter@vanatteveldt.com)
EXPOSE 5001

RUN pip3 install -e git+git://github.com/vanatteveldt/nlpipe.git#egg=nlpipe #redo
COPY run_server.sh /tmp
CMD ["/bin/bash", "/tmp/run_server.sh"]
