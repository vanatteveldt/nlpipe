# This is a comment
FROM python:3.5
MAINTAINER Wouter van Atteveldt (wouter@vanatteveldt.com)
RUN pip3 install -e git+git://github.com/vanatteveldt/nlpipe.git#egg=nlpipe

COPY run_server.sh /tmp

EXPOSE 5001

CMD ["/bin/bash", "/tmp/run_server.sh"]
