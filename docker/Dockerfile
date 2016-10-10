# This is a comment
FROM ubuntu:16.04
MAINTAINER Wouter van Atteveldt (wouter@vanatteveldt.com)
RUN apt-get update && apt-get install -y git python3-pip wget unzip openjdk-9-jre-headless
RUN pip3 install -e git+git://github.com/vanatteveldt/nlpipe.git#egg=nlpipe

RUN wget -O /tmp/stanford.zip http://nlp.stanford.edu/software/stanford-corenlp-full-2015-12-09.zip
RUN cd /tmp; unzip stanford.zip
ENV CORENLP_HOME=/tmp/stanford-corenlp-full-2015-12-09

COPY workers.conf /tmp/
COPY run_server.sh /tmp

EXPOSE 5001

CMD ["/bin/bash", "/tmp/run_server.sh"]
