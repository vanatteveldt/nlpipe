# NLPipe [![Build Status](https://travis-ci.org/vanatteveldt/nlpipe.png?branch=master)](https://travis-ci.org/vanatteveldt/nlpipe)

Client/server based NLP Pipelining

This is a simple, filesystem-based format- and progress agnostic setup for running document processing.
The intended usage is to make it easy to package and distribute different parsers, preprocessors etc.,
and call them from other programs such as R or python without worrying about dependencies, installation, etc. 

Components:

- Storage
- HTTP Server
- Client bindings
- Workers

Installation
===

To use nlpipe you can use the docker image:

```{sh}
$ sudo docker pull vanatteveldt/nlpipe
$ sudo docker run -dp 5001:5001 vanatteveldt/nlpipe
8d8c0017d51f9c4f0e217e64bcd3b64b791a7122dcafec148900032439b1c272
$ curl -XPOST -sd"dit is een test" http://localhost:5001/api/modules/test_upper/
0xed16a14a645f095c94cc18d64b19920a
$ curl http://localhost:5001/api/modules/test_upper/0xed16a14a645f095c94cc18d64b19920a
DIT IS EEN TEST
```

To install nlpipe locally, it is best to create a virtual environment and install nlpipe in it


```{sh}
pyvenv env
env/bin/pip install -e git+git://github.com/vanatteveldt/nlpipe.git#egg=nlpipe
```
Now you can run e.g.:

```{sh}
env/bin/python -m nlpipe.restserver
```

Command line usage
===

The workers and client can be activated from the command line. (currently quite limited)

To start the workers with the provided example.conf configuration, run:

```{sh}
$ env/bin/python -m nlpipe.worker example.conf 
```

To test the workers, you can call the client directly:
```{sh}
$ env/bin/python -m nlpipe.client /tmp/nlpipe test_upper process "this is a test"
0x54b0c58c7ce9f2a8b551351102ee0938
$ env/bin/python -m nlpipe.client /tmp/nlpipe test_upper status 0x54b0c58c7ce9f2a8b551351102ee0938
DONE
$ env/bin/python -m nlpipe.client /tmp/nlpipe test_upper result 0x54b0c58c7ce9f2a8b551351102ee0938
THIS IS A TEST
```


Storage directory layout
===

Ths server uses file system to manage task queue and results cache. 
Each task (e.g. corenlp_lemmatize) contains subfolders containing the documents

```
- <task>
  - queue
  - in_process
  - results
  - errors
```

Process flow:
- client puts document into `<task>/queue`
- worker moves a document from `<task>/queue` to `<task>/in_process` and gets the text
- worker processes the document
- worker stores the result in `<task>/results` and removes it from `<task>/in_process`
- client retrieves the document from `<task>/results`

Clients/workers can either access the filesystem directly or use the HTTP server. 

HTTP Server
====

An HTTP server will allow access to the NLPipe service with the following REST endpoints:

From client perspective:

```
PUT <task>/<hash> # adds a document by hash
POST <task> # adds a document, returning the hash
HEAD <task>/<hash> # gets status of task
GET <task>/<hash> # get result for task (or 404 / error)
```

From worker perspective:

```
GET <task> # gets one document from task (and moves from queue to in_process)
GET <task>?n=N # gets N documents from task (and moves from queue to in_process)
PUT <task>/<hash> # stores result 
```

Client bindings
===

There are client bindings for the direct filesystem access and (in the future) for the HTTP server.
Browse the [Python client bindings API documentation](http://nlpipe.readthedocs.io/en/latest/nlpipe.html)
