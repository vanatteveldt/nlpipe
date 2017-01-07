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

Docker
---

To use nlpipe you can use the docker image: (you might need to run this as superuser)

```{sh}
docker run --name nlpipe -dp 5001:5001 vanatteveldt/nlpipe
```

This will pull the nlpipe docker image and run the nlpipe restserver on port 5001 and by default run all known worker modules. Note: The `-d` means that the docker process will be 'detached', i.e. run in the background. 

For more options, run:

```{sh}
docker run vanatteveldt/nlpipe --help
```

To see (or *f*ollow) the logs of a running worker, use:

```{sh}
docker logs [-f] nlpipe
```

Local install via pip
---

To install nlpipe locally, it is best to create a virtual environment and install nlpipe in it:


```{sh}
pyvenv env
env/bin/pip install -e git+git://github.com/vanatteveldt/nlpipe.git#egg=nlpipe
```

Now you can run nlpipe from the created environment, e.g. to run the webserver do:

```{sh}
env/bin/python -m nlpipe.restserver
```

Using nlpipe
---

Command line usage
===

To use nlpipe, you can use use the file system or HTTP client. 
They can be used on the command line through docker or from the local install.
For example, to start a worker for the `test_upper` module against a local HTTP server run:

```{sh}
$ env/bin/python -m nlpipe.worker http://localhost:5001 test_upper
```

To run commands using docker, use `docker run python` (as a ew container) or `docker exec <container> python` (in an existing container) instead of `env/bin/python`: 

```{sh}
$ docker exec python -m nlpipe.worker http://localhost:5001 test_upper
```

To process documents you can use the following command line calls:

```{sh}
$ env/bin/python -m nlpipe.client /tmp/nlpipe test_upper process "this is a test"
0x54b0c58c7ce9f2a8b551351102ee0938
$ env/bin/python -m nlpipe.client /tmp/nlpipe test_upper status 0x54b0c58c7ce9f2a8b551351102ee0938
DONE
$ env/bin/python -m nlpipe.client /tmp/nlpipe test_upper result 0x54b0c58c7ce9f2a8b551351102ee0938
THIS IS A TEST
```

Example Setups
===

CoreNLP lemmatize
---

To setup corenlp lemmatize and nlpipe, use:

```{sh}
$ docker run --name corenlp -dp 9000:9000 chilland/corenlp-docker 
$ docker run --name nlpipe --link corenlp:corenlp -e "CORENLP_HOST=http://corenlp:9000" -dp 5001:5001 vanatteveldt/nlpipe
```
And e.g. lemmatize a test sentence:

```{sh}
$ docker exec -it nlpipe python -m nlpipe.client /tmp/nlpipe-data corenlp_lemmatize process_inline --format=csv 'this is a test'
id,sentence,offset,word,lemma,POS,POS1,ner
0x54b0c58c7ce9f2a8b551351102ee0938,1,0,this,this,DT,D,O
0x54b0c58c7ce9f2a8b551351102ee0938,1,5,is,be,VBZ,V,O
0x54b0c58c7ce9f2a8b551351102ee0938,1,8,a,a,DT,D,O
0x54b0c58c7ce9f2a8b551351102ee0938,1,10,test,test,NN,N,O
```

Distributed setup
---

You can setup a server on one computer and run workers on a different computer. 

Setting up the server without any workers:

```{sh}
docker run --name nlpipe -dp 5001:5001 vanatteveldt/nlpipe python -m nlpipe.restserver
```

Starting a `corenlp_lemmatize` worker on a different (or the same) machine (assuming the server runs at `example.com`):

```{sh}
$ docker run --name corenlp -dp 9000:9000 chilland/corenlp-docker 
docker run --name nlpipeworker --link corenlp:corenlp -e "CORENLP_HOST=http://corenlp:9000" -dp 5001:5001 vanatteveldt/nlpipe python -m nlpipe.worker http://example.com:5001 corenlp_lemmatize
```

And lemmatizing a document from a third machine:
(note that using a docker is overkill here, it would be better to just use the python or R client)

```{sh}
docker run vanatteveldt/nlpipe python -m nlpipe.client http://i.amcat.nl:5001 corenlp_lemmatize process_inline --format csv "this is a test!"
```

Design
===

Storage directory layout
---

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

The goal of this setup is to use the filesystem as a hierarchical database and use the UNIX atomic FS operations as a thread-safe locking/scheduling mechanism. The worker that manages to e.g. move the document from queue to in_process is the one doing the task. If two workers simultaneously select the same document to process, only the first will be able to move it, and the second will get an error from the file system and should select the next document. 

Before putting a document on the queue, a client should check whether it is not already known and then create it.  
This is not atomic, so it is possible that another thread has created the document at between checking and creating, but in that case the creation will give an error. 
In the (unlikely) event that another thread has created the document and a worked has moved it to in_process in the interval between checking and creating a document, there is a risk that the document will be processed twice, but this should not lead to a problem except for wasted processing time. 


Client access
---

Clients/workers can access the filesystem directly. 
Since it is thread safe, this is the most efficient way of 
or use the HTTP server. 

The built-in HTTP server will allow access to the NLPipe service with the following REST endpoints:

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

There are also client bindings for the direct filesystem access (python) and for the HTTP server (python and R).
The python bindings are included in this repository ([nlpipe/client.py](nlpipe/client.py)). R bindings are available at [http://github.com/vanatteveldt/nlpiper](vanatteveldt/nlpiper). 
