# nlpipe
Client/server based NLP Pipelining

Components:

- Server
- Workers
- Client

Server
===

Uses file system to manage task queue and results cache. 

File system layout e.g.
```
- <task>
  - queue
  - in_process
  - result
```

Each folder contains files with hash of input text as filename

Interface
====
either direct filesystem access or HTTP REST.

Process flow:
- get documents from <task>/queue, move to <task>/in_process
- parse
- store result in <task>/result, remove from <task>/in_process

HTTP REST endpoints

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


