/usr/bin/python3 -m nlpipe.restserver -p 5001 -H 0.0.0.0 /tmp/nlpipe-data &
/usr/bin/python3 -m nlpipe.worker /tmp/nlpipe-data /tmp/workers.conf &

wait
