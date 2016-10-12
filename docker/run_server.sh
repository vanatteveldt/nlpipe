python3 -m nlpipe.restserver -p 5001 -H 0.0.0.0 /tmp/nlpipe-data &
python3 -m nlpipe.worker /tmp/nlpipe-data nlpipe.modules.corenlp.CoreNLPLemmatizer nlpipe.modules.test_upper.TestUpper

wait
