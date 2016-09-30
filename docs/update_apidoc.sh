#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
sphinx-apidoc -fo $DIR $DIR/../nlpipe
