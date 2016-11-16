#!/usr/bin/env python

from distutils.core import setup

setup(
    name="nlpipe",
    version="0.41",
    description="Simple NLP Pipelinining based on a file system",
    author="Wouter van Atteveldt",
    author_email="wouter@vanatteveldt.com",
    packages=["nlpipe"],
    keywords = ["NLP", "pipelining"],
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Topic :: Text Processing",
    ],
    install_requires=[
        "Flask",
        "requests",
        "pynlpl",
        "corenlp_xml>=1.0.4",
    ]
)
