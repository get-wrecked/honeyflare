#!/bin/sh

./venv/bin/py.test \
    --cov honeyflare \
    --cov-config .coveragerc \
    --cov-report html:coverage \
    tests/ \
    -m 'not integration' \
    "$@"

open coverage/index.html
