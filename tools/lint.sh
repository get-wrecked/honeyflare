#!/bin/sh

set -e

./venv/bin/pylint --rcfile .pylintrc honeyflare main.py
# Ignore some extra checks for tests
./venv/bin/pylint --rcfile .pylintrc \
    --disable redefined-outer-name \
    --disable singleton-comparison \
     tests
