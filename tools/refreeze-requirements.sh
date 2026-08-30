#!/bin/sh

# Update requirements.in and dev-requirements.in with your new dependencies (optional),
# then run this script. It resolves the new pins and syncs .venv to them, so
# there is no separate ./configure step afterwards.
#
# This resolves with uv instead of installing into a throwaway virtualenv and
# running pip freeze. A freeze records whatever happened to end up in that
# virtualenv, resolved against whichever python3 the developer had; a compile is
# a real resolve pinned to the runtime we actually deploy on. uv also writes the
# command it used into each output file's header, so the outputs say how to
# reproduce themselves. Same setup as the functions in medal-infrastructure's
# cloud-functions/.

set -e

# Matches the Cloud Function runtime (python314) honeyflare is deployed on.
PYTHON_VERSION=3.14

uv pip compile requirements.in \
    --python-version=$PYTHON_VERSION \
    --output-file=requirements.txt

uv pip compile dev-requirements.in \
    --python-version=$PYTHON_VERSION \
    --output-file=dev-requirements.txt
