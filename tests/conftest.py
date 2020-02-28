import os

import pytest
from google.cloud import storage


@pytest.fixture
def bucket(client):
    return client.bucket(os.environ['HONEYFLARE_TEST_BUCKET'])


@pytest.fixture
def client():
    return storage.Client()
