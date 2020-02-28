import gzip
import json
import os
import tempfile

import pytest
from google.cloud import storage


@pytest.fixture
def bucket(client):
    return client.bucket(os.environ['HONEYFLARE_TEST_BUCKET'])


@pytest.fixture
def client():
    return storage.Client()


class CleanedTempLogFiles():

    def __init__(self):
        self.files_created = []


    def create_file(self, data):
        with tempfile.NamedTemporaryFile(delete=False) as tmp_fh:
            with gzip.GzipFile(fileobj=tmp_fh, mode='w') as gzip_fh:
                gzip_fh.write(json.dumps(data).encode('utf-8'))
            name = tmp_fh.name

        self.files_created.append(name)
        return name


    def clean_up(self):
        for file in self.files_created:
            os.remove(file)


@pytest.fixture
def test_files():
    '''
    Return an object that can be used to create files with arbitrary data
    that will be cleaned up after the test.
    '''
    file_factory = CleanedTempLogFiles()
    try:
        yield file_factory
    finally:
        file_factory.clean_up()
