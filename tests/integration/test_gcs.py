import base64
import os
import unittest.mock as mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage

from honeyflare import process_bucket_object, __version__


pytestmark = pytest.mark.integration


def test_process_file(bucket, test_files, blob_name):
    local_file = test_files.create_file({'key1': 'val1'}, {'key2': 'val2'})
    blob = bucket.blob(blob_name)
    with open(local_file, 'rb') as fh:
        blob.upload_from_file(fh)

    with mock.patch('libhoney.Client') as mock_client:
        mock_event = mock_client.return_value.new_event.return_value
        process_bucket_object(bucket, blob_name, 'test-dataset', 'test-key')
        mock_client.assert_called_with(
            writekey='test-key',
            dataset='test-dataset',
            block_on_send=True,
            user_agent_addition='honeyflare/%s' % __version__,
        )
        mock_event.add.assert_any_call({'key1': 'val1'})
        mock_event.add.assert_any_call({'key2': 'val2'})
        mock_event.send.assert_called_with()
        mock_client.return_value.close.assert_called_once()


@pytest.fixture
def blob_name(bucket):
    _blob_name = 'honeyflare-test-' + base64.urlsafe_b64encode(os.urandom(8)).decode('utf-8')
    try:
        yield _blob_name
    finally:
        try:
            bucket.blob(_blob_name).delete()
        except NotFound:
            pass
