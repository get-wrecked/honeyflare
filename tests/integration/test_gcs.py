import base64
import os
import unittest.mock as mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage

from honeyflare import process_bucket_object, __version__


pytestmark = pytest.mark.integration


def test_process_file(bucket, test_files, blob_name):
    patterns = [
        '/authors/:id/*',
        '/books/:isbn',
    ]
    local_file = test_files.create_file(
        {
            'ClientRequestURI': '/books/f08ca7b3-f51a-44d3-9669-384bc5a65720',
            'EdgeEndTimestamp': 900000000,
            'EdgeStartTimestamp': 1000000000,
        },
        {
            'ClientRequestURI': '/authors/42/pictures?expand=true',
            'EdgeEndTimestamp': 1900000000,
            'EdgeStartTimestamp': 20000000000,
        },
    )
    blob = bucket.blob(blob_name)
    with open(local_file, 'rb') as fh:
        blob.upload_from_file(fh)

    with mock.patch('libhoney.Client') as mock_client:
        mock_event = mock_client.return_value.new_event.return_value
        process_bucket_object(bucket, blob_name, 'test-dataset', 'test-key', patterns, set())
        mock_client.assert_called_with(
            writekey='test-key',
            dataset='test-dataset',
            block_on_send=True,
            user_agent_addition='honeyflare/%s' % __version__,
        )
        first_call = mock_event.add.call_args_list[0]
        assert (first_call[0][0]['ClientRequestURI'] ==
            '/books/f08ca7b3-f51a-44d3-9669-384bc5a65720')
        assert first_call[0][0]['UriShape'] == '/books/:isbn'
        second_call = mock_event.add.call_args_list[1]
        assert (second_call[0][0]['ClientRequestURI'] ==
            '/authors/42/pictures?expand=true')
        assert second_call[0][0]['UriShape'] == '/authors/:id/*?expand=?'
        assert 'Query_expand' not in second_call[0][0]
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
