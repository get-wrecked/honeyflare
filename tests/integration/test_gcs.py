import base64
import datetime
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
            'EdgeResponseStatus': 200,
            'EdgeEndTimestamp': 1000000000,
            'EdgeStartTimestamp': 900000000,
        },
        {
            'ClientRequestURI': '/authors/42/pictures?expand=true',
            'EdgeResponseStatus': 200,
            'EdgeEndTimestamp': 2000000000,
            'EdgeStartTimestamp': 19000000000,
        },
    )
    blob = bucket.blob(blob_name)
    with open(local_file, 'rb') as fh:
        blob.content_encoding = 'gzip'
        blob.upload_from_file(fh, content_type='application/json')

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
        # This property is overwritten for the first event, thus only testing it
        # on the second one
        assert mock_event.created_at == datetime.datetime(1970, 1, 1, 0, 0, 2)
        assert 'Query_expand' not in second_call[0][0]
        mock_event.send_presampled.assert_called_with()
        mock_client.return_value.close.assert_called_once()


def test_process_repeated_file(bucket, test_files, blob_name):
    local_file = test_files.create_file({
        'ClientRequestURI': '/books/f08ca7b3-f51a-44d3-9669-384bc5a65720',
        'EdgeResponseStatus': 200,
        'EdgeEndTimestamp': 1000000000,
        'EdgeStartTimestamp': 900000000,
    })
    blob = bucket.blob(blob_name)
    with open(local_file, 'rb') as fh:
        blob.upload_from_file(fh)

    with mock.patch('libhoney.Client') as mock_client:

        process_bucket_object(bucket, blob_name, 'test-dataset', 'test-key')
        process_bucket_object(bucket, blob_name, 'test-dataset', 'test-key')

        # Should have only processed the event the first time
        assert mock_client.return_value.new_event.call_count == 1


@pytest.fixture
def blob_name(bucket):
    date_prefix = datetime.datetime.utcnow().strftime('%Y%m%d')
    _blob_name = '%s/honeyflare-test-%s.gz' % (
        date_prefix, base64.urlsafe_b64encode(os.urandom(8)).decode('utf-8'))
    try:
        yield _blob_name
    finally:
        try:
            bucket.blob(_blob_name).delete()
        except NotFound:
            pass
