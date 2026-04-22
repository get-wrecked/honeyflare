import base64
import datetime
import os
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound
from urllib3.exceptions import ProtocolError

from honeyflare import download_file, process_bucket_object
from honeyflare.exceptions import RetriableError


pytestmark = pytest.mark.integration


def test_process_file(bucket, test_files, blob_name):
    patterns = [
        "/authors/:id/*",
        "/books/:isbn",
    ]
    local_file = test_files.create_file(
        {
            "ClientRequestURI": "/books/f08ca7b3-f51a-44d3-9669-384bc5a65720",
            "EdgeResponseStatus": 200,
            "EdgeEndTimestamp": 1000000000,
            "EdgeStartTimestamp": 900000000,
            "ClientRequestMethod": "GET",
            "RayID": "6f2de346beec9644",
            "ParentRayID": "00",
        },
        {
            "ClientRequestURI": "/authors/42/pictures?expand=true",
            "EdgeResponseStatus": 200,
            "EdgeEndTimestamp": 2000000000,
            "EdgeStartTimestamp": 19000000000,
            "ClientRequestMethod": "GET",
            "RayID": "1111111111111111",
            "ParentRayID": "00",
        },
    )
    blob = bucket.blob(blob_name)
    with open(local_file, "rb") as fh:
        blob.content_encoding = "gzip"
        blob.upload_from_file(fh, content_type="application/json")

    with mock.patch("honeyflare.OTLPSpanExporter") as mock_exporter_cls:
        mock_exporter = mock_exporter_cls.return_value
        mock_exporter.export.return_value = 0

        events_handled = process_bucket_object(
            bucket,
            blob_name,
            "test-dataset",
            "test-key",
            patterns=patterns,
            query_param_filter=set(),
        )

    assert events_handled == 2
    mock_exporter_cls.assert_called_with(
        endpoint="https://api.honeycomb.io/v1/traces",
        headers={
            "x-honeycomb-team": "test-key",
            "x-honeycomb-dataset": "test-dataset",
        },
    )

    # Pull out the exported spans (BatchSpanProcessor calls export with a list)
    exported_spans = []
    for call in mock_exporter.export.call_args_list:
        exported_spans.extend(call[0][0])

    assert len(exported_spans) == 2
    uri_shapes = {span.attributes["UriShape"] for span in exported_spans}
    assert uri_shapes == {"/books/:isbn", "/authors/:id/*?expand=?"}
    for span in exported_spans:
        # Query_expand was filtered out by query_param_filter=set()
        assert "Query_expand" not in span.attributes


def test_process_repeated_file(bucket, test_files, blob_name):
    local_file = test_files.create_file(
        {
            "ClientRequestURI": "/books/f08ca7b3-f51a-44d3-9669-384bc5a65720",
            "EdgeResponseStatus": 200,
            "EdgeEndTimestamp": 1000000000,
            "EdgeStartTimestamp": 900000000,
            "ClientRequestMethod": "GET",
            "RayID": "6f2de346beec9644",
            "ParentRayID": "00",
        }
    )
    blob = bucket.blob(blob_name)
    with open(local_file, "rb") as fh:
        blob.upload_from_file(fh)

    with mock.patch("honeyflare.OTLPSpanExporter") as mock_exporter_cls:
        mock_exporter = mock_exporter_cls.return_value
        mock_exporter.export.return_value = 0
        process_bucket_object(bucket, blob_name, "test-dataset", "test-key")
        process_bucket_object(bucket, blob_name, "test-dataset", "test-key")

    exported_spans = []
    for call in mock_exporter.export.call_args_list:
        exported_spans.extend(call[0][0])

    # Should have only processed the event the first time
    assert len(exported_spans) == 1


def test_download_raises_retriable_exception(bucket):
    with mock.patch("google.cloud.storage.blob.RawDownload") as download_mock:
        # Some random urllib3 exception
        download_mock.return_value.consume.side_effect = ProtocolError()
        with pytest.raises(RetriableError):
            download_file(bucket, "foo")


@pytest.fixture
def blob_name(bucket):
    date_prefix = datetime.datetime.utcnow().strftime("%Y%m%d")
    _blob_name = "%s/honeyflare-test-%s.gz" % (
        date_prefix,
        base64.urlsafe_b64encode(os.urandom(8)).decode("utf-8"),
    )
    try:
        yield _blob_name
    finally:
        try:
            bucket.blob(_blob_name).delete()
        except NotFound:
            pass
