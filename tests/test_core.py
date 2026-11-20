import math
import shutil
import time
from collections import defaultdict
from unittest import mock

import pytest
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from honeyflare import (
    RetriableError,
    get_raw_file_entries,
    process_bucket_object,
    __version__,
)


def test_get_raw_file_entries(test_files):
    file_path = test_files.create_file({"eventName": "value1"}, {"eventName": "value2"})
    entries = list(get_raw_file_entries(file_path))
    assert entries == [
        '{"eventName": "value1"}\n',
        '{"eventName": "value2"}\n',
    ]


def test_slow_exporter_loses_no_spans(test_files, tmp_path):
    exporter = SlowStartingExporter()

    events = process_log_file(test_files, tmp_path, exporter, lines=5000)

    assert events == 5000
    assert exporter.exported == 5000


def test_failed_batch_is_sent_again(test_files, tmp_path):
    exporter = FailingExporter(failures=1)

    events = process_log_file(test_files, tmp_path, exporter, lines=5000)

    assert events == 5000
    assert exporter.exported == 5000


def test_failed_export_fails_the_run(test_files, tmp_path):
    exporter = FailingExporter(failures=math.inf)

    with (
        mock.patch("honeyflare.mark_as_processed") as mark_as_processed,
        pytest.raises(RetriableError),
    ):
        process_log_file(test_files, tmp_path, exporter, lines=5000)

    mark_as_processed.assert_not_called()


def process_log_file(test_files, tmp_path, exporter, lines):
    entries = [
        {"RayID": f"{number:016x}", "EdgeEndTimestamp": 1767323045000000000}
        for number in range(1, lines + 1)
    ]
    log_file = test_files.create_file(*entries)
    lock_bucket = mock.Mock()
    lock_bucket.blob.return_value.exists.return_value = False

    with (
        mock.patch("honeyflare.OTLPSpanExporter", return_value=exporter),
        mock.patch(
            "honeyflare.download_file",
            side_effect=lambda *args: shutil.copy(log_file, tmp_path),
        ),
    ):
        return process_bucket_object(None, "example.log.gz", lock_bucket=lock_bucket)


class SlowStartingExporter(SpanExporter):
    """
    Takes a second over its first batch, like a collector that has fallen behind.
    """

    def __init__(self):
        self.exported = 0

    def export(self, spans):
        if self.exported == 0:
            time.sleep(1)
        self.exported += len(spans)
        return SpanExportResult.SUCCESS


class FailingExporter(SpanExporter):
    """
    Fails its first batches, like a collector that can't be reached for a while.
    """

    def __init__(self, failures):
        self.failures = failures
        self.exported = 0

    def export(self, spans):
        if self.failures > 0:
            self.failures -= 1
            return SpanExportResult.FAILURE
        self.exported += len(spans)
        return SpanExportResult.SUCCESS
