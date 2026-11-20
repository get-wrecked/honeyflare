import shutil
import time
from collections import defaultdict
from unittest import mock

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from honeyflare import get_raw_file_entries, process_bucket_object, __version__


def test_get_raw_file_entries(test_files):
    file_path = test_files.create_file({"eventName": "value1"}, {"eventName": "value2"})
    entries = list(get_raw_file_entries(file_path))
    assert entries == [
        '{"eventName": "value1"}\n',
        '{"eventName": "value2"}\n',
    ]


def test_slow_exporter_loses_no_spans(test_files, tmp_path):
    entries = [
        {"RayID": f"{number:016x}", "EdgeEndTimestamp": 1767323045000000000}
        for number in range(1, 5001)
    ]
    log_file = test_files.create_file(*entries)
    exporter = SlowStartingExporter()
    lock_bucket = mock.Mock()
    lock_bucket.blob.return_value.exists.return_value = False

    with (
        mock.patch("honeyflare.OTLPSpanExporter", return_value=exporter),
        mock.patch(
            "honeyflare.download_file",
            side_effect=lambda *args: shutil.copy(log_file, tmp_path),
        ),
    ):
        events = process_bucket_object(None, "example.log.gz", lock_bucket=lock_bucket)

    assert events == 5000
    assert exporter.exported == 5000


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
