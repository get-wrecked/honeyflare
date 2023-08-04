from collections import defaultdict

from honeyflare import get_raw_file_entries, __version__


def test_get_raw_file_entries(test_files):
    file_path = test_files.create_file({"eventName": "value1"}, {"eventName": "value2"})
    entries = list(get_raw_file_entries(file_path))
    assert entries == [
        '{"eventName": "value1"}\n',
        '{"eventName": "value2"}\n',
    ]
