from collections import defaultdict

import pytest

from honeyflare import get_raw_file_entries, get_sampled_file_entries, enrich_entry, compile_pattern, __version__


def test_get_raw_file_entries(test_files):
    file_path = test_files.create_file({'eventName': 'value1'}, {'eventName': 'value2'})
    entries = list(get_raw_file_entries(file_path))
    assert entries == [
        '{"eventName": "value1"}\n',
        '{"eventName": "value2"}\n',
    ]


def test_get_sampled_file_entries(test_files):
    lines = []
    for i in range(200):
        lines.append({'EdgeResponseStatus': 200})
        lines.append({'EdgeResponseStatus': 201})
        lines.append({'EdgeResponseStatus': 204})
        lines.append({'EdgeResponseStatus': 301})
        lines.append({'EdgeResponseStatus': 403})
        lines.append({'EdgeResponseStatus': 404})
        lines.append({'EdgeResponseStatus': 502})
        lines.append({'EdgeResponseStatus': 503})
    file_path = test_files.create_file(*lines)

    entries = list(t[1] for t in get_sampled_file_entries(file_path, {
        200: 2,
        201: 5,
        300: 6,
        400: 4,
        404: 0,
        500: 3,
        503: 1,
    }))

    lines_by_status = defaultdict(int)
    for entry in entries:
        lines_by_status[entry['EdgeResponseStatus']] += 1

    # Expected ranges computed with ./tools/expected-success.py 200 <sample rate>
    # to get a range that will pass the test in 99.99% of runs
    assert 72 < lines_by_status[200] < 128
    assert 17 < lines_by_status[201] < 63
    assert 11 < lines_by_status[301] < 55
    assert 39 < lines_by_status[502] < 93
    assert 25 < lines_by_status[403] < 75
    assert lines_by_status[404] == 0
    assert lines_by_status[503] == 200


def test_enrich_entry():
    entry = {
        'ClientRequestURI': "/users/id1337?regularParam=paramValue&emptyParam=",
        'EdgeEndTimestamp': 1582850070117000000,
        'EdgeStartTimestamp': 1582850070112000000,
        'EdgeRequestHost': 'example.com',
        'OriginResponseTime': 150*1e6,
    }

    patterns = [compile_pattern(p) for p in [
        '/other/thing/:thingId',
        '/users/:userId/pictures',
        '/users/:userId',
    ]]
    enrich_entry(entry, patterns, None)

    assert entry['DurationSeconds'] == 0.005
    assert entry['DurationMs'] == 5
    assert entry['OriginResponseTimeSeconds'] == 0.15
    assert entry['OriginResponseTimeMs'] == 150
    assert entry['Path'] == '/users/id1337'
    assert entry['PathShape'] == '/users/:userId'
    assert entry['Path_userId'] == 'id1337'
    assert entry['Query'] == 'regularParam=paramValue&emptyParam='
    assert entry['QueryShape'] == 'emptyParam=?&regularParam=?'
    assert entry['Query_emptyParam'] == ''
    assert entry['Query_regularParam'] == 'paramValue'
    assert entry['UriShape'] == '/users/:userId?emptyParam=?&regularParam=?'


@pytest.mark.parametrize('ip,expected_version', [
    ('1.2.3.4', 4),
    ('2001:0db8:85a3:0000:0000:8a2e:0370:7334', 6)
])
def test_enrich_origin_ip(ip, expected_version):
    entry = {
        'OriginIP': ip,
    }

    enrich_entry(entry, [], None)

    assert entry['OriginIPVersion'] == expected_version
