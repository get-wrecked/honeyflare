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
    for i in range(100):
        lines.append({'EdgeResponseStatus': 200,'eventName': 'value'})
    file_path = test_files.create_file(*lines)

    entries = list(get_sampled_file_entries(file_path, {
        200: 2,
    }))
    assert 20 < len(entries) < 80



def test_enrich_entry():
    entry = {
        'ClientRequestURI': "/users/id1337?regularParam=paramValue&emptyParam=",
        'EdgeEndTimestamp': 1582850070117000000,
        'EdgeStartTimestamp': 1582850070112000000,
        'EdgeRequestHost': 'example.com',
    }

    patterns = [compile_pattern(p) for p in [
        '/other/thing/:thingId',
        '/users/:userId/pictures',
        '/users/:userId',
    ]]
    enrich_entry(entry, patterns, None)

    assert entry['DurationSeconds'] == 0.005
    assert entry['DurationMs'] == 5
    assert entry['Path'] == '/users/id1337'
    assert entry['PathShape'] == '/users/:userId'
    assert entry['Path_userId'] == 'id1337'
    assert entry['Query'] == 'regularParam=paramValue&emptyParam='
    assert entry['QueryShape'] == 'emptyParam=?&regularParam=?'
    assert entry['Query_emptyParam'] == ''
    assert entry['Query_regularParam'] == 'paramValue'
    assert entry['UriShape'] == '/users/:userId?emptyParam=?&regularParam=?'
