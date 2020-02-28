from honeyflare import get_file_entries, enrich_entry, __version__


def test_get_file_entries(test_files):
    file_path = test_files.create_file({'eventName': 'value1'}, {'eventName': 'value2'})
    entries = list(get_file_entries(file_path))
    assert entries == [
        {'eventName': 'value1'},
        {'eventName': 'value2'},
    ]


def test_enrich_entry():
    entry = {
        'ClientRequestURI': "/path/?emptyParam=&regularParam=paramValue",
        'EdgeEndTimestamp': 1582850070117000000,
        'EdgeStartTimestamp': 1582850070112000000,
        'EdgeRequestHost': 'example.com',
    }

    enrich_entry(entry)

    assert entry['DurationSeconds'] == 0.005
    assert entry['DurationMs'] == 5
    assert entry['Query'] == 'emptyParam=&regularParam=paramValue'
    assert entry['QueryShape'] == 'emptyParam=?&regularParam=?'
    # urlparse always appends a trailing slash to the path, might fix later
    assert entry['PathShape'] == '/path/'
    assert entry['UriShape'] == '/path/?emptyParam=?&regularParam=?'
    assert entry['Query_emptyParam'] == ''
    assert entry['Query_regularParam'] == 'paramValue'
