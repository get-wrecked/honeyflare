from honeyflare import get_file_entries


def test_get_file_entries(test_files):
    file_path = test_files.create_file({'eventName': 'value1'}, {'eventName': 'value2'})
    entries = list(get_file_entries(file_path))
    assert entries == [
        {'eventName': 'value1'},
        {'eventName': 'value2'},
    ]
