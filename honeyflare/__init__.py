import gzip
import json
import urllib.parse

import libhoney

from .version import __version__


def process_bucket_object(bucket, object_name, honeycomb_dataset, honeycomb_key, settings=None):
    '''
    :param bucket: A `google.cloud.storage.bucket.Bucket` logs should be downloaded from.
    :param object_name: The name of the object in the bucket to download.
    :param honeycomb_dataset: The name of the honeycomb dataset to write to.
    :param honeycomb_key: The honeycomb API key.
    '''
    local_path = download_file(bucket, object_name)
    libhoney_client = create_libhoney_client(honeycomb_key, honeycomb_dataset)
    for entry in get_file_entries(local_path):
        event = libhoney_client.new_event()
        event.add(entry)
        event.send()

    libhoney_client.close()


def create_libhoney_client(writekey, dataset):
    client = libhoney.Client(
        writekey=writekey,
        dataset=dataset,
        block_on_send=True,
        user_agent_addition='honeyflare/%s' % __version__,
    )
    client.add_field('MetaProcessor', 'honeyflare/%s' % __version__)
    return client


def download_file(bucket, object_name):
    blob = bucket.blob(object_name)
    local_path = '/tmp/' + object_name
    blob.download_to_filename(local_path)
    return local_path


def get_file_entries(input_file):
    with gzip.open(input_file, 'rt') as fh:
        for line in fh:
            yield json.loads(line)


def enrich_entry(entry):
    duration_ms = (entry['EdgeEndTimestamp'] - entry['EdgeStartTimestamp'])/1e6
    entry['DurationSeconds'] = duration_ms/1000
    entry['DurationMs'] = duration_ms

    parsed_uri = urllib.parse.urlparse('https://%s%s' % (
        entry['EdgeRequestHost'], entry['ClientRequestURI']))
    entry['Query'] = parsed_uri.query
    params = []
    for param, value in sorted(urllib.parse.parse_qsl(parsed_uri.query, keep_blank_values=True)):
        entry['Query_' + param] = value
        params.append(param)
    entry['QueryShape'] = '&'.join('%s=?' % param for param in params)
    entry['PathShape'] = parsed_uri.path
    if parsed_uri.query:
        entry['UriShape'] = entry['PathShape'] + '?' + entry['QueryShape']
    else:
        entry['UriShape'] = entry['PathShape']
