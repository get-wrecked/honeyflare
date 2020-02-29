import gzip
import json
import os

import libhoney

from .version import __version__
from .urlshape import compile_pattern, urlshape


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
    os.remove(local_path)


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


def enrich_entry(entry, path_patterns):
    '''
    :param entry: A dictionary with the log entry fields.
    :param path_patterns: A list of `.urlshape.Pattern` for known path patterns
        to parse.
    '''
    duration_ms = (entry['EdgeEndTimestamp'] - entry['EdgeStartTimestamp'])/1e6
    entry['DurationSeconds'] = duration_ms/1000
    entry['DurationMs'] = duration_ms

    url_shape = urlshape(entry['ClientRequestURI'], path_patterns)
    entry['Path'] = url_shape.path
    entry['PathShape'] = url_shape.path_shape
    entry['Query'] = url_shape.query
    entry['QueryShape'] = url_shape.query_shape
    entry['UriShape'] = url_shape.uri_shape
    for path_param, value in url_shape.path_params.items():
        entry['Path_' + path_param] = value
    for query_param, value in url_shape.query_params.items():
        entry['Query_' + query_param] = value
