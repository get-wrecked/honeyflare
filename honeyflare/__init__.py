import gzip
import json

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
    libhoney_client = libhoney.Client(
        writekey=honeycomb_key,
        dataset=honeycomb_dataset,
        block_on_send=True,
        user_agent_addition='honeyflare/%s' % __version__,
    )
    for entry in get_file_entries(local_path):
        event = libhoney_client.new_event()
        event.add(entry)
        event.send()

    libhoney_client.close()


def download_file(bucket, object_name):
    blob = bucket.blob(object_name)
    local_path = '/tmp/' + object_name
    blob.download_to_filename(local_path)
    return local_path


def get_file_entries(input_file):
    with gzip.open(input_file, 'rt') as fh:
        for line in fh:
            yield json.loads(line)
