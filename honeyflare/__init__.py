import gzip
import json
import os
import random
import re
import sys

import libhoney
from google.api_core.exceptions import PreconditionFailed

from .locks import GCSLock
from .urlshape import compile_pattern, urlshape
from .version import __version__


STATUS_CODE_RE = re.compile(r'"EdgeResponseStatus":\s?(\d{3})')


def process_bucket_object(
        bucket,
        object_name,
        honeycomb_dataset,
        honeycomb_key,
        patterns=None,
        query_param_filter=None,
        sampling_rate_by_status=None,
        lock_bucket=None,
        ):
    '''
    :param bucket: A `google.cloud.storage.bucket.Bucket` logs should be
        downloaded from.
    :param object_name: The name of the object in the bucket to download.
    :param honeycomb_dataset: The name of the honeycomb dataset to write to.
    :param honeycomb_key: The honeycomb API key.
    :param patterns: A list of path patterns to match against.
    :param query_param_filter: A set of query parameters to allow. If None, all
        will be allowed. If empty, none.
    :param sampling_rate_by_status: A dictionary mapping a status code to a
        sampling rate. Ie {200: 10, 400: 1}. A specific match will be checked
        first (ie {404: 10}), then the general class of code (ie 400 for a 404).
    :param lock_bucket: If you want to use a dedicated bucket for holding locks
        and completion status, pass it here. Otherwise the bucket that holds the
        logs will be used (requires write access to that bucket).
    '''
    if sampling_rate_by_status is None:
        sampling_rate_by_status = {}

    if lock_bucket is None:
        lock_bucket = bucket

    compiled_patterns = [compile_pattern(p) for p in patterns or []]
    libhoney_client = create_libhoney_client(honeycomb_key, honeycomb_dataset)

    lock = GCSLock(lock_bucket, 'locks/%s' % object_name)
    with lock:
        if is_already_processed(lock_bucket, object_name):
            # We might have been retried due to a failure but another
            # function succeeded in the meantime
            return

        local_path = download_file(bucket, object_name)

        for sample_rate, entry in get_sampled_file_entries(local_path, sampling_rate_by_status):
            event = libhoney_client.new_event()
            event.sample_rate = sample_rate
            enrich_entry(entry, compiled_patterns, query_param_filter)
            event.add(entry)
            event.send_presampled()

        libhoney_client.close()
        os.remove(local_path)
        mark_as_processed(lock_bucket, object_name)



def create_libhoney_client(writekey, dataset):
    client = libhoney.Client(
        writekey=writekey,
        dataset=dataset,
        block_on_send=True,
        user_agent_addition='honeyflare/%s' % __version__,
    )
    client.add_field('MetaProcessor', 'honeyflare/%s' % __version__)
    return client


def is_already_processed(lock_bucket, object_name):
    return _processed_blob(lock_bucket, object_name).exists()


def mark_as_processed(lock_bucket, object_name):
    blob = _processed_blob(lock_bucket, object_name)
    try:
        blob.upload_from_string(b'')
    except PreconditionFailed:
        # Another invocation has already processed this but it failed to be
        # caught in the lock. Ignore
        pass


def _processed_blob(bucket, object_name):
    return bucket.blob('completed/%s' % object_name)


def download_file(bucket, object_name):
    blob = bucket.blob(object_name)
    local_path = '/tmp/' + object_name
    blob.download_to_filename(local_path)
    return local_path


def get_sampled_file_entries(input_file, sampling_rate_by_status):
    for raw_entry in get_raw_file_entries(input_file):
        # Use regex to extract status first to not incur the overhead of json
        # parsing on lines we'll skip
        match = STATUS_CODE_RE.search(raw_entry)
        if not match:
            sys.stderr.write('Log line with missing status code: %s' % raw_entry)
            continue

        status_code = int(match.group(1))
        sampling_rate = 1
        direct_rate = sampling_rate_by_status.get(status_code)
        if direct_rate is not None:
            sampling_rate = direct_rate
        else:
            if status_code < 300:
                class_code = 200
            elif status_code < 400:
                class_code = 300
            elif status_code < 500:
                class_code = 400
            else:
                class_code = 500
            class_rate = sampling_rate_by_status.get(class_code)
            if class_rate is not None:
                sampling_rate = class_rate

        if sampling_rate == 0:
            continue

        if sampling_rate == 1:
            yield sampling_rate, json.loads(raw_entry)
            continue

        if random.randint(1, sampling_rate) == 1:
            yield sampling_rate, json.loads(raw_entry)


def get_raw_file_entries(input_file):
    with gzip.open(input_file, 'rt') as fh:
        yield from fh


def enrich_entry(entry, path_patterns, query_param_filter):
    '''
    :param entry: A dictionary with the log entry fields.
    :param path_patterns: A list of `.urlshape.Pattern` for known path patterns
        to parse.
    '''
    duration_ms = (entry['EdgeEndTimestamp'] - entry['EdgeStartTimestamp'])/1e6
    entry['DurationSeconds'] = duration_ms/1000
    entry['DurationMs'] = duration_ms

    url_shape = urlshape(entry['ClientRequestURI'], path_patterns, query_param_filter)
    entry['Path'] = url_shape.path
    entry['PathShape'] = url_shape.path_shape
    entry['Query'] = url_shape.query
    entry['QueryShape'] = url_shape.query_shape
    entry['UriShape'] = url_shape.uri_shape
    for path_param, value in url_shape.path_params.items():
        entry['Path_' + path_param] = value
    for query_param, value in url_shape.query_params.items():
        entry['Query_' + query_param] = value
