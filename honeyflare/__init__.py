import datetime
import gzip
import json
import os
import random
import re
import sys
import threading

import libhoney
from google.api_core.exceptions import PreconditionFailed
from urllib3.exceptions import HTTPError

from . import enrichment
from .exceptions import RetriableError
from .locks import GCSLock
from .sampler import Sampler
from .urlshape import compile_pattern
from .version import __version__


def process_bucket_object(
    bucket,
    object_name,
    honeycomb_dataset,
    honeycomb_key,
    honeycomb_api="https://api.honeycomb.io",
    patterns=None,
    query_param_filter=None,
    sampling_rate_by_status=None,
    lock_bucket=None,
):
    """
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
    """
    if sampling_rate_by_status is None:
        sampling_rate_by_status = {}

    if lock_bucket is None:
        lock_bucket = bucket

    compiled_patterns = [compile_pattern(p) for p in patterns or []]
    libhoney_client = create_libhoney_client(
        honeycomb_key, honeycomb_dataset, honeycomb_api
    )

    lock = GCSLock(lock_bucket, "locks/%s" % object_name)
    total_events = 0
    with lock:
        if is_already_processed(lock_bucket, object_name):
            # We might have been retried due to a failure but another
            # function succeeded in the meantime
            return

        local_path = download_file(bucket, object_name)

        sampler = Sampler()
        source = get_raw_file_entries(local_path)
        for sample_rate, entry in sampler.sample_lines(source, sampling_rate_by_status):
            event = libhoney_client.new_event()
            event.sample_rate = sample_rate
            enrichment.enrich_entry(entry, compiled_patterns, query_param_filter)
            event.add(entry)
            event.created_at = datetime.datetime.utcfromtimestamp(
                entry["EdgeEndTimestamp"] / 1e9
            )
            event.send_presampled()
            total_events += 1

        libhoney_client.close()
        os.remove(local_path)
        mark_as_processed(lock_bucket, object_name)
    return total_events


def create_libhoney_client(writekey, dataset, honeycomb_api):
    client = libhoney.Client(
        writekey=writekey,
        dataset=dataset,
        block_on_send=True,
        user_agent_addition="honeyflare/%s" % __version__,
        api_host=honeycomb_api,
    )
    client.add_field("MetaProcessor", "honeyflare/%s" % __version__)

    thread = threading.Thread(
        target=read_honeycomb_responses, args=(client.responses(), dataset)
    )
    thread.start()

    return client


def read_honeycomb_responses(resp_queue, dataset):
    """Log failing responses from honeycomb"""
    while True:
        resp = resp_queue.get()
        if resp is None:
            # The client will enqueue a None value after we call client.close()
            break

        if resp["status_code"] > 400:
            sys.stderr.write(
                "Got %d from honeycomb when submitting to %s: %s\n"
                % (resp["status_code"], dataset, resp["error"])
            )


def is_already_processed(lock_bucket, object_name):
    return _processed_blob(lock_bucket, object_name).exists()


def mark_as_processed(lock_bucket, object_name):
    blob = _processed_blob(lock_bucket, object_name)
    try:
        blob.upload_from_string(b"")
    except PreconditionFailed:
        # Another invocation has already processed this but it failed to be
        # caught in the lock. Ignore
        pass


def _processed_blob(bucket, object_name):
    return bucket.blob("completed/%s" % object_name)


def download_file(bucket, object_name):
    blob = bucket.blob(object_name)
    local_path = "/tmp/" + os.path.basename(object_name)
    try:
        blob.download_to_filename(local_path, raw_download=True)
    except HTTPError as e:
        raise RetriableError() from e
    return local_path


def get_raw_file_entries(input_file):
    with gzip.open(input_file, "rt") as fh:
        yield from fh
