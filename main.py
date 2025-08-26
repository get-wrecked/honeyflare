import json
import os
import time
import traceback
import uuid

# We don't use flask ourself, but it's used as the runtime by GCP and thus
# usable by us
from flask import abort  # pylint: disable=import-error
from google.cloud import storage

from honeyflare import (
    create_libhoney_client,
    process_bucket_object,
    RetriableError,
    logfmt,
    vault,
)

# Ignoring invalid names here due to all the globals we cache (which aren't necessarily
# constants)
# pylint: disable=invalid-name

storage_client = storage.Client()

# Check for required envvars to fail early on invalid deployments
honeycomb_dataset = os.environ.get("HONEYCOMB_DATASET")
if honeycomb_dataset is None:
    raise ValueError("Missing environment variable HONEYCOMB_DATASET")

honeycomb_meta_dataset = os.environ.get("HONEYCOMB_META_DATASET")
if honeycomb_meta_dataset is None:
    raise ValueError("Missing environment variable HONEYCOMB_META_DATASET")

honeycomb_key = os.environ.get("HONEYCOMB_KEY")
if honeycomb_key is None:
    raise ValueError("Missing environment variable HONEYCOMB_KEY")

if honeycomb_key.startswith("vault://"):
    vault_start_time = time.time()
    honeycomb_key = vault.get_vault_secret(honeycomb_key)
    print("Vault key lookup finished in %.2fs" % (time.time() - vault_start_time))

patterns = os.environ.get("PATTERNS")
if patterns is not None:
    patterns = json.loads(patterns)

honeycomb_api = os.environ.get("HONEYCOMB_API", "https://api.honeycomb.io")

query_param_filter = os.environ.get("QUERY_PARAM_FILTER")
if query_param_filter is not None:
    query_param_filter = set(json.loads(query_param_filter))

lock_bucket = os.environ.get("LOCK_BUCKET")
if lock_bucket is not None:
    lock_bucket = storage_client.bucket(lock_bucket)

# Convert string keys (the only kind permitted by json) to ints
sampling_rate_by_status = {
    int(key): val
    for key, val in json.loads(os.environ.get("SAMPLING_RATES", "{}")).items()
}


def main(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.

    :param event: Event payload (dict).
    :param context: Metadata for the event (google.cloud.functions.Context)
    """

    meta_client = create_libhoney_client(
        honeycomb_key, honeycomb_meta_dataset, honeycomb_api
    )
    meta_event = meta_client.new_event()
    instrument_invocation(meta_event, event, context)

    start_time = time.time()
    try:
        if event["name"].startswith("ownership-challenge"):
            meta_event.add_field("success", True)
            return

        bucket = storage_client.bucket(event["bucket"])
        events_handled = process_bucket_object(
            bucket,
            event["name"],
            honeycomb_dataset,
            honeycomb_key,
            honeycomb_api,
            patterns=patterns,
            query_param_filter=query_param_filter,
            lock_bucket=lock_bucket,
            sampling_rate_by_status=sampling_rate_by_status,
        )
        meta_event.add_field("events", events_handled)
        meta_event.add_field("success", True)
    except RetriableError as err:
        # Hard exit to make sure this is retried
        meta_event.add_field("success", False)
        meta_event.add_field("retriable", True)
        meta_event.add_field("error", err.__class__.__name__)
        meta_event.add_field("error_message", str(err))
        # To prevent the stacktrace from being logged on retries, abort instead of re-raising
        abort(500)
    except Exception as err:  # pylint: disable=broad-except
        # Swallow these but make sure they are logged and reported so that we can fix them
        traceback.print_exc()
        meta_event.add_field("success", False)
        meta_event.add_field("error", err.__class__.__name__)
        meta_event.add_field("error_message", str(err))
    finally:
        meta_event.add_field("duration_ms", (time.time() - start_time) * 1000)
        print(logfmt.format(meta_event.fields()))
        meta_event.send()
        meta_client.close()


def instrument_invocation(libhoney_event, event, context):
    for event_key in ("name", "bucket", "contentType", "timeCreated", "size"):
        libhoney_event.add_field("event.%s" % event_key, event[event_key])

    owner = event.get("owner")
    if owner:
        libhoney_event.add_field("event.owner", owner.get("entityId"))

    for context_property in ("event_id", "timestamp", "event_type", "resource"):
        value = getattr(context, context_property, None)
        libhoney_event.add_field("context.%s" % context_property, value)

    libhoney_event.add_field("trace.trace_id", str(uuid.uuid4()))
    libhoney_event.add_field("trace.span_id", str(uuid.uuid4()))
    libhoney_event.add_field("service.name", "honeyflare")
    libhoney_event.add_field("name", "process-logfile")
