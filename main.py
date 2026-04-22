import json
import os
import time
import traceback

import requests
from flask import abort  # pylint: disable=import-error
from google.cloud import storage

from honeyflare import (
    create_otel_tracer,
    process_bucket_object,
    RetriableError,
    logfmt,
)

# Ignoring invalid names here due to all the globals we cache (which aren't necessarily
# constants)
# pylint: disable=invalid-name

storage_client = storage.Client()
# Not pretty, but doing this to bump the connection pool size to avoid constant warnings
# during concurrent executions from connections being discarded.
adapter = requests.adapters.HTTPAdapter(pool_maxsize=128)
storage_client._http.mount("https://", adapter)
storage_client._http._auth_request.session.mount("https://", adapter)


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

    meta_tracer, meta_provider = create_otel_tracer(
        service_name="honeyflare",
        honeycomb_api=honeycomb_api,
    )

    try:
        with meta_tracer.start_as_current_span("process-logfile") as meta_span:
            instrument_invocation(meta_span, event, context)

            start_time = time.time()
            try:
                if event["name"].startswith("ownership-challenge"):
                    meta_span.set_attribute("success", True)
                    return

                bucket = storage_client.bucket(event["bucket"])
                events_handled = process_bucket_object(
                    bucket,
                    event["name"],
                    honeycomb_api=honeycomb_api,
                    patterns=patterns,
                    query_param_filter=query_param_filter,
                    lock_bucket=lock_bucket,
                    sampling_rate_by_status=sampling_rate_by_status,
                )
                meta_span.set_attribute("events", events_handled)
                meta_span.set_attribute("success", True)
            except RetriableError as err:
                # Hard exit to make sure this is retried
                meta_span.set_attribute("success", False)
                meta_span.set_attribute("retriable", True)
                meta_span.set_attribute("error", err.__class__.__name__)
                meta_span.set_attribute("error_message", str(err))
                # To prevent the stacktrace from being logged on retries, abort instead of re-raising
                abort(500)
            except Exception as err:  # pylint: disable=broad-except
                # Swallow these but make sure they are logged and reported so that we can fix them
                traceback.print_exc()
                meta_span.set_attribute("success", False)
                meta_span.set_attribute("error", err.__class__.__name__)
                meta_span.set_attribute("error_message", str(err))
            finally:
                meta_span.set_attribute(
                    "duration_ms", (time.time() - start_time) * 1000
                )
                print(logfmt.format(dict(meta_span.attributes)))
    finally:
        meta_provider.shutdown()


def instrument_invocation(span, event, context):
    for event_key in ("name", "bucket", "contentType", "timeCreated", "size"):
        span.set_attribute("event.%s" % event_key, event[event_key])

    owner = event.get("owner")
    if owner:
        span.set_attribute("event.owner", owner.get("entityId"))

    for context_property in ("event_id", "timestamp", "event_type", "resource"):
        value = getattr(context, context_property, None)
        if value is None:
            continue
        if isinstance(value, (str, bool, int, float)):
            span.set_attribute("context.%s" % context_property, value)
        else:
            span.set_attribute("context.%s" % context_property, str(value))
