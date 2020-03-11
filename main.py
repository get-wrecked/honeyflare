import json
import os
import sys
import time
import traceback

from google.cloud import storage

from honeyflare import (
    create_libhoney_client,
    process_bucket_object,
    RetriableError,
    logfmt,
    vault,
)

storage_client = storage.Client()

# Check for required envvars to fail early on invalid deployments
honeycomb_dataset = os.environ.get('HONEYCOMB_DATASET')
if honeycomb_dataset is None:
    raise ValueError('Missing environment variable HONEYCOMB_DATASET')

honeycomb_meta_dataset = os.environ.get('HONEYCOMB_META_DATASET')
if honeycomb_meta_dataset is None:
    raise ValueError('Missing environment variable HONEYCOMB_META_DATASET')

honeycomb_key = os.environ.get('HONEYCOMB_KEY')
if honeycomb_key is None:
    raise ValueError('Missing environment variable HONEYCOMB_KEY')

# Convert string keys (the only kind permitted by json) to ints
sampling_rate_by_status = {int(key): val for key, val in json.loads(os.environ.get('SAMPLING_RATES', '{}')).items()}
lock_bucket = os.environ.get('LOCK_BUCKET')


def main(event, context):
    '''
    Triggered by a change to a Cloud Storage bucket.

    :param event: Event payload (dict).
    :param context: Metadata for the event (google.cloud.functions.Context)
    '''
    global honeycomb_key, lock_bucket
    meta_client = create_libhoney_client(honeycomb_key, honeycomb_dataset)
    meta_event = meta_client.new_event()
    instrument_invocation(meta_event, event, context)

    if lock_bucket:
        lock_bucket = storage_client.bucket(lock_bucket)

    start_time = time.time()
    try:
        if honeycomb_key.startswith('vault://'):
            honeycomb_key = vault.get_vault_secret(honeycomb_key)

        bucket = storage_client.bucket(event['bucket'])
        events_handled = process_bucket_object(
            bucket,
            event['name'],
            honeycomb_dataset,
            honeycomb_key,
            lock_bucket=lock_bucket,
            sampling_rate_by_status=sampling_rate_by_status,
        )
        meta_event.add_field('events', events_handled)
    except RetriableError as e:
        # Hard exit to make sure this is retried
        meta_event.add_field('error', e.__class__.__name__)
        meta_event.add_field('error_message', str(e))
        sys.exit(1)
    except Exception as e:
        # Swallow these but make sure they are logged and reported so that we can fix them
        traceback.print_exc()
        meta_event.add_field('error', e.__class__.__name__)
        meta_event.add_field('error_message', str(e))
    finally:
        meta_event.add_field('processing_time_seconds', time.time() - start_time)
        print(logfmt.format(meta_event.fields()))
        meta_event.send()
        meta_client.close()


def instrument_invocation(libhoney_event, event, context):
    for event_key in ('name', 'bucket', 'contentType', 'timeCreated', 'size'):
        libhoney_event.add_field('event.%s' % event_key, event[event_key])

    owner = event.get('owner')
    if owner:
        libhoney_event.add_field('event.owner', owner.get('entityId'))

    for context_property in ('event_id', 'timestamp', 'event_type', 'resource'):
        value = getattr(context, context_property, None)
        libhoney_event.add_field('context.%s' % context_property, value)
