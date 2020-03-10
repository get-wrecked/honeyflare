import base64
import os
import sys
import urllib.parse
import tempfile
import time
import traceback
from pathlib import PurePosixPath

import hvac
from google.cloud import storage

from honeyflare import create_libhoney_client, process_bucket_object, RetriableError, logfmt

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


def get_vault_secret(vault_url):
    '''
    Fetch honeycomb API token from Vault.

    The url should be on the form
        vault://<host>:<port>/<mount-point>/<secret-path>?key=<lookup-key>&ca=<urlsafe-b64-ca-cert>[&https=false]
    '''
    parsed_url = urllib.parse.urlparse(vault_url)
    parsed_parameters = urllib.parse.parse_qs(parsed_url.query)

    scheme = 'https'
    https = parsed_parameters.get('https')
    if https == 'false':
        scheme = 'http'

    b64_ca_cert = parsed_parameters.get('ca')
    if b64_ca_cert:
        ca_cert = base64.urlsafe_b64decode(b64_ca_cert[0].encode('utf-8'))
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(ca_cert)
            os.environ['REQUESTS_CA_BUNDLE'] = fh.name

    vault_url = '%s://%s' % (scheme, parsed_url.netloc)
    client = hvac.Client(url=vault_url)
    client.gcp.login()

    parsed_path = PurePosixPath(parsed_url.path)

    honeycomb_key = client.secrets.kv.read_secret_version(
        path='/'.join(parsed_path.parts[2:]),
        mount_point=parsed_path.parts[1],
    )
    key = parsed_parameters['key'][0]
    return honeycomb_key['data']['data'][key]


def main(event, context):
    '''
    Triggered by a change to a Cloud Storage bucket.

    :param event: Event payload (dict).
    :param context: Metadata for the event (google.cloud.functions.Context)
    '''
    global honeycomb_key
    if honeycomb_key.startswith('vault://'):
        honeycomb_key = get_vault_secret(honeycomb_key)
    meta_client = create_libhoney_client(honeycomb_dataset, honeycomb_key)
    meta_event = meta_client.new_event()
    instrument_invocation(meta_event, event, context)

    start_time = time.time()
    try:
        bucket = storage_client.bucket(event['bucket'])
        process_bucket_object(bucket, event['name'], honeycomb_dataset, honeycomb_key)
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


def instrument_invocation(libhoney_event, event, context):
    for event_key in ('name', 'bucket', 'contentType', 'timeCreated', 'size'):
        libhoney_event.add_field('event.%s' % event_key, event[event_key])

    owner = event.get('owner')
    if owner:
        libhoney_event.add_field('event.owner', owner.get('entityId'))

    for context_property in ('event_id', 'timestamp', 'event_type', 'resource'):
        value = getattr(context, context_property, None)
        libhoney_event.add_field('context.%s' % context_property, value)
