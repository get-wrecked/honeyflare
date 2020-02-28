from google.cloud import storage

from honeyflare import process_bucket_object

storage_client = storage.Client()
bucket = storage_client.bucket(os.environ['HONEYFLARE_BUCKET'])


def main(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    process_bucket_object(bucket, event['name'])
