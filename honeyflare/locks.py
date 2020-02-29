from io import BytesIO

from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed

from .exceptions import FileLockedException


# Patch generation match preconditions onto the requests since the python
# libraries doesn't expose this functionality
# Ref. https://github.com/googleapis/google-cloud-python/issues/4490
storage.blob._MULTIPART_URL_TEMPLATE += '&ifGenerationMatch=0'
storage.blob._RESUMABLE_URL_TEMPLATE += '&ifGenerationMatch=0'

EMPTY_FILEOBJ = BytesIO()


class GCSLock():
    def __init__(self, bucket, lock_name):
        self.bucket = bucket
        self.lock_name = lock_name


    def __enter__(self):
        if not lock(self.bucket, self.lock_name):
            raise FileLockedException()
        return self


    def __exit__(self, *args):
        unlock(self.bucket, self.lock_name)


def lock(bucket, lock_name):
    blob = bucket.blob(lock_name)
    try:
        blob.upload_from_file(EMPTY_FILEOBJ)
    except PreconditionFailed:
        return False
    return True


def unlock(bucket, lock_name):
    blob = bucket.blob(lock_name)
    blob.delete()
