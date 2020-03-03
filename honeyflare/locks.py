from io import BytesIO
from datetime import datetime, timezone

from google.cloud import storage
from google.api_core.exceptions import PreconditionFailed, NotFound

from .exceptions import FileLockedException


# Patch generation match preconditions onto the requests since the python
# libraries doesn't expose this functionality
# Ref. https://github.com/googleapis/google-cloud-python/issues/4490
storage.blob._MULTIPART_URL_TEMPLATE += '&ifGenerationMatch=0'
storage.blob._RESUMABLE_URL_TEMPLATE += '&ifGenerationMatch=0'

EMPTY_FILEOBJ = BytesIO()
# This is the maximum support timeout by Google Cloud Functions
MAX_EXECUTION_TIME_SECONDS = 540


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
        return True
    except PreconditionFailed:
        # Check that the lock was created recently (ie the creator function
        # might still be running)
        blob.reload()
        lock_age = datetime.now(timezone.utc) - blob.time_created
        if lock_age.total_seconds() > MAX_EXECUTION_TIME_SECONDS:
            try:
                blob.delete()
            except NotFound:
                # A parallel function might have just deleted the lock
                pass

            # Retry acuiring the lock
            try:
                blob.upload_from_file(EMPTY_FILEOBJ)
                return True
            except PreconditionFailed:
                pass

    return False


def unlock(bucket, lock_name):
    blob = bucket.blob(lock_name)
    try:
        blob.delete()
    except NotFound:
        # This can happen if another function thinks we have timed out but we
        # are for some reason still running (which can happen if the instance
        # that timed out is scheduled for more work after timing out for the
        # first request) At this point both functions will
        # have submitted the data, not much we can do to recover.
        pass
