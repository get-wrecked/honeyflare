import os
import base64
import unittest.mock as mock

import pytest
from google.cloud import storage

from honeyflare import locks
from honeyflare.exceptions import FileLockedException
from google.api_core.exceptions import NotFound


pytestmark = pytest.mark.integration


def test_locks(bucket, lock_name):
    assert locks.lock(bucket, lock_name) == True
    assert locks.lock(bucket, lock_name) == False


def test_unlock(bucket, lock_name):
    assert locks.lock(bucket, lock_name) == True
    locks.unlock(bucket, lock_name)
    assert locks.lock(bucket, lock_name) == True


def test_lock_object(bucket, lock_name):
    lock = locks.GCSLock(bucket, lock_name)
    with lock:
        other_lock = locks.GCSLock(bucket, lock_name)
        with pytest.raises(FileLockedException):
            with other_lock:
                pass


@pytest.fixture
def lock_name(bucket):
    lock_name = 'honeyflare-test-' + base64.urlsafe_b64encode(os.urandom(8)).decode('utf-8')
    try:
        yield lock_name
    finally:
        try:
            bucket.blob(lock_name).delete()
        except NotFound:
            pass
