import gzip
import json


def process_bucket_object(bucket, object_name):
    pass


def get_file_entries(input_file):
    with gzip.open(input_file, 'rt') as fh:
        for line in fh:
            yield json.loads(line)
