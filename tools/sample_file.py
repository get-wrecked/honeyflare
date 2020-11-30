#!./venv/bin/python

'''
This script applies the sampling and enrichment to a local file. Use this if you want to
test sampling strategies or performance locally.
'''

import argparse
import datetime
import json
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from honeyflare import compile_pattern, enrichment, get_raw_file_entries
from honeyflare.sampler import Sampler


def main():
    args = get_args()
    sampler = Sampler()
    start_time = time.time()
    source = get_raw_file_entries(args.file)
    compiled_patterns = [compile_pattern(p) for p in args.patterns]
    total_events = 0

    field_counts = defaultdict(int)

    for sample_rate, entry in sampler.sample_lines(source, args.sample_by_status, args.dynamic_sampling_fields):
        event = {}
        event['sample_rate'] = sample_rate
        enrichment.enrich_entry(entry, compiled_patterns, args.query_param_filter)
        event['data'] = entry
        raw_end = entry['EdgeEndTimestamp']/1e9
        event['created_at'] = datetime.datetime.utcfromtimestamp(raw_end).isoformat()
        if not args.no_out:
            args.output.write(json.dumps(event))
            args.output.write('\n')
        key = tuple(entry[key] for key in args.dynamic_sampling_fields)
        field_counts[key] += 1
        total_events += 1
    print('Wrote %d sampled events in %.2fs' % (total_events, time.time() - start_time))
    for key, count in sorted(field_counts.items(), key=lambda t: -t[1]):
        print('%s\t%d' % (key, count))


def get_args():
    def json_type(value):
        return json.loads(value)

    def csv_type(value):
        return value.split(',')

    parser = argparse.ArgumentParser()
    parser.add_argument('file');
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default=sys.stdout,
        help='Where to write the sampled output as a stream of json lines. Default is stdout.')
    parser.add_argument('-s', '--sample-by-status', type=json_type,
        help='Head sample rate by status as a json dict')
    parser.add_argument('-p', '--patterns', type=csv_type, default=[],
        help='Path patterns to extract as csv')
    parser.add_argument('-q', '--query-param-filter', type=csv_type,
        help='Query param filter as csv')
    parser.add_argument('-n', '--no-out', action='store_true',
        help='Disable output entirely. Useful if you only want to test performance '
        'without incurring any extra overhead')
    parser.add_argument('-d', '--dynamic-sampling-fields', type=csv_type, default=(),
        help='Fields to use for dynamic sampling')
    args = parser.parse_args()

    if args.sample_by_status:
        args.sample_by_status = {int(key): val for key, val in args.sample_by_status.items()}

    if args.dynamic_sampling_fields:
        args.dynamic_sampling_fields = tuple(args.dynamic_sampling_fields)

    return args


if __name__ == '__main__':
    main()
