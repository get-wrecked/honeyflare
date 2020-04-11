import json
import math
import random
import re
from collections import defaultdict

STATUS_CODE_RE = re.compile(r'"EdgeResponseStatus":\s?(\d{3})')


class Sampler():
    def __init__(self, alpha=0.12, target_events_by_second=10, batch_size=10):
        '''
        :param alpha: How much older values should be weighted. The lower the value the
            more old values will be part of the value. In the range (0, 1]. 1 is no
            history.
        '''
        if not (0 < alpha <= 1):
            raise ValueError('alpha must be in the range (0, 1] (was %.3f)' % alpha)

        if target_events_by_second < 1:
            raise ValueError('target_events_by_second must be a positive integer')

        if batch_size < 1:
            raise ValueError('batch_size must be a positive integer')

        self.alpha = alpha
        self.target_events_by_second = target_events_by_second
        self.batch_size = batch_size
        self.total = 0
        # TODO: This should be LRU or top heap
        self.key_distribution = {}
        self.inverse_alpha = 1 - alpha


    def sample_lines(self, line_iterator, head_sampling_rate_by_status):
        '''
        Applies head sampling to a line-based iterator.
        '''
        for index, (file_pos, line) in enumerate(line_iterator):
            # Use regex to extract status first to not incur the overhead of json
            # parsing on lines we'll skip
            sampling_rate = 1

            if head_sampling_rate_by_status:
                sampling_rate = sample_line_by_status(line, head_sampling_rate_by_status)

            if sampling_rate == 0:
                continue

            if sampling_rate == 1:
                yield sampling_rate, json.loads(line)
                continue

            if random.randint(1, sampling_rate) == 1:
                yield sampling_rate, json.loads(line)



    def sample_events(self, event_iterator, keys):
        batch = []
        for index, event in enumerate(event_iterator, 1):
            if not batch:
                batch_start_time = event['EdgeEndTimestamp']
            batch.append(event)
            if index % self.batch_size == 0:
                self._handle_batch_events(batch)
                batch.clear()
        if batch:
            self._handle_batch_events(batch, batch_start_time, )

        for key, value in batch_counts.items():
            state.add_key(key, value)

        events_to_send = (end_time - start_time) * target_events_by_second
        rates = state.get_rates(batch_counts, events_to_send)
        print('events to send: %d' % (events_to_send))

        for key, val in sorted(rates.items(), key=lambda t: -t[1]):
            print('%s: %s' % (key, val))

        print('Total events: %d' % sum(batch_counts[key]/rates[key] for key, val in rates.items()))
        return state


    # def _handle_batch_events(self, batch_counts, start_time, end_time, state=None):


    def add_key(self, key, counts):
        key_log = math.log(1 + counts)
        existing_log = self.key_distribution.get(key)
        if existing_log is not None:
            key_log = self.alpha*key_log + self.inverse_alpha*existing_log

        self.key_distribution[key] = key_log
        self.total += key_log


    def get_rates(self, batch_counts, events_to_send):
        rates = {}
        for key, count in batch_counts.items():
            dist_val = self.key_distribution[key]
            key_events = events_to_send*(dist_val/self.total)
            key_rate = math.floor(count/key_events)
            if key_rate == 0:
                key_rate = 1
            rates[key] = key_rate
        return rates


def sample_line_by_status(line, rate_by_status):
    match = STATUS_CODE_RE.search(line)
    if not match:
        # TODO: Instrument this somehow
        sys.stderr.write('Log line with missing status code: %s' % line)
        return 0

    status_code = int(match.group(1))
    direct_rate = rate_by_status.get(status_code)
    if direct_rate is not None:
        return direct_rate

    if status_code < 300:
        class_code = 200
    elif status_code < 400:
        class_code = 300
    elif status_code < 500:
        class_code = 400
    else:
        class_code = 500

    class_rate = rate_by_status.get(class_code)
    if class_rate is not None:
        return class_rate

    return 1
