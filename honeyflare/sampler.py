import random
import re
import sys
import math

import orjson
import cachetools


STATUS_CODE_RE = re.compile(r'"EdgeResponseStatus":\s?(\d{3})')
ORIGIN_RESPONSE_TIME_RE = re.compile(r'"OriginResponseTime":\s?(\d+)')


class Sampler():
    def sample_lines(self, line_iterator, head_sampling_rate_by_status, dynamic_sampling_fields):
        '''
        Applies head sampling to a line-based iterator.
        '''
        key_counts = cachetools.LFUCache(250)
        key_rates = cachetools.LFUCache(250)

        def update_rates():
            total_events = sum(key_counts.values())
            total_log = sum(math.log(v) for v in key_counts.values())
            target_rate = total_events/50
            target_ratio = target_rate/total_log
            key_rates.clear()
            for key, val in key_counts.items():
                key_goal = int(max(1, math.log(val) * target_ratio))
                key_rates[key] = key_goal

            # import pdb; pdb.set_trace()
            key_counts.clear()



        for line_count, line in enumerate(line_iterator, 1):
            # Use regex to extract status first to not incur the overhead of json
            # parsing on lines we'll skip
            sampling_rate = 1

            if head_sampling_rate_by_status:
                sampling_rate = sample_line_by_status(line, head_sampling_rate_by_status)

            if sampling_rate == 0:
                continue

            # if sampling_rate == 1:
            #     yield sampling_rate, orjson.loads(line)
            #     continue


            match = ORIGIN_RESPONSE_TIME_RE.search(line)
            response_time = int(match.group(1)) if match else 0

            # Treat any request slower than 1s as an error
            if response_time > 1e9:
                if head_sampling_rate_by_status:
                    sampling_rate = head_sampling_rate_by_status.get(500, 1)
                else:
                    sampling_rate = 1

            data = orjson.loads(line)
            key = tuple(data[k] for k in dynamic_sampling_fields)
            key_count = key_counts.get(key, 0)
            key_counts[key] = key_count + 1
            sampling_rate = key_rates.get(key, 1)

            if line_count % 1000 == 0:
                update_rates()

            if random.randint(1, sampling_rate) == 1:
                yield sampling_rate, data



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
