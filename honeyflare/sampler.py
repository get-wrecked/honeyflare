import random
import re
import sys

import orjson


STATUS_CODE_RE = re.compile(r'"EdgeResponseStatus":\s?(\d{3})')
ORIGIN_RESPONSE_TIME_RE = re.compile(r'"OriginResponseTime":\s?(\d+)')


class Sampler:
    def sample_lines(self, line_iterator, head_sampling_rate_by_status):
        """
        Applies head sampling to a line-based iterator.
        """
        for line in line_iterator:
            # Use regex to extract status first to not incur the overhead of json
            # parsing on lines we'll skip
            sampling_rate = 1

            if head_sampling_rate_by_status:
                sampling_rate = sample_line_by_status(
                    line, head_sampling_rate_by_status
                )

            if sampling_rate == 0:
                continue

            if sampling_rate == 1:
                yield sampling_rate, orjson.loads(line)
                continue

            match = ORIGIN_RESPONSE_TIME_RE.search(line)
            response_time = int(match.group(1)) if match else 0

            # Treat any request slower than 1s as an error
            if response_time > 1e9:
                if head_sampling_rate_by_status:
                    sampling_rate = head_sampling_rate_by_status.get(500, 1)
                else:
                    sampling_rate = 1

            if random.randint(1, sampling_rate) == 1:
                yield sampling_rate, orjson.loads(line)


def sample_line_by_status(line, rate_by_status):
    match = STATUS_CODE_RE.search(line)
    if not match:
        # TODO: Instrument this somehow
        sys.stderr.write("Log line with missing status code: %s" % line)
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
