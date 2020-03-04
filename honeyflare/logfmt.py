# Lifted from flask-events
import numbers
import re

from collections import OrderedDict


NEEDS_QUOTES_RE = re.compile(r'[\s=]')


def format(data):
    return ' '.join(format_key_value_pair(key, val) for (key, val) in data.items())


def format_key_value_pair(key, value):
    if value is None:
        value = ''
    elif value is True:
        value = 'true'
    elif value is False:
        value = 'false'
    elif isinstance(value, numbers.Integral):
        value = str(value)
    elif isinstance(value, numbers.Real):
        value = '%.4f' % value
    else:
        value = str(value)

    should_quote = NEEDS_QUOTES_RE.search(value)

    if should_quote:
        value = '"%s"' % value

    return '%s=%s' % (key, value)
