import re
import urllib.parse
from collections import namedtuple
from pathlib import PurePosixPath


Pattern = namedtuple('Pattern', 'shape regex')
UrlShape = namedtuple('UrlShape', [
    'uri_shape',
    'path',
    'query',
    'path_shape',
    'query_shape',
    'path_params',
    'query_params',
])


def compile_pattern(path_pattern):
    '''
    :param path_patterns: A list of patterns like `/user/:userId/`
    :return: A list of `Pattern`s which canbe passed to `urlshape` later.
    '''
    regex_parts = []
    path = PurePosixPath(path_pattern)
    for part in path.parts[1:]:
        if part.startswith(':'):
            regex_parts.append(r'/(?P<%s>[^/]+)' % part[1:])
        else:
            regex_parts.append('/' + part)

    if path_pattern.endswith('*'):
        regex_parts.append('.*')

    regex = re.compile('^' + ''.join(regex_parts) + '$')
    return Pattern(path_pattern, regex)


def urlshape(uri, patterns, query_params_filter=None):
    '''
    :param uri: A relative uri to be parsed.
    :param patterns: A list of `Pattern` to match against.
    :param query_params_filter: A set of the query parameters that will be included
        in the result. If `None` all params will be included, if empty set none
        will be included.
    :returns: A UrlShape
    '''
    parsed_uri = urllib.parse.urlparse('s://' + uri)
    path_params = {}
    query_params = {}
    for pattern in patterns:
        match = pattern.regex.match(parsed_uri.path)
        if not match:
            continue
        path_shape = pattern.shape
        path_params = match.groupdict()
        break
    else:
        path_shape = parsed_uri.path

    params = []
    for param, value in sorted(urllib.parse.parse_qsl(parsed_uri.query, keep_blank_values=True)):
        if query_params_filter is None or param in query_params_filter:
            query_params[param] = value
        params.append(param)

    query_shape = '&'.join('%s=?' % param for param in params)

    if query_shape:
        uri_shape = path_shape + '?' + query_shape
    else:
        uri_shape = path_shape

    return UrlShape(
        uri_shape,
        parsed_uri.path,
        parsed_uri.query,
        path_shape,
        query_shape,
        path_params,
        query_params,
    )
