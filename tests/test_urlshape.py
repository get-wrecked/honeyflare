import pytest

from honeyflare.urlshape import compile_pattern, urlshape, UrlShape


@pytest.mark.parametrize('uri,pattern,query_params_filter,expected', [
    ('/foo', '/:first', None, UrlShape(
        '/:first',
        '/foo',
        '',
        '/:first',
        '',
        {'first': 'foo'},
        {},
    )),
    ('/foo?key=val', '/other', None, UrlShape(
        '/foo?key=?',
        '/foo',
        'key=val',
        '/foo',
        'key=?',
        {},
        {'key': 'val'},
    )),
    ('/foo?key=val', '/other', set(), UrlShape(
        '/foo?key=?',
        '/foo',
        'key=val',
        '/foo',
        'key=?',
        {},
        {},
    )),
    ('/foo?key=val&filtered=ignore', '/other', set(['key']), UrlShape(
        '/foo?filtered=?&key=?',
        '/foo',
        'key=val&filtered=ignore',
        '/foo',
        'filtered=?&key=?',
        {},
        {'key': 'val'},
    )),
    ('/user/id1337/pictures?key=val', '/user/:userId/*', None, UrlShape(
        '/user/:userId/*?key=?',
        '/user/id1337/pictures',
        'key=val',
        '/user/:userId/*',
        'key=?',
        {'userId': 'id1337'},
        {'key': 'val'},
    )),
])
def test_urlshape(uri, pattern, query_params_filter, expected):
    ret = urlshape(uri, [compile_pattern(pattern)], query_params_filter)
    assert ret == expected
