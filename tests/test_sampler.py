from collections import defaultdict

from honeyflare.sampler import Sampler


def test_get_sampled_lines():
    lines = []
    for _ in range(200):
        lines.append('{"EdgeResponseStatus": 200}')
        lines.append('{"EdgeResponseStatus": 201}')
        lines.append('{"EdgeResponseStatus": 204}')
        lines.append('{"EdgeResponseStatus": 301}')
        lines.append('{"EdgeResponseStatus": 403}')
        lines.append('{"EdgeResponseStatus": 404}')
        lines.append('{"EdgeResponseStatus": 502}')
        lines.append('{"EdgeResponseStatus": 503}')

    sampler = Sampler()
    entries = list(t[1] for t in sampler.sample_lines(lines, {
        200: 2,
        201: 5,
        300: 6,
        400: 4,
        404: 0,
        500: 3,
        503: 1,
    }))

    lines_by_status = defaultdict(int)
    for entry in entries:
        lines_by_status[entry['EdgeResponseStatus']] += 1

    # Expected ranges computed with ./tools/expected-success.py 200 <sample rate>
    # to get a range that will pass the test in 99.99% of runs
    assert 72 < lines_by_status[200] < 128
    assert 17 < lines_by_status[201] < 63
    assert 11 < lines_by_status[301] < 55
    assert 39 < lines_by_status[502] < 93
    assert 25 < lines_by_status[403] < 75
    assert lines_by_status[404] == 0
    assert lines_by_status[503] == 200


def test_get_sample_default():
    lines = []
    for _ in range(10):
        lines.append('{"EdgeResponseStatus": 200}')
        lines.append('{"EdgeResponseStatus": 300}')

    sampler = Sampler()
    entries = list(t[1] for t in sampler.sample_lines(lines, {
        200: 1,
    }))

    lines_by_status = defaultdict(int)
    for entry in entries:
        lines_by_status[entry['EdgeResponseStatus']] += 1

    assert lines_by_status[200] == 10
    assert lines_by_status[300] == 10


def test_noop_head_sampling():
    lines = []
    for _ in range(50):
        lines.append('{"EdgeResponseStatus": 200}')

    sampler = Sampler()
    entries = list(sampler.sample_lines(lines, {}))

    assert len(entries) == 50
