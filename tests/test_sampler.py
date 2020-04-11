from collections import defaultdict

from honeyflare.sampler import Sampler


def test_get_dynamically_sampled_entries():
    entries = []
    for _ in range(200):
        entries.append({'EdgeResponseStatus': 200, 'EdgeEndTimestamp': 0})
        entries.append({'EdgeResponseStatus': 200, 'EdgeEndTimestamp': 0})
        entries.append({'EdgeResponseStatus': 200, 'EdgeEndTimestamp': 0})
        entries.append({'EdgeResponseStatus': 404, 'EdgeEndTimestamp': 0})
    entries.append({'EdgeResponseStatus': 503})

    sampler = Sampler()
    entries = list(t[1] for t in sampler.sample_events(entries, ['EdgeResponseStatus'])

    lines_by_status = defaultdict(int)
    for entry in entries:
        lines_by_status[entry['EdgeResponseStatus']] += 1

    # Out of 801 lines in the source there's 600 lines of 200, 200 lines of 404 and 1 503.
    # Logarithmically this
    assert lines_by_status[503] == 1


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


def test_noop_head_sampling():
    lines = []
    for _ in range(50):
        lines.append('{"EdgeResponseStatus": 200}')

    sampler = Sampler()
    entries = list(sampler.sample_lines(lines, {}))

    assert len(entries) == 50

# if __name__ == "__main__":
#     batch = []
#     batch_size = 100
#     for index, line in enumerate(get_raw_file_entries('test-data/short-sample-logs.gz')):
#         batch.append(json.loads(line))
#         if index % batch_size == 0: # TODO: This processes batch_size + 1 events
#             handle_batch_events(batch)
#             batch.clear()
#     if batch:
#         handle_batch_events(batch)

    # state = handle_batch({
    #     200: 530,
    #     404: 100,
    #     503: 30,
    #     400: 10,
    #     502: 4,
    #     408: 3,
    #     429: 1,
    # }, start_time=0, end_time=2)

    # TODO: Test lots of unique keys

    # state = handle_batch({
    #     200: 20,
    #     404: 20,
    #     503: 3,
    #     400: 1,
    #     502: 2,
    #     408: 1,
    # }, start_time=0, end_time=2, state=state)

    # handle_batch({
    #     200: 1500,
    #     404: 300,
    #     503: 100,
    #     429: 1,
    # }, start_time=0, end_time=6)
