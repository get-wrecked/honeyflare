import uuid

import pytest

from honeyflare import compile_pattern
from honeyflare.enrichment import enrich_entry


def test_enrich_entry():
    entry = {
        "ClientRequestURI": "/users/id1337?regularParam=paramValue&emptyParam=",
        "EdgeEndTimestamp": 1582850070117000000,
        "EdgeStartTimestamp": 1582850070112000000,
        "EdgeRequestHost": "example.com",
        "OriginResponseTime": 150 * 1e6,
        "RayID": "6f2de346beec9644",
        "ParentRayID": "00",
        "ClientRequestMethod": "POST",
    }

    patterns = [
        compile_pattern(p)
        for p in [
            "/other/thing/:thingId",
            "/users/:userId/pictures",
            "/users/:userId",
        ]
    ]
    enrich_entry(entry, patterns, None)

    assert entry["DurationSeconds"] == 0.005
    assert entry["DurationMs"] == 5
    assert entry["OriginResponseTimeSeconds"] == 0.15
    assert entry["OriginResponseTimeMs"] == 150
    assert entry["Path"] == "/users/id1337"
    assert entry["PathShape"] == "/users/:userId"
    assert entry["Path_userId"] == "id1337"
    assert entry["Query"] == "regularParam=paramValue&emptyParam="
    assert entry["QueryShape"] == "emptyParam=?&regularParam=?"
    assert entry["Query_emptyParam"] == ""
    assert entry["Query_regularParam"] == "paramValue"
    assert entry["UriShape"] == "/users/:userId?emptyParam=?&regularParam=?"
    assert entry["trace.trace_id"] == str(uuid.UUID("00000000000000006f2de346beec9644"))
    assert entry["trace.span_id"] == str(uuid.UUID("00000000000000006f2de346beec9644"))
    assert "trace.parent_id" not in entry
    assert entry["name"] == "HTTP POST"


def test_enrich_entry_with_parent():
    entry = {
        "RayID": "bbbbbbbbbbbbbbbb",
        "ParentRayID": "aaaaaaaaaaaaaaaa",
    }

    enrich_entry(entry, [], None)

    assert entry["trace.trace_id"] == str(uuid.UUID("0000000000000000aaaaaaaaaaaaaaaa"))
    assert entry["trace.span_id"] == str(uuid.UUID("0000000000000000bbbbbbbbbbbbbbbb"))
    assert entry["trace.parent_id"] == str(
        uuid.UUID("0000000000000000aaaaaaaaaaaaaaaa")
    )


def test_enrich_path_shape_explicit_trailing_slash():
    entry = {
        "ClientRequestURI": "/users/id1337/",
        "EdgeEndTimestamp": 1582850070117000000,
        "EdgeStartTimestamp": 1582850070112000000,
        "EdgeRequestHost": "example.com",
        "OriginResponseTime": 150 * 1e6,
        "RayId": "6f2de346beec9644",
        "ParentRayId": "00",
        "ClientRequestMethod": "POST",
    }

    patterns = [
        compile_pattern(p)
        for p in [
            "/users/:userId/",
        ]
    ]
    enrich_entry(entry, patterns, None)

    assert entry["Path"] == "/users/id1337/"
    assert entry["PathShape"] == "/users/:userId/"

    # Without the trailing slash it should no longer match

    entry["ClientRequestURI"] = "/users/id1337"
    enrich_entry(entry, patterns, None)

    assert entry["Path"] == "/users/id1337"
    assert entry["PathShape"] == "/users/id1337"


def test_enrich_path_shape_implicit_trailing_slash():
    entry = {
        "ClientRequestURI": "/users/id1337/",
        "EdgeEndTimestamp": 1582850070117000000,
        "EdgeStartTimestamp": 1582850070112000000,
        "EdgeRequestHost": "example.com",
        "OriginResponseTime": 150 * 1e6,
        "RayId": "6f2de346beec9644",
        "ParentRayId": "00",
        "ClientRequestMethod": "POST",
    }

    patterns = [
        compile_pattern(p)
        for p in [
            "/users/:userId",
        ]
    ]
    enrich_entry(entry, patterns, None)

    assert entry["Path"] == "/users/id1337/"
    assert entry["PathShape"] == "/users/:userId"


@pytest.mark.parametrize(
    "ip,expected_version",
    [("1.2.3.4", 4), ("2001:0db8:85a3:0000:0000:8a2e:0370:7334", 6)],
)
def test_enrich_client_ip(ip, expected_version):
    entry = {
        "ClientIP": ip,
    }

    enrich_entry(entry, [], None)

    assert entry["ClientIPVersion"] == expected_version
