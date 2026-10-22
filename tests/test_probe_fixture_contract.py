from __future__ import annotations

import copy
import gzip
import json

import pytest
from probe_fixtures import (
    SCENARIOS,
    encoded_body,
    event_fixture,
    fixture_identity,
    observation_inventory,
    respond,
)

FROZEN_IDENTITIES = {
    "balanced": "9a888c5021fc538a230ec0a548c4c18cb7d4e326531ed3982d02164955245135",
    "many-small": "b86f83abcdceaf63d10dbc9a54596feb9157f1884649964ef13596040d8ac50d",
    "empty": "81270ef8610665b8726127de3da15f547fe19786a1ea5fa6c988786a48a466eb",
    "skew": "f5f158552c8885e65cf21144433b3e50adb80b3c60f8df65d202264cbc8e5c43",
    "dominant": "661a6ac5d847f05b1c39cfe28608aefd42232ed0f12c98d2abca1ca1cab6fbc0",
    "duplicates": "31acd936dc666326aeaf83fb594116cd2b2abd8b9d92119b53bb270afd97076c",
    "wide": "bb78867fd5017fee3a7a1b595ba61664efc1f4d8b8b37ba345c259050e55cec1",
}


def _all_pages(fixture, endpoint, params=None):
    rows = []
    cursor = ""
    seen = set()
    while True:
        status, payload = respond(fixture, endpoint, {**(params or {}), "cursor": cursor})
        assert status == 200
        assert len(payload["events"]) <= fixture["page_size"]
        rows.extend(payload["events"])
        cursor = payload["cursor"]
        if not cursor:
            return rows
        assert cursor not in seen
        seen.add(cursor)


@pytest.mark.parametrize("name", SCENARIOS)
def test_fixture_identity_and_global_partition_observations_are_frozen(name):
    fixture = event_fixture(name)
    assert fixture == event_fixture(name)
    assert fixture_identity(fixture) == FROZEN_IDENTITIES[name]
    assert fixture_identity(fixture) == fixture_identity(copy.deepcopy(fixture))
    partition_rows = []
    for partition in fixture["partitions"]:
        ordinary = partition["kind"] == "series"
        endpoint = "/events" if ordinary else "/events/multivariate"
        parameter = "series_ticker" if ordinary else "collection_ticker"
        rows = _all_pages(fixture, endpoint, {parameter: partition["ticker"], "limit": 200})
        assert rows == partition["events"]
        assert all(row["series_ticker"] == partition["series_ticker"] for row in rows)
        assert all("markets" not in row for row in rows)
        partition_rows.extend(rows)
    global_rows = _all_pages(fixture, "/events") + _all_pages(fixture, "/events/multivariate")
    assert observation_inventory(global_rows) == observation_inventory(partition_rows)


def test_empty_scopes_and_exact_page_boundary_have_a_terminal_cursor():
    fixture = event_fixture("empty")
    assert respond(fixture, "/events", {"series_ticker": "SERIES-0"}) == (
        200,
        {"events": [], "cursor": ""},
    )
    fixture = event_fixture("balanced")
    _, first = respond(fixture, "/events", {"series_ticker": "SERIES-0"})
    assert first["cursor"] == "offset-2"
    _, last = respond(fixture, "/events", {"series_ticker": "SERIES-0", "cursor": first["cursor"]})
    assert len(last["events"]) == 2
    assert last["cursor"] == ""


def test_duplicates_and_corrected_payloads_remain_distinct_observations():
    fixture = event_fixture("duplicates")
    rows = _all_pages(fixture, "/events", {"series_ticker": "SERIES-0"})
    assert len(rows) == 6
    inventory = observation_inventory(rows)
    assert sum(count for _, _, count in inventory) == 6
    repeated = [(checksum, count) for ticker, checksum, count in inventory if ticker == "EVENT-0-0"]
    assert sorted(count for _, count in repeated) == [1, 2]
    assert len({checksum for checksum, _ in repeated}) == 2
    assert observation_inventory(rows) != observation_inventory(
        {row["event_ticker"]: row for row in rows}.values()
    )


def test_fixture_shapes_expose_balancing_and_indivisible_hotspot_limits():
    balanced = event_fixture("balanced")["partitions"]
    assert {len(partition["events"]) for partition in balanced} == {4}
    small = event_fixture("many-small")["partitions"]
    assert len(small) == 8
    assert all(len(partition["events"]) == 1 for partition in small)
    dominant = [len(partition["events"]) for partition in event_fixture("dominant")["partitions"]]
    assert max(dominant) / sum(dominant) > 0.9
    assert 0 in dominant
    assert (
        event_fixture("dominant")["partitions"][dominant.index(max(dominant))]["kind"]
        == "collection"
    )
    assert (
        len(set(len(partition["events"]) for partition in event_fixture("skew")["partitions"])) == 4
    )


def test_collection_definitions_bind_series_without_claiming_generated_membership():
    fixture = event_fixture()
    status, payload = respond(fixture, "/multivariate_event_collections/COLLECTION-1")
    assert status == 200
    definition = payload["multivariate_contract"]
    assert definition["collection_ticker"] == "COLLECTION-1"
    rows = _all_pages(fixture, "/events/multivariate", {"collection_ticker": "COLLECTION-1"})
    assert all(row["series_ticker"] == definition["series_ticker"] for row in rows)
    assert not {row["event_ticker"] for row in rows}.intersection(
        definition["associated_event_tickers"]
    )
    assert respond(fixture, "/multivariate_event_collections/MISSING") == (404, {})


def test_wide_unicode_exact_decimals_and_gzip_bytes_are_deterministic():
    fixture = event_fixture("wide")
    _, payload = respond(fixture, "/events", {"series_ticker": "SERIES-0"})
    identity = encoded_body(payload)
    compressed = encoded_body(payload, "gzip")
    assert compressed == encoded_body(copy.deepcopy(payload), "gzip")
    assert compressed[4:8] == b"\x00\x00\x00\x00"
    assert compressed[9] == 255  # The fixture pins a deterministic gzip header.
    assert gzip.decompress(compressed) == identity
    decoded = json.loads(identity)
    assert decoded == payload
    assert decoded["events"][0]["product_metadata"]["exact_decimal"] == "0.123456789123456789"
    assert "café 雨" in decoded["events"][0]["title"]
    assert len(identity) > 20_000
    assert len(compressed) < len(identity)
    with pytest.raises(ValueError, match="compression"):
        encoded_body(payload, "br")


@pytest.mark.parametrize("cursor", [None, True, 1, "invalid", "offset-0", "offset-99"])
def test_invalid_cursor_never_silently_restarts_fixture(cursor):
    assert respond(event_fixture(), "/events", {"cursor": cursor})[0] == 400


@pytest.mark.parametrize("limit", [None, True, 0, 201, "2"])
def test_invalid_page_limit_is_rejected(limit):
    assert respond(event_fixture(), "/events", {"limit": limit})[0] == 400


def test_response_mutation_cannot_change_another_request_or_fixture_identity():
    fixture = event_fixture()
    before = fixture_identity(fixture)
    _, payload = respond(fixture, "/events", {"series_ticker": "SERIES-0"})
    payload["events"][0]["product_metadata"]["rules"] = "tampered"
    _, definition = respond(fixture, "/multivariate_event_collections/COLLECTION-1")
    definition["multivariate_contract"]["series_ticker"] = "tampered"
    assert fixture_identity(fixture) == before
    fixture["partitions"][0]["events"][0]["title"] = "actual fixture change"
    assert fixture_identity(fixture) != before


def test_unknown_fixture_and_conflicting_filters_fail_explicitly():
    with pytest.raises(ValueError, match="unknown"):
        event_fixture("missing")
    fixture = event_fixture()
    assert respond(fixture, "/unknown") == (404, {})
    assert (
        respond(
            fixture,
            "/events/multivariate",
            {"series_ticker": "MVE-SERIES", "collection_ticker": "COLLECTION-1"},
        )[0]
        == 400
    )
    assert respond(fixture, "/events", {"collection_ticker": "COLLECTION-1"})[0] == 400
