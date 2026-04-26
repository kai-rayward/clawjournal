"""``events features`` tests."""

from __future__ import annotations

from clawjournal.events.capabilities import effective_matrix
from clawjournal.events.doctor.features import (
    EVENTS_FEATURES_SCHEMA_VERSION,
    feature_records,
    features_payload,
)


def test_features_payload_carries_schema_version():
    payload = features_payload()
    assert payload["events_features_schema_version"] == EVENTS_FEATURES_SCHEMA_VERSION


def test_features_payload_field_set():
    payload = features_payload()
    expected = {
        "events_features_schema_version",
        "version",
        "bundle_schema_version",
        "recorder_schema_version",
        "features",
        "connectors",
        "limits",
    }
    assert expected.issubset(set(payload.keys()))


def test_request_id_echoed_into_meta():
    payload = features_payload(request_id="req-99")
    assert payload["_meta"]["request_id"] == "req-99"


def test_no_request_id_omits_meta():
    payload = features_payload()
    assert "_meta" not in payload


def test_connectors_match_effective_matrix():
    payload = features_payload()
    matrix = effective_matrix()
    expected = sorted(
        {client for (client, _et), (sup, _r) in matrix.items() if sup}
    )
    assert payload["connectors"] == expected


def test_features_array_derives_from_yaml():
    records = feature_records()
    payload = features_payload()
    assert payload["features"] == [r["id"] for r in records]


def test_features_includes_doctor_command():
    records = feature_records()
    ids = {r["id"] for r in records}
    assert "events.doctor" in ids
    assert "events.features" in ids
    assert "events.docs" in ids


def test_limits_carries_bundle_soft_limit():
    payload = features_payload()
    assert payload["limits"]["bundle_soft_limit_bytes"] == 50_000_000
