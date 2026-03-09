"""Tests for schema drift detection."""
from sentineldq.detect.schema_drift import detect_schema_drift


def test_no_alert_when_no_previous_baseline():
    assert detect_schema_drift("t", "abc123", None) is None


def test_no_alert_when_schema_unchanged():
    assert detect_schema_drift("t", "same_hash", "same_hash") is None


def test_alert_when_schema_changed():
    alert = detect_schema_drift("public.orders", "new_hash_xyz", "old_hash_abc")
    assert alert is not None
    assert alert["rule_name"] == "schema_drift"
    assert alert["severity"] == "high"
    assert "schema changed" in alert["message"]
    assert "public.orders" in alert["message"]
    assert alert["details"]["previous_schema_hash"] == "old_hash_abc"
    assert alert["details"]["current_schema_hash"] == "new_hash_xyz"
