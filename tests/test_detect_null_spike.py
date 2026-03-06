"""Tests for null-rate spike detection."""
from sentineldq.detect.null_spike import detect_null_spike


def test_no_alert_when_insufficient_history():
    assert detect_null_spike("t", "c", 0.5, [], min_history=3) is None
    assert detect_null_spike("t", "c", 0.5, [0.1, 0.2], min_history=3) is None


def test_no_alert_when_below_threshold():
    # baseline ~0.1, current 0.15 -> +0.05, threshold 0.10
    assert detect_null_spike(
        "t", "c", 0.15, [0.08, 0.10, 0.12], spike_abs_threshold=0.10
    ) is None


def test_alert_on_spike():
    # baseline 0.1, current 0.25 -> +0.15 > 0.10
    alert = detect_null_spike(
        "t", "col", 0.25, [0.08, 0.10, 0.12], spike_abs_threshold=0.10
    )
    assert alert is not None
    assert alert["rule_name"] == "null_spike"
    assert alert["severity"] in ("med", "high")
    assert "col" in alert["message"] and "0.25" in alert["message"]
