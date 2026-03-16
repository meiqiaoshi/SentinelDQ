"""Tests for volume anomaly detection."""
from sentineldq.detect.volume import detect_volume_anomaly


def test_no_alert_when_insufficient_history():
    assert detect_volume_anomaly("t", 100, [], min_history=3) is None
    assert detect_volume_anomaly("t", 100, [90, 95], min_history=3) is None


def test_no_alert_when_within_threshold():
    # baseline median 100, current 100 -> 0% change
    assert detect_volume_anomaly("t", 100, [90, 100, 110], change_pct_threshold=0.30) is None
    # ~20% change, threshold 30%
    assert detect_volume_anomaly("t", 120, [100, 100, 100], change_pct_threshold=0.30) is None


def test_alert_on_drop():
    # median 100, current 50 -> -50% > 30%
    alert = detect_volume_anomaly("t", 50, [100, 100, 100], change_pct_threshold=0.30)
    assert alert is not None
    assert alert["rule_name"] == "volume_anomaly"
    assert alert["severity"] in ("med", "high")
    assert "drop" in alert["message"] or "50" in alert["message"]


def test_alert_on_spike():
    alert = detect_volume_anomaly("t", 200, [100, 100, 100], change_pct_threshold=0.30)
    assert alert is not None
    assert "spike" in alert["message"] or "200" in alert["message"]


def test_baseline_zero_current_nonzero():
    alert = detect_volume_anomaly("t", 10, [0, 0, 0], change_pct_threshold=0.30)
    assert alert is not None
    assert alert["severity"] == "high"
    assert "0" in alert["message"] and "10" in alert["message"]


def test_baseline_zero_current_zero_no_alert():
    """Edge case: current and baseline both zero -> no alert (no division-by-zero, no spurious alert)."""
    assert detect_volume_anomaly("t", 0, [0, 0, 0], change_pct_threshold=0.30) is None
