"""Tests for freshness anomaly detection."""
from datetime import datetime, timezone, timedelta
from sentineldq.detect.freshness import detect_freshness_anomaly


def test_alert_when_max_ts_missing():
    alert = detect_freshness_anomaly("t", None, threshold_minutes=60)
    assert alert is not None
    assert alert["rule_name"] == "freshness_missing"
    assert alert["severity"] == "high"


def test_no_alert_when_fresh():
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=10)).isoformat()
    assert detect_freshness_anomaly(
        "t", recent, threshold_minutes=60, now_utc=now
    ) is None


def test_alert_when_stale():
    now = datetime.now(timezone.utc)
    old = (now - timedelta(minutes=120)).isoformat()
    alert = detect_freshness_anomaly(
        "t", old, threshold_minutes=60, now_utc=now
    )
    assert alert is not None
    assert alert["rule_name"] == "freshness_stale"
    assert alert["severity"] in ("med", "high")
    assert "stale" in alert["message"] or "120" in alert["message"]
