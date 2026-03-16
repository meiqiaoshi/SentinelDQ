"""Tests for alert sinks."""
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from sentineldq.alerts import ConsoleSink, FileSink, SlackSink


def test_console_sink_send_persists_and_returns_alert_id():
    class MockStore:
        def __init__(self):
            self.saved = []

        def save_alert(self, run_id, table_name, severity, rule_name, message):
            self.saved.append(
                {"run_id": run_id, "table_name": table_name, "severity": severity, "rule_name": rule_name, "message": message}
            )
            return "alert-uuid-123"

    store = MockStore()
    sink = ConsoleSink(store)
    alert_payload = {"severity": "high", "rule_name": "volume_anomaly", "message": "test"}
    alert_id = sink.send("run-1", "public.t", alert_payload)
    assert alert_id == "alert-uuid-123"
    assert len(store.saved) == 1
    assert store.saved[0]["run_id"] == "run-1"
    assert store.saved[0]["table_name"] == "public.t"
    assert store.saved[0]["severity"] == "high"
    assert store.saved[0]["rule_name"] == "volume_anomaly"


def test_file_sink_persists_and_appends_to_file():
    class MockStore:
        def save_alert(self, run_id, table_name, severity, rule_name, message):
            return "alert-456"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        path = f.name
    try:
        store = MockStore()
        sink = FileSink(store, path)
        alert_payload = {"severity": "med", "rule_name": "freshness_stale", "message": "data is stale"}
        alert_id = sink.send("run-2", "public.events", alert_payload)
        assert alert_id == "alert-456"
        lines = Path(path).read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["alert_id"] == "alert-456"
        assert record["table_name"] == "public.events"
        assert record["rule_name"] == "freshness_stale"
    finally:
        Path(path).unlink(missing_ok=True)


def test_slack_sink_persists_and_returns_alert_id():
    """SlackSink persists to store and returns alert_id; webhook POST may fail without a real URL."""
    class MockStore:
        def save_alert(self, run_id, table_name, severity, rule_name, message):
            return "alert-slack-789"

    store = MockStore()
    sink = SlackSink(store, "https://hooks.slack.com/services/fake/fake/fake")
    alert_payload = {"severity": "high", "rule_name": "schema_drift", "message": "schema changed"}
    alert_id = sink.send("run-3", "public.orders", alert_payload)
    assert alert_id == "alert-slack-789"


def test_slack_sink_posts_to_webhook_when_mocked():
    """SlackSink calls urlopen with POST to webhook URL and body containing alert text."""
    class MockStore:
        def save_alert(self, run_id, table_name, severity, rule_name, message):
            return "alert-999"

    store = MockStore()
    webhook_url = "https://hooks.slack.com/services/T/B/X"
    sink = SlackSink(store, webhook_url)
    alert_payload = {"severity": "med", "rule_name": "volume_anomaly", "message": "volume drop 50%"}

    with patch("sentineldq.alerts.urllib.request.urlopen", MagicMock()) as mock_urlopen:
        alert_id = sink.send("run-4", "public.events", alert_payload)

    assert alert_id == "alert-999"
    assert mock_urlopen.call_count == 1
    call_args = mock_urlopen.call_args
    assert call_args[1].get("timeout") == 10
    req = call_args[0][0]
    assert req.get_full_url() == webhook_url
    assert req.data is not None
    body = json.loads(req.data.decode("utf-8"))
    assert "text" in body
    assert "SentinelDQ" in body["text"]
    assert "volume_anomaly" in body["text"]
    assert "volume drop 50%" in body["text"]
