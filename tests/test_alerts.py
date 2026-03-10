"""Tests for alert sinks."""
from sentineldq.alerts import ConsoleSink


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
