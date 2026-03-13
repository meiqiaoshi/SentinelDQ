"""
Alert sinks: dispatch alerts to console, file, Slack, and later email, etc.
"""
from __future__ import annotations

import json
import logging
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Protocol

logger = logging.getLogger(__name__)


class AlertSink(Protocol):
    """Interface for sending detection alerts. Implement to add Slack, email, etc."""

    def send(
        self,
        run_id: str,
        table_name: str,
        alert_payload: Dict[str, Any],
    ) -> Optional[str]:
        """
        Handle one alert. May persist and/or notify.
        Returns an alert_id if persisted, else None.
        """
        ...


class ConsoleSink:
    """Persist alerts to the metadata store and log to the console."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def send(
        self,
        run_id: str,
        table_name: str,
        alert_payload: Dict[str, Any],
    ) -> Optional[str]:
        alert_id = self._store.save_alert(
            run_id=run_id,
            table_name=table_name,
            severity=alert_payload["severity"],
            rule_name=alert_payload["rule_name"],
            message=alert_payload["message"],
        )
        logger.info(
            "ALERT[%s]: %s (alert_id=%s)",
            alert_payload["severity"],
            alert_payload["message"],
            alert_id,
        )
        return alert_id


class FileSink:
    """Persist alerts to the metadata store and append them to a file (one JSON line per alert)."""

    def __init__(self, store: Any, file_path: str | Path) -> None:
        self._store = store
        self._path = Path(file_path)

    def send(
        self,
        run_id: str,
        table_name: str,
        alert_payload: Dict[str, Any],
    ) -> Optional[str]:
        alert_id = self._store.save_alert(
            run_id=run_id,
            table_name=table_name,
            severity=alert_payload["severity"],
            rule_name=alert_payload["rule_name"],
            message=alert_payload["message"],
        )
        line = json.dumps(
            {
                "alert_id": alert_id,
                "run_id": run_id,
                "table_name": table_name,
                "severity": alert_payload["severity"],
                "rule_name": alert_payload["rule_name"],
                "message": alert_payload["message"],
            },
            ensure_ascii=False,
        ) + "\n"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(line)
        return alert_id


class SlackSink:
    """Persist alerts to the metadata store and post a summary to a Slack webhook."""

    def __init__(self, store: Any, webhook_url: str) -> None:
        self._store = store
        self._webhook_url = webhook_url.strip()

    def send(
        self,
        run_id: str,
        table_name: str,
        alert_payload: Dict[str, Any],
    ) -> Optional[str]:
        alert_id = self._store.save_alert(
            run_id=run_id,
            table_name=table_name,
            severity=alert_payload["severity"],
            rule_name=alert_payload["rule_name"],
            message=alert_payload["message"],
        )
        text = (
            f"*[SentinelDQ]* `{alert_payload['severity']}` | {alert_payload['rule_name']} | "
            f"`{table_name}`\n{alert_payload['message']}"
        )
        payload = {"text": text}
        try:
            req = urllib.request.Request(
                self._webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            logger.warning("Slack webhook failed: %s", e)
        return alert_id
