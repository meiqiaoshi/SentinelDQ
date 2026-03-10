"""
Alert sinks: dispatch alerts to console, and later to Slack, email, etc.
"""
from __future__ import annotations

import logging
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
