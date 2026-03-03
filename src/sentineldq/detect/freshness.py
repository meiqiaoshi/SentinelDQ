from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Dict, Any


def _parse_iso(ts: str) -> datetime:
    # handles "2026-03-03T..." style; if tz missing, assume UTC
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def detect_freshness_anomaly(
    table_name: str,
    max_freshness_ts: Optional[str],
    threshold_minutes: int,
    now_utc: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """
    If (now - max_ts) exceeds threshold, return alert payload.
    """
    if not max_freshness_ts:
        return {
            "severity": "high",
            "rule_name": "freshness_missing",
            "message": f"[{table_name}] freshness check failed: no max timestamp available.",
            "details": {"threshold_minutes": threshold_minutes},
        }

    now_utc = now_utc or datetime.now(timezone.utc)
    max_dt = _parse_iso(max_freshness_ts)
    age_minutes = (now_utc - max_dt).total_seconds() / 60.0

    if age_minutes > threshold_minutes:
        sev = "high" if age_minutes > threshold_minutes * 2 else "med"
        return {
            "severity": sev,
            "rule_name": "freshness_stale",
            "message": (
                f"[{table_name}] data is stale: max_ts={max_freshness_ts}, "
                f"age={age_minutes:.1f} min (threshold={threshold_minutes} min)."
            ),
            "details": {
                "max_freshness_ts": max_freshness_ts,
                "age_minutes": age_minutes,
                "threshold_minutes": threshold_minutes,
            },
        }

    return None