"""
Schema drift detection: compare current schema_hash to the previous run.
Alerts when the table schema has changed (columns added/removed or types changed).
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def detect_schema_drift(
    table_name: str,
    current_schema_hash: str,
    previous_schema_hash: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    If there is a previous run and the schema hash changed, return an alert payload.
    Returns None when there is no previous baseline (first run) or when schema is unchanged.
    """
    if previous_schema_hash is None:
        return None
    if current_schema_hash == previous_schema_hash:
        return None
    return {
        "severity": "high",
        "rule_name": "schema_drift",
        "message": (
            f"[{table_name}] schema changed: hash was {previous_schema_hash[:8]}..., "
            f"now {current_schema_hash[:8]}... (columns or types may have changed)."
        ),
        "details": {
            "table_name": table_name,
            "previous_schema_hash": previous_schema_hash,
            "current_schema_hash": current_schema_hash,
        },
    }
