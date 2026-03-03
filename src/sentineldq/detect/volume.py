from __future__ import annotations
from typing import List, Optional, Dict, Any


def _median(values: List[int]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0:
        raise ValueError("median of empty list")
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


def detect_volume_anomaly(
    table_name: str,
    current_row_count: int,
    history_row_counts: List[int],
    change_pct_threshold: float = 0.30,
    min_history: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Compare current row_count to historical median baseline.
    If deviation exceeds threshold, return an alert payload; else None.

    change_pct_threshold: 0.30 means +/-30% change triggers anomaly.
    """
    if len(history_row_counts) < min_history:
        return None  # not enough history to make a stable call

    baseline = _median(history_row_counts)

    # if baseline is 0, handle edge case
    if baseline == 0:
        if current_row_count != 0:
            return {
                "severity": "high",
                "rule_name": "volume_anomaly",
                "message": f"[{table_name}] row_count changed from baseline 0 to {current_row_count}.",
                "details": {
                    "baseline": baseline,
                    "current": current_row_count,
                    "history": history_row_counts,
                    "threshold": change_pct_threshold,
                },
            }
        return None

    deviation = (current_row_count - baseline) / baseline  # e.g. -0.5 means -50%
    if abs(deviation) > change_pct_threshold:
        direction = "drop" if deviation < 0 else "spike"
        severity = "high" if abs(deviation) > (change_pct_threshold * 2) else "med"
        pct = round(deviation * 100, 2)

        return {
            "severity": severity,
            "rule_name": "volume_anomaly",
            "message": (
                f"[{table_name}] volume {direction}: current row_count={current_row_count}, "
                f"baseline(median)={baseline}, deviation={pct}% (threshold={change_pct_threshold*100:.0f}%)."
            ),
            "details": {
                "baseline": baseline,
                "current": current_row_count,
                "deviation_pct": deviation,
                "history": history_row_counts,
                "threshold": change_pct_threshold,
            },
        }

    return None