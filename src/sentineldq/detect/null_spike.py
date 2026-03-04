from __future__ import annotations
from typing import List, Optional, Dict, Any


def _median(values: List[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0:
        raise ValueError("median of empty list")
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


def detect_null_spike(
    table_name: str,
    column_name: str,
    current_null_rate: float,
    history_null_rates: List[float],
    spike_abs_threshold: float = 0.10,   # +10% absolute increase triggers
    min_history: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Detects sudden increase in null_rate compared to historical median baseline.

    spike_abs_threshold: absolute increase threshold, e.g. 0.10 means +10%.
    """
    if len(history_null_rates) < min_history:
        return None

    baseline = _median(history_null_rates)
    increase = current_null_rate - baseline

    if increase > spike_abs_threshold:
        severity = "high" if increase > spike_abs_threshold * 2 else "med"
        return {
            "severity": severity,
            "rule_name": "null_spike",
            "message": (
                f"[{table_name}.{column_name}] null_rate spike: current={current_null_rate:.3f}, "
                f"baseline(median)={baseline:.3f}, increase=+{increase:.3f} "
                f"(threshold=+{spike_abs_threshold:.3f})."
            ),
            "details": {
                "table_name": table_name,
                "column_name": column_name,
                "current_null_rate": current_null_rate,
                "baseline_null_rate": baseline,
                "increase": increase,
                "threshold": spike_abs_threshold,
                "history": history_null_rates,
            },
        }

    return None