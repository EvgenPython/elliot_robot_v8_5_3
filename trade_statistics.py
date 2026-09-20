"""Deterministic trading statistics derived from durable Trade State."""

from __future__ import annotations

from collections import Counter
from math import isfinite


def _number(value) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def build_trade_statistics(trade_state: dict | None) -> dict:
    """Summarise broker-confirmed lifecycle outcomes without guessing.

    Only history rows with ``status=closed`` and a numeric ``net_result``
    contribute to P/L. Cancelled, expired, rejected and unresolved plans are
    lifecycle statistics, never fake zero-profit trades.
    """

    state = trade_state if isinstance(trade_state, dict) else {}
    history = state.get("history")
    if not isinstance(history, list):
        history = []

    lifecycle = Counter()
    order_types = Counter()
    close_reasons = Counter()
    closed = []

    for raw in history:
        if not isinstance(raw, dict):
            continue
        status = str(raw.get("status") or "unknown").lower()
        lifecycle[status] += 1
        # A filled pending order is normalised into a managed position and its
        # original ticket moves to source_pending_ticket.  Count either field
        # so the lifecycle is not erased merely because the order filled.
        if raw.get("pending_ticket") or raw.get("source_pending_ticket"):
            lifecycle["pending_ticket_issued"] += 1

        if status != "closed":
            continue
        net = _number(raw.get("net_result"))
        if net is None:
            lifecycle["closed_without_confirmed_result"] += 1
            continue

        close_reason = str(raw.get("close_reason") or "UNKNOWN").upper()
        order_type = str(raw.get("order_type") or "unknown").lower()
        close_reasons[close_reason] += 1
        order_types[order_type] += 1
        closed.append({
            "plan_id": raw.get("plan_id"),
            "position_ticket": raw.get("position_ticket"),
            "action": raw.get("action"),
            "order_type": order_type,
            "volume": _number(raw.get("actual_volume") or raw.get("volume")),
            "entry_price": _number(raw.get("actual_entry_price") or raw.get("entry_price")),
            "close_price": _number(raw.get("close_price")),
            "close_reason": close_reason,
            "closed_at_fp": raw.get("close_time_fp") or raw.get("closed_at_fp"),
            "net_result": net,
        })

    wins = [item for item in closed if item["net_result"] > 0]
    losses = [item for item in closed if item["net_result"] < 0]
    breakeven = [item for item in closed if item["net_result"] == 0]
    gross_profit = sum(item["net_result"] for item in wins)
    gross_loss = abs(sum(item["net_result"] for item in losses))
    net_result = sum(item["net_result"] for item in closed)
    count = len(closed)

    return {
        "schema_version": 1,
        "history_rows": len(history),
        "closed_trades": count,
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(breakeven),
        "win_rate_percent": (len(wins) / count * 100.0) if count else None,
        "net_result": net_result,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "average_result": (net_result / count) if count else None,
        "profit_factor": (gross_profit / gross_loss) if gross_loss else None,
        "lifecycle_counts": dict(sorted(lifecycle.items())),
        "close_reason_counts": dict(sorted(close_reasons.items())),
        "closed_order_type_counts": dict(sorted(order_types.items())),
        "recent_closed_trades": closed[-20:][::-1],
    }
