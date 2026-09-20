"""Deterministic M15/M5 watcher for a Claude-defined conditional setup."""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import MetaTrader5 as mt5

from market_data import TIMEFRAMES, mt5_timestamp_to_fp
from prop_time import now_fp
from instruments import symbol_state_path


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
ENTRY_WATCH_PATH = symbol_state_path("entry_watch.json")
ENTRY_CHECK_RETRY_SECONDS = 300
ENTRY_CHECK_MAX_ATTEMPTS = 5


def _atomic_write(value: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    temporary = ENTRY_WATCH_PATH.with_suffix(".tmp")
    with open(temporary, "w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, ENTRY_WATCH_PATH)


def load_entry_watch() -> dict | None:
    if not ENTRY_WATCH_PATH.exists():
        return None
    try:
        with open(ENTRY_WATCH_PATH, "r", encoding="utf-8") as file:
            value = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def refresh_entry_watch(analysis: dict, source_h1: str | None = None) -> dict:
    """Stores one best primary M5/M15 projection; never invents a trigger."""
    visualization = analysis.get("visualization") or {}
    projections = [
        item for item in visualization.get("projected_waves", [])
        if isinstance(item, dict)
        and item.get("scenario") == "primary"
        and item.get("timeframe") in {"M5", "M15"}
        and item.get("confirmation_level") is not None
        and item.get("direction") in {"up", "down", "bullish", "bearish"}
        and str(item.get("status", "")).lower()
        not in {"invalidated", "failed", "completed"}
    ]
    projections.sort(
        key=lambda item: (
            0 if item.get("timeframe") == "M5" else 1,
            str(item.get("projection_id", "")),
        )
    )
    selected = projections[0] if projections else None
    state = {
        "schema_version": 1,
        "updated_at_fp": now_fp().isoformat(),
        "status": "watching" if selected else "inactive",
        "source_analysis_timestamp": analysis.get("timestamp"),
        "source_h1_closed_bar_time": source_h1,
        "projection": selected,
        "fvg_context": {
            "role": (analysis.get("recommendation") or {}).get("fvg_role"),
            "ids": (analysis.get("recommendation") or {}).get("fvg_ids", ""),
            "basis": (analysis.get("recommendation") or {}).get("fvg_basis", ""),
        },
        "last_checked_closed_bar_time": None,
        "triggered_closed_bar_time": None,
        "last_checked_bar": None,
        "terminal_event": None,
        "last_result": None,
        "entry_check_attempts": 0,
        "last_entry_check_at_fp": None,
        "next_retry_at_fp": None,
    }
    _atomic_write(state)
    return state


def _latest_closed_bar(symbol: str, timeframe: str) -> dict | None:
    mt5_timeframe = TIMEFRAMES.get(timeframe)
    if mt5_timeframe is None:
        return None
    rates = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, 3)
    if rates is None or len(rates) < 2:
        return None
    row = rates[-2]
    return {
        "time": mt5_timestamp_to_fp(int(row["time"])).isoformat(),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
    }


def inspect_entry_trigger(symbol: str = "XAUUSD") -> dict:
    state = load_entry_watch()
    if not state or state.get("status") not in {
        "watching", "triggered", "retry_pending"
    }:
        return {"triggered": False, "reason": "no_active_watch"}

    status = str(state.get("status"))
    if status == "retry_pending":
        retry_at = state.get("next_retry_at_fp")
        try:
            retry_at_dt = datetime.fromisoformat(str(retry_at))
        except (TypeError, ValueError):
            retry_at_dt = None
        if retry_at_dt is not None and now_fp() < retry_at_dt:
            return {
                "triggered": False,
                "reason": "entry_check_retry_cooldown",
                "next_retry_at_fp": retry_at,
            }
    projection = state.get("projection") or {}
    timeframe = str(projection.get("timeframe", ""))
    bar = _latest_closed_bar(symbol, timeframe)
    if bar is None:
        return {"triggered": False, "reason": "closed_bar_unavailable"}
    if (
        status == "watching"
        and bar["time"] == state.get("last_checked_closed_bar_time")
    ):
        return {"triggered": False, "reason": "bar_already_checked", "bar": bar}

    state["last_checked_closed_bar_time"] = bar["time"]
    state["last_checked_bar"] = dict(bar)
    direction = str(projection.get("direction", "")).lower()
    confirmation = float(projection["confirmation_level"])
    invalidation = projection.get("invalidation_level")
    invalidated = False
    if invalidation is not None:
        invalidation = float(invalidation)
        invalidated = bar["close"] <= invalidation if direction in {"up", "bullish"} else bar["close"] >= invalidation
    if invalidated:
        state["status"] = "invalidated"
        state["last_result"] = "closed_bar_crossed_invalidation"
        state["terminal_event"] = {
            "kind": "entry_projection_invalidated",
            "bar": dict(bar),
            "level": invalidation,
            "direction": direction,
            "projection_id": projection.get("projection_id"),
        }
        _atomic_write(state)
        return {"triggered": False, "invalidated": True, "bar": bar, "projection": projection}

    triggered = bar["close"] >= confirmation if direction in {"up", "bullish"} else bar["close"] <= confirmation
    if triggered:
        state["status"] = "triggered"
        state["triggered_closed_bar_time"] = bar["time"]
        state["last_result"] = "confirmation_closed_bar"
        state["terminal_event"] = {
            "kind": "entry_projection_confirmed",
            "bar": dict(bar),
            "level": confirmation,
            "direction": direction,
            "projection_id": projection.get("projection_id"),
        }
    _atomic_write(state)
    return {"triggered": triggered, "bar": bar, "projection": projection}


def mark_entry_check_result(
    result: str,
    *,
    retryable: bool = False,
    retry_after_seconds: int = ENTRY_CHECK_RETRY_SECONDS,
    max_attempts: int = ENTRY_CHECK_MAX_ATTEMPTS,
    count_attempt: bool = True,
) -> dict:
    """Finalize or schedule a sequential retry of a triggered entry check.

    A transient Claude/validation/IO error must not silently consume the
    trigger. Retries are spaced by five minutes and remain bounded by the
    lifetime of this setup/new H1, so they cannot create a request flood.
    """

    state = load_entry_watch() or {}
    attempts = int(state.get("entry_check_attempts", 0) or 0)
    if count_attempt:
        attempts += 1
    state["entry_check_attempts"] = attempts
    state["last_entry_check_at_fp"] = now_fp().isoformat()

    if retryable and attempts < max(1, int(max_attempts)):
        state["status"] = "retry_pending"
        state["next_retry_at_fp"] = (
            now_fp() + timedelta(seconds=max(1, int(retry_after_seconds)))
        ).isoformat()
    else:
        state["status"] = "checked" if not retryable else "failed"
        state["next_retry_at_fp"] = None

    state["last_result"] = str(result)
    state["updated_at_fp"] = now_fp().isoformat()
    _atomic_write(state)
    return state
