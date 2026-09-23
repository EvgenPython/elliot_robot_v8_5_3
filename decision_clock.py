"""Persistent intermediate M30 decision clock.

H1 remains the owner of the market structure.  This clock creates one extra
decision opportunity halfway through the hour, after a fully closed M30 bar,
without pretending that an unclosed H1 candle is a confirmed structural bar.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from instruments import active_instrument, symbol_state_path
from market_data import TIMEFRAMES, get_closed_bars
from prop_time import FUNDINGPIPS_TZ, now_fp
from analysis_state import load_analysis_state
from ai_local_event_gate import was_h1_evaluated


SYMBOL = active_instrument()
STATE_PATH = symbol_state_path("m30_decision_state.json")
MIN_SECONDS_AFTER_M30_CLOSE = 2
MAX_CLOSED_M30_AGE_SECONDS = 25 * 60
FAILED_RETRY_COOLDOWN_SECONDS = 300


def _as_fp_datetime(value) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value))
    if result.tzinfo is None:
        result = result.replace(tzinfo=FUNDINGPIPS_TZ)
    return result.astimezone(FUNDINGPIPS_TZ)


def _empty_state() -> dict:
    return {
        "version": 1,
        "symbol": SYMBOL,
        "last_completed_m30": None,
        "last_attempt_m30": None,
        "last_attempt_at_fp": None,
        "last_status": None,
        "last_error": None,
    }


def load_m30_decision_state() -> dict:
    if not STATE_PATH.exists():
        return _empty_state()
    try:
        value = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Не удалось прочитать {STATE_PATH}: {error}") from error
    if not isinstance(value, dict):
        raise RuntimeError("m30_decision_state.json должен содержать object.")
    result = _empty_state()
    result.update(value)
    return result


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, STATE_PATH)


def latest_closed_m30_time(symbol: str = SYMBOL) -> datetime:
    bars = get_closed_bars(
        symbol=symbol,
        timeframe=TIMEFRAMES["M30"],
        count=1,
    )
    value = bars.iloc[-1]["time_fp"]
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    return _as_fp_datetime(value)


def inspect_m30_decision_due(
    *,
    symbol: str = SYMBOL,
    market_gate: dict,
) -> dict:
    current = now_fp()
    m30_open = latest_closed_m30_time(symbol)
    m30_close = m30_open + timedelta(minutes=30)
    age = (current - m30_close).total_seconds()
    # V8.5.3 M30 PRIMARY EVERY CLOSED M30
    #
    # Every CLOSED M30 can own an entry decision.
    #
    # At xx:00 the freshly closed H1 must first pass its structural cycle.
    # Runner polls again ~10 sec later; only then may the same xx:00 M30
    # become an entry-decision clock.
    #
    # At xx:30 there is no new H1 close, therefore M30 may proceed directly.

    top_of_hour = (
        m30_close.minute == 0
    )

    intermediate_half_hour = (
        m30_close.minute == 30
    )

    fresh = (
        MIN_SECONDS_AFTER_M30_CLOSE
        <= age
        <= MAX_CLOSED_M30_AGE_SECONDS
    )

    latest_h1 = str(
        market_gate.get(
            "latest_closed_h1_time"
        )
        or ""
    )

    h1_structural_ready = True


    if top_of_hour:

        analysis_state = (
            load_analysis_state()
        )

        completed_h1 = str(
            analysis_state.get(
                "last_analyzed_h1"
            )
            or ""
        )

        fully_processed = bool(
            latest_h1
            and completed_h1
            == latest_h1
        )

        locally_processed = bool(
            latest_h1
            and was_h1_evaluated(
                latest_h1
            )
        )

        h1_structural_ready = bool(
            fully_processed
            or locally_processed
        )
    state = load_m30_decision_state()
    m30_key = m30_open.isoformat()
    already_completed = str(state.get("last_completed_m30")) == m30_key

    retry_wait = 0.0
    if (
        str(state.get("last_attempt_m30")) == m30_key
        and str(state.get("last_status") or "").upper() in {"STARTED", "FAILED"}
        and state.get("last_attempt_at_fp")
    ):
        try:
            elapsed = (current - _as_fp_datetime(state["last_attempt_at_fp"])).total_seconds()
            retry_wait = max(0.0, FAILED_RETRY_COOLDOWN_SECONDS - elapsed)
        except (TypeError, ValueError):
            retry_wait = 0.0

    reasons = []
    if not bool(market_gate.get("analysis_window_allowed")):
        reasons.append("Вне окна новых торговых идей.")
    if not bool(market_gate.get("tick_fresh")):
        reasons.append("Последний MT5 tick устарел.")
    if (
        top_of_hour
        and not h1_structural_ready
    ):
        reasons.append(
            "Сначала должна завершиться структурная H1-проверка; "
            "после неё закрытая M30 получит своё entry decision."
        )
    if not fresh:
        reasons.append(
            f"Закрытая M30 не свежая: age={age:.1f}s, "
            f"allowed={MIN_SECONDS_AFTER_M30_CLOSE}..{MAX_CLOSED_M30_AGE_SECONDS}s."
        )
    if already_completed:
        reasons.append("Эта M30 уже успешно обработана.")
    if retry_wait > 0:
        reasons.append(f"Retry cooldown ещё {retry_wait:.0f}s.")

    return {
        "due": not reasons,
        "symbol": symbol,
        "m30_open_time_fp": m30_key,
        "m30_close_time_fp": m30_close.isoformat(),
        "m30_age_from_close_seconds": age,
        "top_of_hour": top_of_hour,
        "intermediate_half_hour": intermediate_half_hour,
        "h1_structural_ready": h1_structural_ready,
        "latest_h1_time_fp": latest_h1,
        "fresh": fresh,
        "already_completed": already_completed,
        "retry_after_seconds": retry_wait,
        "reasons": reasons,
    }


def mark_m30_decision_attempt(
    m30_open_time_fp: str,
    *,
    status: str,
    error: str | None = None,
) -> dict:
    normalized = str(status).strip().upper()
    if normalized not in {"STARTED", "FAILED", "COMPLETED"}:
        raise ValueError(f"Неизвестный M30 decision status: {status}")
    state = load_m30_decision_state()
    state.update(
        {
            "last_attempt_m30": str(m30_open_time_fp),
            "last_attempt_at_fp": now_fp().isoformat(),
            "last_status": normalized,
            "last_error": str(error) if error else None,
        }
    )
    if normalized == "COMPLETED":
        state["last_completed_m30"] = str(m30_open_time_fp)
    _save_state(state)
    return state
