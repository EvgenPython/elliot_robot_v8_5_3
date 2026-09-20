"""Deterministic SL/TP protection for one already managed MT5 position.

Claude may describe the Elliott structure and raw-bar anchors. This module is
the only bridge to ``TRADE_ACTION_SLTP`` and rejects ambiguous, stale or
unverifiable proposals. It never opens, adds, reverses or closes a position.
"""

from __future__ import annotations
from observability import observe, emit

import copy
import json
import math
import os
from datetime import datetime
from pathlib import Path

import MetaTrader5 as mt5

from execution_control import inspect_execution_safety_gate
from instruments import active_instrument, symbol_state_path
from prop_time import now_fp
from market_data import get_current_tick
from runtime_policy import MAX_LAST_TICK_AGE_SECONDS
from trade_executor import MAGIC_NUMBER, mt5_object_to_dict
from trade_state import update_managed_position_protection


SYMBOL = active_instrument()
STATE_PATH = symbol_state_path("position_protection_state.json")
STATE_VERSION = 1
MAX_HISTORY = 100

STOP_ACTIONS = {"tighten_stop", "tighten_stop_and_recalculate_target"}
TARGET_ACTIONS = {
    "recalculate_target",
    "tighten_stop_and_recalculate_target",
}

# Owner-defined practical rule: the current motive wave protects behind the
# extreme of the previous completed motive wave of the same direction.
STOP_REFERENCE_BY_CURRENT_WAVE = {
    "3": "1",
    "5": "3",
    "C": "A",
    "Y": "W",
}

FIB_RATIOS_BY_METHOD = {
    "wave3_extension": {1.0, 1.618, 2.618},
    "wave5_projection": {0.618, 1.0, 1.618},
    "correction_b_retracement": {0.382, 0.5, 0.618, 0.786},
    "correction_c_projection": {1.0, 1.272, 1.618},
    "wxy_y_projection": {1.0, 1.618},
    "diagonal_projection": {0.618, 1.0},
}

FIB_WAVES_BY_METHOD = {
    "wave3_extension": {"3"},
    "wave5_projection": {"5"},
    "correction_b_retracement": {"B"},
    "correction_c_projection": {"C"},
    "wxy_y_projection": {"Y"},
    "diagonal_projection": {"3", "5"},
}


class PositionProtectionError(RuntimeError):
    """Fail-closed rejection of an unsafe position-management proposal."""


def _empty_state() -> dict:
    return {
        "version": STATE_VERSION,
        "updated_at_fp": None,
        "active_intent": None,
        "last_result": None,
        "history": [],
    }


def _load_state() -> dict:
    try:
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return _empty_state()
    if not isinstance(raw, dict):
        return _empty_state()
    result = _empty_state()
    result.update(raw)
    if not isinstance(result.get("history"), list):
        result["history"] = []
    return result


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = copy.deepcopy(state)
    payload["version"] = STATE_VERSION
    payload["updated_at_fp"] = now_fp().isoformat()
    temporary = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    with open(temporary, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, STATE_PATH)


def _append_history(state: dict, item: dict) -> None:
    history = state.setdefault("history", [])
    history.append(copy.deepcopy(item))
    if len(history) > MAX_HISTORY:
        state["history"] = history[-MAX_HISTORY:]


def _as_rows(snapshot: dict, timeframe: str) -> list[dict]:
    data = (snapshot.get("timeframes") or {}).get(timeframe) or {}
    bars = data.get("closed_bars")
    if bars is None:
        return []
    if hasattr(bars, "to_dict"):
        rows = bars.to_dict(orient="records")
    elif isinstance(bars, list):
        rows = copy.deepcopy(bars)
    else:
        return []
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        time_value = item.get("time_fp", item.get("time"))
        if hasattr(time_value, "isoformat"):
            time_value = time_value.isoformat()
        item["time_fp"] = str(time_value or "")
        result.append(item)
    return result


def _parse_time(value) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    try:
        result = datetime.fromisoformat(text)
    except ValueError as error:
        raise PositionProtectionError(f"Некорректное время anchor: {value!r}.") from error
    if result.tzinfo is None:
        raise PositionProtectionError("Время anchor не содержит timezone.")
    return result


def _same_time(left, right) -> bool:
    try:
        return _parse_time(left) == _parse_time(right)
    except PositionProtectionError:
        return False


def _price_tolerance(snapshot: dict) -> float:
    info = snapshot.get("symbol_info") or {}
    point = abs(float(info.get("point", 0.0) or 0.0))
    tick_size = abs(float(info.get("trade_tick_size", 0.0) or 0.0))
    return max(point, tick_size, 1e-9) * 0.51


def _validate_raw_anchor(
    snapshot: dict,
    *,
    timeframe: str,
    time_value: str,
    price: float,
    kind: str,
) -> float:
    if kind not in {"open", "high", "low", "close"}:
        raise PositionProtectionError(f"Недопустимый OHLC kind: {kind}.")
    matches = [
        row
        for row in _as_rows(snapshot, timeframe)
        if _same_time(row.get("time_fp"), time_value)
    ]
    if len(matches) != 1:
        raise PositionProtectionError(
            f"Anchor {timeframe} {time_value} не найден среди закрытых свечей."
        )
    try:
        raw_price = float(matches[0][kind])
    except (KeyError, TypeError, ValueError) as error:
        raise PositionProtectionError(
            f"У anchor {timeframe} {time_value} отсутствует {kind}."
        ) from error
    if not math.isfinite(raw_price) or not math.isfinite(float(price)):
        raise PositionProtectionError("Anchor price must be finite.")
    if abs(raw_price - float(price)) > _price_tolerance(snapshot):
        raise PositionProtectionError(
            "Цена anchor не совпадает с raw MT5 OHLC: "
            f"получено {price}, ожидалось {raw_price}."
        )
    return raw_price


def _validate_previous_wave_range(management, snapshot, anchor, kind):
    if management.get("stop_wave_status") != "completed":
        raise PositionProtectionError("Stop reference wave is not completed.")
    start = _parse_time(management.get("stop_wave_start_time"))
    end = _parse_time(management.get("stop_wave_end_time"))
    anchor_time = _parse_time(management.get("stop_anchor_time"))
    rows = _as_rows(snapshot, "M15")
    by_time = {_parse_time(row["time_fp"]): row for row in rows}
    if start not in by_time or end not in by_time or not start < end:
        raise PositionProtectionError("Previous wave endpoints must be distinct closed M15 candles.")
    if not start <= anchor_time <= end:
        raise PositionProtectionError("Stop anchor lies outside the previous completed wave.")
    values = [float(row[kind]) for stamp, row in by_time.items() if start <= stamp <= end]
    if not all(math.isfinite(value) for value in values):
        raise PositionProtectionError("Invalid OHLC in previous wave range.")
    extreme = min(values) if kind == "low" else max(values)
    if abs(anchor - extreme) > _price_tolerance(snapshot):
        raise PositionProtectionError("Stop anchor is not the extreme of the declared previous wave.")


def _refresh_execution_snapshot(snapshot):
    """Use a fresh quote after the potentially long API call; candles stay frozen."""
    fresh = dict(snapshot)
    tick = get_current_tick(str(snapshot["instrument"]))
    age = (now_fp() - _parse_time(tick.get("time_fp"))).total_seconds()
    if not 0 <= age <= MAX_LAST_TICK_AGE_SECONDS:
        raise PositionProtectionError(f"Fresh execution quote unavailable: age={age:.1f}s.")
    info = mt5.symbol_info(str(snapshot["instrument"]))
    if info is None:
        raise PositionProtectionError("Fresh symbol specification unavailable.")
    fresh["tick"] = tick
    fresh["symbol_info"] = dict(snapshot.get("symbol_info") or {})
    for key in ("point", "digits", "trade_tick_size", "trade_stops_level", "trade_freeze_level"):
        fresh["symbol_info"][key] = getattr(info, key)
    return fresh


def _normalize_price(price: float, tick_size: float, digits: int, mode: str) -> float:
    step = tick_size if tick_size > 0 else 10 ** (-max(0, digits))
    scaled = float(price) / step
    if mode == "floor":
        scaled = math.floor(scaled + 1e-10)
    elif mode == "ceil":
        scaled = math.ceil(scaled - 1e-10)
    else:
        scaled = round(scaled)
    return round(scaled * step, digits)


def _current_position(ticket: int):
    positions = mt5.positions_get(ticket=int(ticket))
    if positions is None:
        raise PositionProtectionError(
            f"positions_get(ticket={ticket}) вернул None: {mt5.last_error()}."
        )
    if len(positions) != 1:
        raise PositionProtectionError(
            f"Ожидалась одна открытая позиция #{ticket}, найдено {len(positions)}."
        )
    return positions[0]


def _proposal_direction(position) -> str:
    position_type = int(getattr(position, "type", -1))
    if position_type == int(mt5.POSITION_TYPE_BUY):
        return "bullish"
    if position_type == int(mt5.POSITION_TYPE_SELL):
        return "bearish"
    raise PositionProtectionError("Неизвестное направление MT5 position.")


def _validate_ratio(method: str, ratio: float) -> float:
    allowed = FIB_RATIOS_BY_METHOD.get(str(method))
    if not allowed:
        raise PositionProtectionError(f"Недопустимый Fib method: {method}.")
    for candidate in allowed:
        if abs(float(ratio) - candidate) <= 0.0005:
            return candidate
    raise PositionProtectionError(
        f"Fib ratio {ratio} не разрешён для {method}."
    )


def _build_prices(
    *,
    review: dict,
    snapshot: dict,
    position,
    config: dict,
) -> dict:
    management = review.get("position_management") or {}
    action = str(management.get("action") or "hold")
    direction = _proposal_direction(position)
    if management.get("position_direction") != direction:
        raise PositionProtectionError(
            "Направление protection plan не совпадает с MT5 position."
        )
    if review.get("confidence") != str(config.get("minimum_confidence", "high")):
        raise PositionProtectionError(
            "Для автоматического изменения protection требуется confidence=high."
        )
    if management.get("structure_confirmed") is not True:
        raise PositionProtectionError("Вложенная M15-структура не подтверждена.")
    if (review.get("data_quality") or {}).get("sufficient") is not True:
        raise PositionProtectionError("Data quality недостаточно для SL/TP.")
    position_status = review.get("position_status")
    if position_status not in {
        "healthy", "target_near", "weakened", "thesis_invalidated"
    }:
        raise PositionProtectionError(
            "Неясный статус позиции запрещает автоматическое изменение SL/TP."
        )
    if position_status not in {"healthy", "target_near"} and action != "tighten_stop":
        raise PositionProtectionError(
            "При ослаблении сценария допустимо только подтверждённое подтягивание стопа."
        )
    if review.get("advisory_action") not in {"hold", "watch_closely"}:
        raise PositionProtectionError(
            "Advisory manual_review/no_assessment запрещает изменение SL/TP."
        )

    info = snapshot.get("symbol_info") or {}
    tick = snapshot.get("tick") or {}
    point = abs(float(info.get("point", 0.0) or 0.0))
    tick_size = abs(float(info.get("trade_tick_size", 0.0) or 0.0)) or point
    digits = int(info.get("digits", 5) or 5)
    bid = float(tick.get("bid", 0.0) or 0.0)
    ask = float(tick.get("ask", 0.0) or 0.0)
    if bid <= 0 or ask <= 0 or ask < bid:
        raise PositionProtectionError("Некорректный свежий Bid/Ask snapshot.")
    current_sl = float(getattr(position, "sl", 0.0) or 0.0)
    current_tp = float(getattr(position, "tp", 0.0) or 0.0)
    open_price = float(getattr(position, "price_open", 0.0) or 0.0)
    if current_sl <= 0 or current_tp <= 0:
        raise PositionProtectionError("У позиции должен уже существовать SL и TP.")

    minimum_distance = (
        max(
            int(info.get("trade_stops_level", 0) or 0),
            int(info.get("trade_freeze_level", 0) or 0),
        )
        * point
        + max(tick_size, point)
    )
    tolerance = max(tick_size, point, 1e-9) * 0.51
    new_sl = current_sl
    new_tp = current_tp
    stop_changed = False
    target_changed = False
    evidence = {}

    if action in STOP_ACTIONS:
        current_wave = str(management.get("current_m15_wave") or "unclear")
        expected_reference = STOP_REFERENCE_BY_CURRENT_WAVE.get(current_wave)
        actual_reference = str(management.get("stop_reference_wave") or "none")
        if expected_reference is None or actual_reference != expected_reference:
            raise PositionProtectionError(
                "Stop reference не соответствует правилу предыдущей волны: "
                f"current={current_wave}, reference={actual_reference}."
            )
        expected_kind = "low" if direction == "bullish" else "high"
        if management.get("stop_anchor_kind") != expected_kind:
            raise PositionProtectionError(
                f"Для {direction} stop anchor должен быть {expected_kind}."
            )
        anchor = _validate_raw_anchor(
            snapshot,
            timeframe="M15",
            time_value=management.get("stop_anchor_time"),
            price=management.get("stop_anchor_price"),
            kind=expected_kind,
        )
        _validate_previous_wave_range(management, snapshot, anchor, expected_kind)
        # Owner rule: SL sits immediately behind the extreme of the previous
        # completed wave.  No ATR/spread discretionary padding is allowed.
        # One broker price step is the smallest value that is literally
        # behind (rather than on) the swing; a larger adjustment is made only
        # when the broker's current stop/freeze distance legally requires it.
        buffer_price = max(tick_size, point, 10 ** (-max(0, digits)))
        if direction == "bullish":
            candidate = anchor - buffer_price
            candidate = min(candidate, bid - minimum_distance)
            candidate = _normalize_price(candidate, tick_size, digits, "floor")
            stop_changed = candidate > current_sl + tolerance
        else:
            candidate = anchor + buffer_price
            candidate = max(candidate, ask + minimum_distance)
            candidate = _normalize_price(candidate, tick_size, digits, "ceil")
            stop_changed = candidate < current_sl - tolerance
        if stop_changed:
            new_sl = candidate
        evidence["stop"] = {
            "current_wave": current_wave,
            "reference_wave": actual_reference,
            "anchor_time": management.get("stop_anchor_time"),
            "anchor_price": anchor,
            "anchor_kind": expected_kind,
            "buffer_price": buffer_price,
            "buffer_policy": "one_price_step_behind_previous_wave",
            "candidate": candidate,
            "accepted": stop_changed,
        }

    if action in TARGET_ACTIONS:
        timeframe = str(management.get("fib_timeframe") or "none")
        method = str(management.get("fib_method") or "none")
        current_wave = str(management.get("current_m15_wave") or "unclear")
        if current_wave not in FIB_WAVES_BY_METHOD.get(method, set()):
            raise PositionProtectionError(
                f"Fib method {method} не соответствует M15 wave {current_wave}."
            )
        ratio = _validate_ratio(method, float(management.get("fib_ratio")))
        start_time = _parse_time(management.get("fib_leg_start_time"))
        end_time = _parse_time(management.get("fib_leg_end_time"))
        projection_time = _parse_time(management.get("fib_projection_time"))
        if not (start_time < end_time <= projection_time):
            raise PositionProtectionError(
                "Fib anchors должны идти по времени start < end <= projection."
            )
        start_price = _validate_raw_anchor(
            snapshot,
            timeframe=timeframe,
            time_value=management.get("fib_leg_start_time"),
            price=management.get("fib_leg_start_price"),
            kind=management.get("fib_leg_start_kind"),
        )
        end_price = _validate_raw_anchor(
            snapshot,
            timeframe=timeframe,
            time_value=management.get("fib_leg_end_time"),
            price=management.get("fib_leg_end_price"),
            kind=management.get("fib_leg_end_kind"),
        )
        projection_price = _validate_raw_anchor(
            snapshot,
            timeframe=timeframe,
            time_value=management.get("fib_projection_time"),
            price=management.get("fib_projection_price"),
            kind=management.get("fib_projection_kind"),
        )
        leg_length = abs(end_price - start_price)
        if leg_length <= tolerance:
            raise PositionProtectionError("Длина Fib leg равна нулю.")
        sign = 1.0 if direction == "bullish" else -1.0
        measured_direction = 1.0 if end_price > start_price else -1.0
        expected_measured_direction = (
            -sign if method == "correction_b_retracement" else sign
        )
        if measured_direction != expected_measured_direction:
            raise PositionProtectionError(
                "Направление измеряемой Fib leg не соответствует "
                f"методу {method} и позиции {direction}."
            )
        candidate = projection_price + sign * leg_length * ratio
        mode = "floor" if direction == "bullish" else "ceil"
        candidate = _normalize_price(candidate, tick_size, digits, mode)
        if direction == "bullish":
            if candidate <= max(ask + minimum_distance, open_price + tolerance):
                raise PositionProtectionError(
                    "Bullish Fib target уже пройден или не является profit target."
                )
        else:
            if candidate >= min(bid - minimum_distance, open_price - tolerance):
                raise PositionProtectionError(
                    "Bearish Fib target уже пройден или не является profit target."
                )
        target_changed = abs(candidate - current_tp) > tolerance
        if target_changed:
            new_tp = candidate
        evidence["target"] = {
            "method": method,
            "timeframe": timeframe,
            "ratio": ratio,
            "leg_start": start_price,
            "leg_end": end_price,
            "projection": projection_price,
            "candidate": candidate,
            "accepted": target_changed,
        }

    # Never allow a stop regression even if the proposal or broker rounding is
    # wrong. This invariant applies after all calculations.
    if direction == "bullish" and new_sl + tolerance < current_sl:
        raise PositionProtectionError("Запрещено отодвигать LONG SL вниз.")
    if direction == "bearish" and new_sl - tolerance > current_sl:
        raise PositionProtectionError("Запрещено отодвигать SHORT SL вверх.")

    return {
        "action": action,
        "direction": direction,
        "current_sl": current_sl,
        "current_tp": current_tp,
        "new_sl": new_sl,
        "new_tp": new_tp,
        "stop_changed": stop_changed,
        "target_changed": target_changed,
        "evidence": evidence,
    }


def _matches_request(position, request: dict, tolerance: float) -> bool:
    return (
        abs(float(getattr(position, "sl", 0.0)) - float(request["sl"]))
        <= tolerance
        and abs(float(getattr(position, "tp", 0.0)) - float(request["tp"]))
        <= tolerance
    )


def _reconcile_active_intent(state: dict, snapshot: dict) -> dict | None:
    intent = state.get("active_intent")
    if not isinstance(intent, dict):
        return None
    ticket = int(intent.get("position_ticket", 0) or 0)
    request = intent.get("request") or {}
    position = _current_position(ticket)
    tolerance = _price_tolerance(snapshot)
    if not request or not _matches_request(position, request, tolerance):
        return {
            "ok": False,
            "status": "unresolved_previous_intent",
            "position_ticket": ticket,
            "error": (
                "Предыдущий SL/TP SEND_INTENT не подтверждён MT5; новая "
                "отправка заблокирована до ручной проверки."
            ),
        }
    update_managed_position_protection(
        position_ticket=ticket,
        actual_stop_loss=float(position.sl),
        actual_take_profit=float(position.tp),
        audit={"reconciled_after_restart": True, "intent": intent},
    )
    result = {
        "ok": True,
        "status": "reconciled_after_restart",
        "position_ticket": ticket,
        "stop_loss": float(position.sl),
        "take_profit": float(position.tp),
        "completed_at_fp": now_fp().isoformat(),
    }
    state["active_intent"] = None
    state["last_result"] = copy.deepcopy(result)
    _append_history(state, result)
    _save_state(state)
    return result


@observe("position")
def execute_position_protection(
    *,
    review: dict,
    snapshot: dict,
    position_context: dict,
    config: dict,
    event_key: str,
) -> dict:
    """Validate and, when armed, modify SL/TP of exactly one position."""
    state = _load_state()
    reconciled = _reconcile_active_intent(state, snapshot)
    if reconciled is not None and not reconciled.get("ok"):
        return reconciled

    management = review.get("position_management") or {}
    action = str(management.get("action") or "hold")
    if action in {"hold", "manual_review"}:
        return {"ok": True, "status": action, "order_send_called": False}

    live_contexts = position_context.get("live_positions") or []
    if len(live_contexts) != 1:
        raise PositionProtectionError(
            "Автосопровождение требует ровно одну managed position."
        )
    live_context = live_contexts[0]
    if live_context.get("errors"):
        raise PositionProtectionError(
            "Position reconciliation содержит ошибки: "
            + "; ".join(str(item) for item in live_context["errors"])
        )
    ticket = int(live_context.get("position_ticket", 0) or 0)
    if str(review.get("position_ticket")) != str(ticket):
        raise PositionProtectionError("Ticket review не совпадает с position.")

    position = _current_position(ticket)
    if str(getattr(position, "symbol", "")) != str(snapshot.get("instrument")):
        raise PositionProtectionError("Symbol MT5 position не совпадает со snapshot.")
    if int(getattr(position, "magic", -1)) != int(MAGIC_NUMBER):
        raise PositionProtectionError("Position не принадлежит этому роботу (Magic).")
    context_tolerance = _price_tolerance(snapshot)
    if (
        abs(float(position.sl) - float(live_context.get("stop_loss", 0.0)))
        > context_tolerance
        or abs(float(position.tp) - float(live_context.get("take_profit", 0.0)))
        > context_tolerance
    ):
        raise PositionProtectionError(
            "SL/TP изменились после построения position context; нужен новый review."
        )

    execution_snapshot = _refresh_execution_snapshot(snapshot)
    prices = _build_prices(
        review=review,
        snapshot=execution_snapshot,
        position=position,
        config=config,
    )
    emit("position", "protection_prices", data={"position_ticket": ticket, "prices": prices,
         "management": management, "fresh_tick": execution_snapshot["tick"]})
    if not prices["stop_changed"] and not prices["target_changed"]:
        result = {
            "ok": True,
            "status": "no_safer_change",
            "order_send_called": False,
            "position_ticket": ticket,
            "prices": prices,
        }
        state["last_result"] = copy.deepcopy(result)
        _append_history(state, result)
        _save_state(state)
        return result

    safety = inspect_execution_safety_gate()
    if not safety.get("order_send_allowed"):
        return {
            "ok": True,
            "status": "dry_run_or_execution_blocked",
            "order_send_called": False,
            "position_ticket": ticket,
            "prices": prices,
            "execution": safety,
        }
    if not bool(config.get("automatic_trade_changes", False)):
        return {
            "ok": True,
            "status": "automatic_management_disabled",
            "order_send_called": False,
            "position_ticket": ticket,
            "prices": prices,
        }

    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": str(position.symbol),
        "position": int(ticket),
        "sl": float(prices["new_sl"]),
        "tp": float(prices["new_tp"]),
        "magic": int(MAGIC_NUMBER),
    }
    intent = {
        "event_key": str(event_key),
        "position_ticket": ticket,
        "created_at_fp": now_fp().isoformat(),
        "request": copy.deepcopy(request),
        "prices": copy.deepcopy(prices),
    }
    state["active_intent"] = intent
    _save_state(state)

    emit("mt5", "order_send.request", data={"request": request})
    raw_result = mt5.order_send(request)
    emit("mt5", "order_send.response", data={"result": raw_result, "last_error": mt5.last_error() if raw_result is None else None})
    result_dict = mt5_object_to_dict(raw_result)
    success_retcodes = {
        int(getattr(mt5, "TRADE_RETCODE_DONE", 10009)),
        int(getattr(mt5, "TRADE_RETCODE_NO_CHANGES", 10025)),
    }
    if raw_result is None:
        # Unknown outcome: keep SEND_INTENT and never retry automatically.
        return {
            "ok": False,
            "status": "order_send_outcome_unknown",
            "order_send_called": True,
            "position_ticket": ticket,
            "mt5_last_error": mt5.last_error(),
        }
    retcode = int(getattr(raw_result, "retcode", -1))
    if retcode not in success_retcodes:
        failure = {
            "ok": False,
            "status": "mt5_rejected",
            "order_send_called": True,
            "position_ticket": ticket,
            "retcode": retcode,
            "mt5_result": result_dict,
        }
        state = _load_state()
        state["active_intent"] = None
        state["last_result"] = copy.deepcopy(failure)
        _append_history(state, failure)
        _save_state(state)
        return failure

    actual = _current_position(ticket)
    tolerance = _price_tolerance(snapshot)
    if not _matches_request(actual, request, tolerance):
        # Keep the SEND_INTENT unresolved: a retry could duplicate an operation
        # whose server-side result is not yet observable.
        return {
            "ok": False,
            "status": "mt5_result_not_reconciled",
            "order_send_called": True,
            "position_ticket": ticket,
            "retcode": retcode,
            "mt5_result": result_dict,
        }

    state_update = update_managed_position_protection(
        position_ticket=ticket,
        actual_stop_loss=float(actual.sl),
        actual_take_profit=float(actual.tp),
        expected_previous_stop_loss=float(live_context.get("stop_loss", 0.0)),
        expected_previous_take_profit=float(live_context.get("take_profit", 0.0)),
        audit={
            "event_key": str(event_key),
            "review_trigger": review.get("review_trigger"),
            "current_m15_wave": management.get("current_m15_wave"),
            "management_reason": management.get("management_reason"),
            "evidence": prices.get("evidence"),
            "retcode": retcode,
        },
    )
    success = {
        "ok": True,
        "status": "protection_modified",
        "order_send_called": True,
        "position_ticket": ticket,
        "stop_loss": float(actual.sl),
        "take_profit": float(actual.tp),
        "state_update": state_update,
        "mt5_result": result_dict,
        "completed_at_fp": now_fp().isoformat(),
    }
    state = _load_state()
    state["active_intent"] = None
    state["last_result"] = copy.deepcopy(success)
    _append_history(state, success)
    _save_state(state)
    return success
