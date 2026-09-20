from __future__ import annotations

from datetime import timedelta

from ai_local_event_gate import (
    _bar_value,
    _float,
    _parse_time,
    _rows,
)


def _result(
    *,
    trigger: str,
    deep_review_required: bool,
    critical: bool,
    kind: str,
    en: str,
    ru: str,
    details=None,
    progress_r=None,
    distance_to_sl_r=None,
    distance_to_tp_r=None,
):
    return {
        "trigger": str(trigger),
        "deep_review_required": bool(
            deep_review_required
        ),
        "critical": bool(critical),
        "event_kind": str(kind),
        "details": list(details or []),
        "progress_r": progress_r,
        "distance_to_sl_r": distance_to_sl_r,
        "distance_to_tp_r": distance_to_tp_r,
        "reason": (
            f"EN: {en}\n"
            f"RU: {ru}"
        ),
        "local_cost_gate": True,
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
        },
        "request_id": None,
    }


def _current_price(
    snapshot: dict,
    action: str,
):
    tick = (
        snapshot.get("tick")
        or {}
    )

    if action == "enter_long":
        value = (
            tick.get("bid")
            or tick.get("last")
        )
    elif action == "enter_short":
        value = (
            tick.get("ask")
            or tick.get("last")
        )
    else:
        value = (
            tick.get("bid")
            or tick.get("ask")
            or tick.get("last")
        )

    result = _float(value)

    if result is not None:
        return result

    for tf in ("M15", "H1"):
        rows = _rows(
            snapshot,
            tf,
            1,
        )

        if rows:
            result = _float(
                _bar_value(
                    rows[-1],
                    "close",
                )
            )

            if result is not None:
                return result

    return None


def inspect_position_event(
    *,
    snapshot: dict,
    position_context: dict,
    previous_review: dict | None,
    trigger: str,
) -> dict:
    """
    Deterministic $0 filter for an already-open position.

    It NEVER changes SL/TP and NEVER decides a new trade.
    It only answers whether buying a deep POSITION_REVIEW
    is justified.
    """

    live = (
        position_context.get(
            "live_positions"
        )
        or []
    )

    if len(live) != 1:
        return _result(
            trigger=trigger,
            deep_review_required=True,
            critical=True,
            kind="reconciliation",
            en=(
                "Position reconciliation is ambiguous. "
                "A deep review is required before any "
                "protection change."
            ),
            ru=(
                "Сверка открытой позиции неоднозначна. "
                "Перед любым изменением защиты требуется "
                "глубокая проверка."
            ),
        )

    position = live[0]

    errors = (
        position.get("errors")
        or []
    )

    if errors:
        return _result(
            trigger=trigger,
            deep_review_required=True,
            critical=True,
            kind="reconciliation",
            details=[
                str(item)
                for item in errors
            ],
            en=(
                "The live MT5 position no longer matches "
                "the stored managed state."
            ),
            ru=(
                "Фактическая позиция MT5 больше не полностью "
                "совпадает с сохранённым состоянием робота."
            ),
        )

    action = str(
        position.get("action")
        or ""
    )

    entry = _float(
        position.get("open_price")
    )

    stop = _float(
        position.get("stop_loss")
    )

    target = _float(
        position.get("take_profit")
    )

    current = _current_price(
        snapshot,
        action,
    )

    if (
        entry is None
        or stop is None
        or target is None
        or current is None
    ):
        return _result(
            trigger=trigger,
            deep_review_required=True,
            critical=False,
            kind="uncertainty",
            en=(
                "The local position gate cannot establish "
                "Entry/SL/TP/current price safely."
            ),
            ru=(
                "Локальный фильтр не может надёжно определить "
                "Entry/SL/TP/текущую цену."
            ),
        )

    risk = abs(
        entry - stop
    )

    if risk <= 0:
        return _result(
            trigger=trigger,
            deep_review_required=True,
            critical=True,
            kind="invalid_risk",
            en="Stored position risk is invalid.",
            ru=(
                "Сохранённый риск позиции некорректен."
            ),
        )

    if action == "enter_long":
        progress_r = (
            current - entry
        ) / risk
    elif action == "enter_short":
        progress_r = (
            entry - current
        ) / risk
    else:
        return _result(
            trigger=trigger,
            deep_review_required=True,
            critical=True,
            kind="unknown_direction",
            en="Position direction is unknown.",
            ru="Направление позиции неизвестно.",
        )

    distance_to_sl_r = (
        abs(current - stop)
        / risk
    )

    distance_to_tp_r = (
        abs(target - current)
        / risk
    )

    details = [
        f"progress={progress_r:.3f}R",
        (
            "distance_to_sl="
            f"{distance_to_sl_r:.3f}R"
        ),
        (
            "distance_to_tp="
            f"{distance_to_tp_r:.3f}R"
        ),
    ]

    # ------------------------------------------------------
    # Protection proximity: always worth a deep review,
    # provided the separate position budget allows it.
    # Existing broker SL/TP still protects the position
    # even if the budget later blocks the review.
    # ------------------------------------------------------

    if (
        distance_to_sl_r <= 0.30
        or distance_to_tp_r <= 0.30
    ):
        return _result(
            trigger=trigger,
            deep_review_required=True,
            critical=True,
            kind="protection_proximity",
            details=details,
            progress_r=progress_r,
            distance_to_sl_r=distance_to_sl_r,
            distance_to_tp_r=distance_to_tp_r,
            en=(
                "Price is close to the active Stop Loss "
                "or Take Profit. A fresh protection review "
                "is justified."
            ),
            ru=(
                "Цена приблизилась к действующему Stop Loss "
                "или Take Profit. Свежая проверка сопровождения "
                "оправдана."
            ),
        )

    timeframe = (
        "H1"
        if trigger == "h1_close"
        else "M15"
    )

    bars = _rows(
        snapshot,
        timeframe,
        2,
    )

    range_fraction = 0.0
    close_change = 0.0
    close_hour = None

    if bars:
        current_bar = bars[-1]

        high = _float(
            _bar_value(
                current_bar,
                "high",
            )
        )

        low = _float(
            _bar_value(
                current_bar,
                "low",
            )
        )

        close = _float(
            _bar_value(
                current_bar,
                "close",
            )
        )

        if (
            high is not None
            and low is not None
            and close is not None
            and close > 0
        ):
            range_fraction = (
                max(
                    0.0,
                    high - low,
                )
                / close
            )

        if len(bars) >= 2:
            previous_close = _float(
                _bar_value(
                    bars[-2],
                    "close",
                )
            )

            if (
                close is not None
                and previous_close is not None
                and previous_close > 0
            ):
                close_change = (
                    abs(
                        close
                        - previous_close
                    )
                    / previous_close
                )

        time_value = (
            _bar_value(
                current_bar,
                "time",
            )
            or _bar_value(
                current_bar,
                "time_fp",
            )
            or _bar_value(
                current_bar,
                "datetime",
            )
        )

        bar_time = _parse_time(
            time_value
        )

        if bar_time is not None:
            close_hour = (
                bar_time
                + timedelta(hours=1)
            ).hour

    # ------------------------------------------------------
    # M15: cheap local monitoring.
    # Only a genuinely large short-term movement escalates.
    # ------------------------------------------------------

    if trigger == "m15_close":

        strong_m15 = bool(
            range_fraction >= 0.0018
            or close_change >= 0.0014
        )

        if strong_m15:
            details.extend([
                (
                    "M15_range="
                    f"{range_fraction:.4%}"
                ),
                (
                    "M15_close_change="
                    f"{close_change:.4%}"
                ),
            ])

            return _result(
                trigger=trigger,
                deep_review_required=True,
                critical=False,
                kind="m15_material_move",
                details=details,
                progress_r=progress_r,
                distance_to_sl_r=distance_to_sl_r,
                distance_to_tp_r=distance_to_tp_r,
                en=(
                    "The closed M15 made a material move "
                    "while a position is open."
                ),
                ru=(
                    "Закрытая M15 совершила существенное "
                    "движение при открытой позиции."
                ),
            )

        return _result(
            trigger=trigger,
            deep_review_required=False,
            critical=False,
            kind="quiet_m15",
            details=details,
            progress_r=progress_r,
            distance_to_sl_r=distance_to_sl_r,
            distance_to_tp_r=distance_to_tp_r,
            en=(
                "The new M15 does not justify paying for "
                "another position review."
            ),
            ru=(
                "Новая M15 не даёт оснований оплачивать "
                "очередной анализ открытой позиции."
            ),
        )

    # ------------------------------------------------------
    # H1: wider event filter.
    # Review meaningful progress/adverse movement,
    # a strong hourly candle, or one insurance checkpoint.
    # ------------------------------------------------------

    if trigger == "h1_close":

        meaningful_progress = bool(
            progress_r >= 0.55
            or progress_r <= -0.45
        )

        strong_h1 = bool(
            range_fraction >= 0.0032
            or close_change >= 0.0024
        )

        insurance = (
            close_hour == 14
        )

        if (
            meaningful_progress
            or strong_h1
            or insurance
        ):
            details.extend([
                (
                    "H1_range="
                    f"{range_fraction:.4%}"
                ),
                (
                    "H1_close_change="
                    f"{close_change:.4%}"
                ),
                (
                    "insurance_checkpoint="
                    f"{insurance}"
                ),
            ])

            return _result(
                trigger=trigger,
                deep_review_required=True,
                critical=False,
                kind=(
                    "h1_position_event"
                    if not insurance
                    else "h1_insurance_checkpoint"
                ),
                details=details,
                progress_r=progress_r,
                distance_to_sl_r=distance_to_sl_r,
                distance_to_tp_r=distance_to_tp_r,
                en=(
                    "The closed H1 materially changed the "
                    "position context or reached the single "
                    "daily insurance checkpoint."
                ),
                ru=(
                    "Закрытая H1 существенно изменила "
                    "контекст позиции либо достигнута "
                    "единственная дневная страховочная "
                    "контрольная точка."
                ),
            )

        return _result(
            trigger=trigger,
            deep_review_required=False,
            critical=False,
            kind="quiet_h1",
            details=details,
            progress_r=progress_r,
            distance_to_sl_r=distance_to_sl_r,
            distance_to_tp_r=distance_to_tp_r,
            en=(
                "The open position remains inside the "
                "existing protection context. No paid "
                "hourly review is justified."
            ),
            ru=(
                "Открытая позиция остаётся внутри текущего "
                "контекста защиты. Платный часовой review "
                "не требуется."
            ),
        )

    return _result(
        trigger=trigger,
        deep_review_required=True,
        critical=False,
        kind="unknown_trigger",
        details=details,
        progress_r=progress_r,
        distance_to_sl_r=distance_to_sl_r,
        distance_to_tp_r=distance_to_tp_r,
        en=(
            "Unknown position-monitor trigger; use a deep "
            "review rather than silently ignore it."
        ),
        ru=(
            "Неизвестный триггер сопровождения; безопаснее "
            "выполнить глубокую проверку, чем игнорировать его."
        ),
    )
