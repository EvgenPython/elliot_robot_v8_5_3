from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

STATE_PATH = (
    BASE_DIR
    / "state"
    / "cost_event_gate.json"
)


def _float(value):

    try:
        value = float(value)
    except (TypeError, ValueError):
        return None

    if value != value:
        return None

    return value


def _bar_value(
    bar: dict,
    name: str,
):

    for key, value in bar.items():

        if str(key).lower() == name.lower():
            return value

    return None


def _rows(
    snapshot: dict,
    timeframe: str,
    limit: int = 2,
) -> list[dict]:

    try:
        closed = (
            snapshot["timeframes"]
            [timeframe]
            ["closed_bars"]
        )
    except Exception:
        return []


    # pandas.DataFrame
    if hasattr(
        closed,
        "tail",
    ) and hasattr(
        closed,
        "iterrows",
    ):

        result = []

        for index, row in (
            closed.tail(
                limit
            ).iterrows()
        ):

            item = dict(
                row.to_dict()
            )

            if not any(
                str(key).lower()
                in {
                    "time",
                    "time_fp",
                    "datetime",
                }
                for key in item
            ):
                item["time"] = (
                    index.isoformat()
                    if hasattr(
                        index,
                        "isoformat",
                    )
                    else str(index)
                )

            result.append(
                item
            )

        return result


    if isinstance(
        closed,
        list,
    ):

        return [
            dict(item)
            for item in closed[-limit:]
            if isinstance(
                item,
                dict,
            )
        ]


    # compact transport:
    # {"columns":[...], "rows":[...]}
    if isinstance(
        closed,
        dict,
    ):

        columns = closed.get(
            "columns"
        )

        raw_rows = closed.get(
            "rows"
        )

        if (
            isinstance(columns, list)
            and isinstance(
                raw_rows,
                list,
            )
        ):

            result = []

            for raw in raw_rows[-limit:]:

                if not isinstance(
                    raw,
                    list,
                ):
                    continue

                result.append(
                    dict(
                        zip(
                            columns,
                            raw,
                        )
                    )
                )

            return result


    return []


def _collect_price_levels(
    node,
    *,
    key_hint: str = "",
) -> list[float]:

    result = []

    if isinstance(
        node,
        dict,
    ):

        for key, value in node.items():

            key_lower = str(
                key
            ).lower()

            price_key = any(
                token in key_lower
                for token in (
                    "price",
                    "level",
                    "target",
                    "stop",
                    "invalidation",
                    "confirmation",
                )
            )

            if price_key:

                value_float = _float(
                    value
                )

                if value_float is not None:
                    result.append(
                        value_float
                    )

            result.extend(
                _collect_price_levels(
                    value,
                    key_hint=key_lower,
                )
            )


    elif isinstance(
        node,
        list,
    ):

        for item in node:

            result.extend(
                _collect_price_levels(
                    item,
                    key_hint=key_hint,
                )
            )


    return result


def _parse_time(value):

    if isinstance(
        value,
        datetime,
    ):
        return value

    text = str(
        value or ""
    ).strip()

    if not text:
        return None

    try:
        return datetime.fromisoformat(
            text.replace(
                "Z",
                "+00:00",
            )
        )
    except ValueError:
        return None


def _watch_owned(
    previous_reference: dict,
) -> bool:

    try:
        from entry_watch import (
            load_entry_watch
        )

        watch = (
            load_entry_watch()
            or {}
        )

    except Exception:
        return False


    if watch.get(
        "status"
    ) != "watching":
        return False


    reference_timestamp = str(
        (
            previous_reference.get(
                "analysis"
            )
            or {}
        ).get(
            "timestamp"
        )
        or ""
    )

    source_timestamp = str(
        watch.get(
            "source_analysis_timestamp"
        )
        or ""
    )


    return bool(
        reference_timestamp
        and source_timestamp
        and reference_timestamp
        == source_timestamp
    )


def _result(
    *,
    snapshot: dict,
    material_change: bool,
    possible_setup: bool,
    full_required: bool,
    trigger_kind: str,
    en: str,
    ru: str,
    details: list[str] | None = None,
) -> dict:

    return {
        "instrument": str(
            snapshot.get(
                "instrument"
            )
            or "XAUUSD"
        ),
        "timestamp": str(
            snapshot.get(
                "generated_at_fp"
            )
            or ""
        ),
        "material_change": bool(
            material_change
        ),
        "possible_setup": bool(
            possible_setup
        ),
        "full_analysis_required": bool(
            full_required
        ),
        "confidence": (
            "high"
            if material_change
            or possible_setup
            else "medium"
        ),
        "trigger_kind": str(
            trigger_kind
        ),
        "observed_changes": list(
            details or []
        ),
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


def inspect_h1_event(
    snapshot: dict,
    previous_reference: dict,
) -> dict:
    """
    Pure deterministic cost gate.

    It does NOT decide long/short and does NOT create Entry/SL/TP.
    It only decides whether buying a new Claude analysis is justified.
    """

    analysis = (
        previous_reference.get(
            "analysis"
        )
        if isinstance(
            previous_reference,
            dict,
        )
        else None
    )

    if not isinstance(
        analysis,
        dict,
    ):

        return _result(
            snapshot=snapshot,
            material_change=True,
            possible_setup=True,
            full_required=True,
            trigger_kind="uncertainty",
            en=(
                "No validated reference map is available; "
                "a deep analysis is required."
            ),
            ru=(
                "Нет подтверждённой предыдущей карты; "
                "требуется глубокий анализ."
            ),
        )


    if _watch_owned(
        previous_reference
    ):

        return _result(
            snapshot=snapshot,
            material_change=False,
            possible_setup=False,
            full_required=False,
            trigger_kind="none",
            en=(
                "A validated entry watch already owns the "
                "M15/M5 trigger; another hourly Claude decision "
                "would duplicate paid work."
            ),
            ru=(
                "Подтверждённый Entry Watch уже контролирует "
                "триггер M15/M5; ещё одна платная часовая "
                "проверка дублировала бы работу."
            ),
        )


    h1 = _rows(
        snapshot,
        "H1",
        2,
    )

    if not h1:

        return _result(
            snapshot=snapshot,
            material_change=True,
            possible_setup=True,
            full_required=False,
            trigger_kind="uncertainty",
            en=(
                "The local gate cannot read the latest closed H1; "
                "use a paid H1 decision rather than silently skip it."
            ),
            ru=(
                "Локальный фильтр не смог прочитать последнюю "
                "закрытую H1; безопаснее выполнить платную H1 "
                "проверку, чем молча пропустить её."
            ),
        )


    bar = h1[-1]

    high = _float(
        _bar_value(
            bar,
            "high",
        )
    )

    low = _float(
        _bar_value(
            bar,
            "low",
        )
    )

    close = _float(
        _bar_value(
            bar,
            "close",
        )
    )

    if (
        high is None
        or low is None
        or close is None
        or close <= 0
    ):

        return _result(
            snapshot=snapshot,
            material_change=True,
            possible_setup=True,
            full_required=False,
            trigger_kind="uncertainty",
            en=(
                "Closed-H1 OHLC is incomplete; request an H1 "
                "decision instead of guessing."
            ),
            ru=(
                "OHLC закрытой H1 неполный; вместо догадки "
                "нужна H1-проверка."
            ),
        )


    # ----------------------------------------------------------
    # 1. Higher-timeframe wave invalidation.
    # ----------------------------------------------------------

    wave_count = (
        analysis.get(
            "wave_count"
        )
        or {}
    )

    invalidation = _float(
        wave_count.get(
            "invalidation_level"
        )
    )

    direction = str(
        wave_count.get(
            "direction"
        )
        or ""
    ).lower()


    invalidated = False

    if invalidation is not None:

        if (
            direction
            in {
                "bullish",
                "up",
                "long",
            }
            and close
            < invalidation
        ):
            invalidated = True

        elif (
            direction
            in {
                "bearish",
                "down",
                "short",
            }
            and close
            > invalidation
        ):
            invalidated = True


    if invalidated:

        return _result(
            snapshot=snapshot,
            material_change=True,
            possible_setup=True,
            full_required=True,
            trigger_kind="structure",
            details=[
                (
                    "H1 close crossed "
                    f"wave invalidation {invalidation}"
                )
            ],
            en=(
                "The validated higher-timeframe wave "
                "invalidation was crossed. Rebuild the map."
            ),
            ru=(
                "Закрытие H1 пересекло подтверждённую "
                "инвалидацию старшей волновой карты. "
                "Карту нужно перестроить."
            ),
        )


    # ----------------------------------------------------------
    # 2. Significant known levels.
    # ----------------------------------------------------------

    # IMPORTANT:
    # Visualization is presentation-only and must never
    # influence a trading decision or a paid-analysis trigger.
    #
    # Only validated analytical/trading fields participate
    # in the local event gate.
    raw_levels = (
        _collect_price_levels(
            {
                "recommendation": (
                    analysis.get(
                        "recommendation"
                    )
                ),
                "wave_count": (
                    analysis.get(
                        "wave_count"
                    )
                ),
            }
        )
    )


    levels = []

    for value in raw_levels:

        # Ignore obviously unrelated historical coordinates.
        if (
            abs(
                value - close
            )
            / close
            > 0.05
        ):
            continue

        if not any(
            abs(
                value - old
            )
            <= max(
                close * 0.00002,
                0.01,
            )
            for old in levels
        ):
            levels.append(
                value
            )


    touched = [
        value
        for value in levels
        if low <= value <= high
    ]


    near = [
        value
        for value in levels
        if (
            abs(
                close - value
            )
            / close
            <= 0.0012
        )
    ]


    # ----------------------------------------------------------
    # 3. Abnormally large H1 movement.
    # ----------------------------------------------------------

    range_fraction = (
        max(
            0.0,
            high - low,
        )
        / close
    )


    previous_close = None

    if len(h1) >= 2:

        previous_close = _float(
            _bar_value(
                h1[-2],
                "close",
            )
        )


    close_change = 0.0

    if (
        previous_close is not None
        and previous_close > 0
    ):

        close_change = (
            abs(
                close - previous_close
            )
            / previous_close
        )


    strong_move = bool(
        range_fraction >= 0.0065
        or close_change >= 0.0045
    )


    if (
        touched
        or near
        or strong_move
    ):

        details = []

        if touched:
            details.append(
                "touched levels: "
                + ", ".join(
                    f"{value:.5f}"
                    for value in touched[:6]
                )
            )

        if near:
            details.append(
                "near levels: "
                + ", ".join(
                    f"{value:.5f}"
                    for value in near[:6]
                )
            )

        if strong_move:
            details.append(
                (
                    "H1 movement "
                    f"range={range_fraction:.4%}; "
                    f"close_change={close_change:.4%}"
                )
            )

        return _result(
            snapshot=snapshot,
            material_change=True,
            possible_setup=True,
            full_required=False,
            trigger_kind="setup",
            details=details,
            en=(
                "The closed H1 reached a decision-relevant "
                "level or made a material move. A fresh H1 "
                "trade decision is justified; the higher map "
                "does not need rebuilding."
            ),
            ru=(
                "Закрытая H1 достигла важного для решения "
                "уровня либо совершила существенное движение. "
                "Свежая H1-проверка сделки оправдана, но "
                "старшую карту перестраивать не нужно."
            ),
        )


    # ----------------------------------------------------------
    # 4. One insurance recheck later in the day.
    # ----------------------------------------------------------

    time_value = (
        _bar_value(
            bar,
            "time",
        )
        or _bar_value(
            bar,
            "time_fp",
        )
        or _bar_value(
            bar,
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

        if close_hour == 14:

            return _result(
                snapshot=snapshot,
                material_change=False,
                possible_setup=True,
                full_required=False,
                trigger_kind="setup",
                details=[
                    (
                        "single daily insurance "
                        "H1 recheck at 14:00 FP"
                    )
                ],
                en=(
                    "No structural event was detected, but "
                    "this is the single daily insurance H1 "
                    "recheck."
                ),
                ru=(
                    "Структурного события нет, но это "
                    "единственная дневная страховочная "
                    "H1-проверка."
                ),
            )


    return _result(
        snapshot=snapshot,
        material_change=False,
        possible_setup=False,
        full_required=False,
        trigger_kind="none",
        en=(
            "The new H1 remains inside the validated map "
            "without a material level interaction. No paid "
            "Claude refresh is justified."
        ),
        ru=(
            "Новая H1 остаётся внутри подтверждённой карты "
            "и не взаимодействует существенно с важными "
            "уровнями. Платное обновление Claude не требуется."
        ),
    )


def _load_state() -> dict:

    if not STATE_PATH.exists():
        return {
            "version": 1,
            "evaluated_h1": {},
        }

    try:
        state = json.loads(
            STATE_PATH.read_text(
                encoding="utf-8"
            )
        )
    except Exception:
        state = {}

    if not isinstance(
        state,
        dict,
    ):
        state = {}

    state.setdefault(
        "version",
        1,
    )

    state.setdefault(
        "evaluated_h1",
        {},
    )

    return state


def _save_state(
    state: dict,
) -> None:

    STATE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp = STATE_PATH.with_suffix(
        ".json.tmp"
    )

    tmp.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    tmp.replace(
        STATE_PATH
    )


def was_h1_evaluated(
    h1_key: str,
) -> bool:

    state = _load_state()

    return str(
        h1_key
    ) in (
        state.get(
            "evaluated_h1"
        )
        or {}
    )


def mark_h1_evaluated(
    h1_key: str,
    result: dict,
) -> None:

    state = _load_state()

    values = state.setdefault(
        "evaluated_h1",
        {},
    )

    values[
        str(h1_key)
    ] = {
        "result": result,
        "recorded_at": (
            datetime.utcnow()
            .isoformat()
            + "Z"
        ),
    }

    # Keep a bounded rolling history.
    keys = list(
        values
    )

    while len(keys) > 200:

        oldest = keys.pop(
            0
        )

        values.pop(
            oldest,
            None,
        )

    _save_state(
        state
    )
