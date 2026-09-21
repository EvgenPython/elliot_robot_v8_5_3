from __future__ import annotations

import copy
import math
from datetime import datetime

from chart_contract import (
    _collect_bars,
    _deterministic_fvg_candidates,
    sanitize_visualization,
)


def _price(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None

    if (
        not math.isfinite(value)
        or value <= 0
    ):
        return None

    return value


def _time_key(value):
    text = str(value or "")

    try:
        return datetime.fromisoformat(
            text.replace("Z", "+00:00")
        )
    except Exception:
        return datetime.min


def _unique(items, key):
    result = []
    seen = set()

    for item in items:

        marker = key(item)

        if marker in seen:
            continue

        seen.add(marker)
        result.append(item)

    return result


def _wave_points(visualization):
    return [
        item
        for item in (
            visualization.get("wave_points")
            or []
        )
        if isinstance(item, dict)
        and _price(item.get("price")) is not None
        and item.get("time")
        and item.get("timeframe")
    ]


def _wave_structures(visualization):
    return [
        item
        for item in (
            visualization.get("wave_structures")
            or []
        )
        if isinstance(item, dict)
    ]


def _build_levels(
    visualization: dict,
) -> list[dict]:

    levels = list(
        visualization.get("levels")
        or []
    )

    for point in _wave_points(
        visualization
    ):

        price = _price(
            point.get("price")
        )

        levels.append({
            "kind": "wave_anchor",
            "scenario": str(
                point.get("scenario")
                or "primary"
            ),
            "timeframe": str(
                point.get("timeframe")
                or "H1"
            ),
            "price": price,
            "label": str(
                point.get("label")
                or "Wave anchor"
            ),
            "basis": (
                "EN: Exact validated Elliott wave anchor.\n"
                "RU: Точная подтверждённая опорная точка волн Эллиотта."
            ),
        })


    for structure in _wave_structures(
        visualization
    ):

        for field, kind, suffix in (
            (
                "confirmation_level",
                "confirmation",
                "confirmation",
            ),
            (
                "invalidation_level",
                "invalidation",
                "invalidation",
            ),
        ):

            price = _price(
                structure.get(field)
            )

            if price is None:
                continue

            levels.append({
                "kind": kind,
                "scenario": str(
                    structure.get("scenario")
                    or "primary"
                ),
                "timeframe": str(
                    structure.get("timeframe")
                    or "H1"
                ),
                "price": price,
                "label": (
                    f"{structure.get('label') or structure.get('structure_id')} "
                    f"{suffix}"
                ),
                "basis": (
                    "EN: Exact structural level from the validated wave structure.\n"
                    "RU: Точный структурный уровень из подтверждённой волновой структуры."
                ),
            })


    return _unique(
        levels,
        lambda item: (
            str(item.get("kind")),
            str(item.get("scenario")),
            str(item.get("timeframe")),
            round(
                float(item.get("price")),
                6,
            ),
        ),
    )[:30]


def _exact_bar_time(
    payload: dict,
    timeframe: str,
    candidate_time,
) -> str:

    bars = _collect_bars(payload)

    wanted = _time_key(
        candidate_time
    )

    if wanted == datetime.min:
        return ""

    for (
        bar_timeframe,
        bar_time,
    ) in bars:

        if str(bar_timeframe) != str(timeframe):
            continue

        if _time_key(bar_time) == wanted:
            return str(bar_time)

    return ""


def _build_fvg_zones(
    visualization: dict,
    payload: dict,
) -> list[dict]:

    zones = [
        item
        for item in (
            visualization.get("zones")
            or []
        )
        if not (
            isinstance(item, dict)
            and str(
                item.get("kind")
                or ""
            ).strip().lower()
            in {
                "fvg",
                "imbalance",
                "fair_value_gap",
            }
        )
    ]


    for candidate in (
        _deterministic_fvg_candidates(
            payload
        )
    ):

        if not isinstance(
            candidate,
            dict,
        ):
            continue

        if candidate.get(
            "active"
        ) is not True:
            continue

        timeframe = str(
            candidate.get("timeframe")
            or ""
        )

        if timeframe not in {
            "D1",
            "H4",
            "H1",
        }:
            continue

        low = _price(
            candidate.get("price_low")
        )

        high = _price(
            candidate.get("price_high")
        )

        raw_start_time = (
            candidate.get("start_time")
            or candidate.get("formed_at")
            or ""
        )

        start_time = _exact_bar_time(
            payload,
            timeframe,
            raw_start_time,
        )

        if (
            low is None
            or high is None
            or not start_time
        ):
            continue

        zones.append({
            "kind": "fvg",
            "scenario": "primary",
            "timeframe": timeframe,
            "start_time": start_time,
            "end_time": start_time,
            "price_low": min(low, high),
            "price_high": max(low, high),
            "label": (
                f"{timeframe} active FVG"
            ),
        })


    return _unique(
        zones,
        lambda item: (
            str(item.get("kind")),
            str(item.get("timeframe")),
            str(item.get("start_time")),
            float(item.get("price_low")),
            float(item.get("price_high")),
        ),
    )[:20]


def _build_events(
    analysis: dict,
    visualization: dict,
) -> list[dict]:

    events = list(
        visualization.get(
            "market_events"
        )
        or []
    )

    points = _wave_points(
        visualization
    )


    for index, point in enumerate(
        points,
        1,
    ):

        status = str(
            point.get("status")
            or ""
        ).lower()

        label = str(
            point.get("label")
            or ""
        )

        if status == "forming":
            kind = "wave_forming"

        elif (
            "end" in label.lower()
            or "top" in label.lower()
        ):
            kind = "wave_complete"

        else:
            kind = "wave_anchor"


        events.append({
            "event_id": (
                f"LOCAL_WAVE_{index}"
            ),
            "kind": kind,
            "scenario": str(
                point.get("scenario")
                or "primary"
            ),
            "timeframe": str(
                point.get("timeframe")
                or "H1"
            ),
            "time": str(
                point.get("time")
            ),
            "price": float(
                point.get("price")
            ),
            "status": (
                status or "confirmed"
            ),
            "label": label,
            "basis": (
                "EN: Event is anchored to a validated Elliott point.\n"
                "RU: Событие привязано к подтверждённой точке волн Эллиотта."
            ),
        })


    h1_text = str(
        (
            analysis.get(
                "timeframe_analysis"
            )
            or {}
        ).get(
            "H1"
        )
        or ""
    )


    if "CHOCH" in h1_text.upper():

        a_point = next(
            (
                item
                for item in points
                if str(
                    item.get(
                        "timeframe"
                    )
                ) == "H1"
                and str(
                    item.get(
                        "label"
                    )
                ).lower() == "a"
            ),
            None,
        )

        if a_point:

            events.append({
                "event_id": (
                    "LOCAL_H1_CHOCH"
                ),
                "kind": "CHOCH",
                "scenario": "primary",
                "timeframe": "H1",
                "time": str(
                    a_point.get("time")
                ),
                "price": float(
                    a_point.get("price")
                ),
                "status": "confirmed",
                "label": (
                    "H1 bearish CHOCH"
                ),
                "basis": (
                    "EN: Validated H1 analysis explicitly reports a bearish CHOCH at this structural break.\n"
                    "RU: Подтверждённый анализ H1 явно фиксирует медвежий CHOCH на этом структурном пробое."
                ),
            })


    return _unique(
        events,
        lambda item: str(
            item.get("event_id")
        ),
    )[:20]


def _find_structure(
    visualization,
    structure_id,
):

    return next(
        (
            item
            for item in _wave_structures(
                visualization
            )
            if str(
                item.get(
                    "structure_id"
                )
            ) == structure_id
        ),
        None,
    )


def _build_scenario_paths(
    visualization: dict,
) -> list[dict]:

    paths = list(
        visualization.get(
            "scenario_paths"
        )
        or []
    )

    points = _wave_points(
        visualization
    )

    primary_current = next(
        (
            item
            for item in reversed(
                points
            )
            if str(
                item.get("scenario")
            ) == "primary"
            and str(
                item.get("status")
            ).lower() == "forming"
        ),
        None,
    )

    alternate_current = next(
        (
            item
            for item in reversed(
                points
            )
            if str(
                item.get("scenario")
            ) == "alternate"
        ),
        None,
    )

    h1_structure = _find_structure(
        visualization,
        "H1_abc",
    )

    h4_structure = _find_structure(
        visualization,
        "H4_W1_2",
    )


    if primary_current:

        anchor = _price(
            primary_current.get(
                "price"
            )
        )

        h1_confirmation = _price(
            (
                h1_structure
                or {}
            ).get(
                "confirmation_level"
            )
        )

        h4_confirmation = _price(
            (
                h4_structure
                or {}
            ).get(
                "confirmation_level"
            )
        )


        if (
            anchor is not None
            and h1_confirmation is not None
        ):

            paths.append({
                "scenario": "primary",
                "timeframe": str(
                    primary_current.get(
                        "timeframe"
                    )
                ),
                "anchor_time": str(
                    primary_current.get(
                        "time"
                    )
                ),
                "anchor_price": anchor,
                "direction": (
                    "down"
                    if h1_confirmation < anchor
                    else "up"
                ),
                "target_price_low": (
                    h1_confirmation
                ),
                "target_price_high": (
                    h1_confirmation
                ),
                "label": (
                    "Primary: wave (2) decision level"
                ),
            })


        if (
            anchor is not None
            and h4_confirmation is not None
            and h4_confirmation > anchor
        ):

            paths.append({
                "scenario": "primary",
                "timeframe": str(
                    primary_current.get(
                        "timeframe"
                    )
                ),
                "anchor_time": str(
                    primary_current.get(
                        "time"
                    )
                ),
                "anchor_price": anchor,
                "direction": "up",
                "target_price_low": (
                    h4_confirmation
                ),
                "target_price_high": (
                    h4_confirmation
                ),
                "label": (
                    "Primary bullish continuation if support holds"
                ),
            })


    if (
        alternate_current
        and h4_structure
    ):

        anchor = _price(
            alternate_current.get(
                "price"
            )
        )

        target = _price(
            h4_structure.get(
                "invalidation_level"
            )
        )

        if (
            anchor is not None
            and target is not None
            and target < anchor
        ):

            paths.append({
                "scenario": "alternate",
                "timeframe": str(
                    alternate_current.get(
                        "timeframe"
                    )
                ),
                "anchor_time": str(
                    alternate_current.get(
                        "time"
                    )
                ),
                "anchor_price": anchor,
                "direction": "down",
                "target_price_low": target,
                "target_price_high": target,
                "label": (
                    "Alternate bearish continuation"
                ),
            })


    return _unique(
        paths,
        lambda item: (
            str(item.get("scenario")),
            str(item.get("timeframe")),
            str(item.get("anchor_time")),
            str(item.get("direction")),
            float(item.get("target_price_low")),
        ),
    )[:10]


def _h1_rows(
    payload: dict,
) -> list[dict]:

    bars = _collect_bars(
        payload
    )

    result = []

    for (
        timeframe,
        time_value,
    ), bar in bars.items():

        if timeframe != "H1":
            continue

        high = _price(
            bar.get("high")
        )

        low = _price(
            bar.get("low")
        )

        if (
            high is None
            or low is None
        ):
            continue

        result.append({
            "time": str(
                time_value
            ),
            "high": high,
            "low": low,
        })


    result.sort(
        key=lambda item: _time_key(
            item["time"]
        )
    )

    return result[-120:]


def _pivots(
    rows: list[dict],
):

    highs = []
    lows = []


    for index in range(
        1,
        len(rows) - 1,
    ):

        previous = rows[
            index - 1
        ]

        current = rows[
            index
        ]

        following = rows[
            index + 1
        ]


        if (
            current["high"]
            >= previous["high"]
            and current["high"]
            > following["high"]
        ):

            highs.append({
                "time": current["time"],
                "price": current["high"],
            })


        if (
            current["low"]
            <= previous["low"]
            and current["low"]
            < following["low"]
        ):

            lows.append({
                "time": current["time"],
                "price": current["low"],
            })


    return highs, lows


def _build_geometry(
    analysis: dict,
    visualization: dict,
    payload: dict,
):

    trendlines = list(
        visualization.get(
            "trendlines"
        )
        or []
    )

    channels = list(
        visualization.get(
            "channels"
        )
        or []
    )

    patterns = list(
        visualization.get(
            "pattern_shapes"
        )
        or []
    )


    rows = _h1_rows(
        payload
    )

    highs, lows = _pivots(
        rows
    )


    high_pair = (
        highs[-2:]
        if len(highs) >= 2
        else []
    )

    low_pair = (
        lows[-2:]
        if len(lows) >= 2
        else []
    )


    if len(high_pair) == 2:

        trendlines.append({
            "line_id": (
                "LOCAL_H1_RESISTANCE"
            ),
            "kind": "resistance",
            "scenario": "primary",
            "timeframe": "H1",
            "start_time": (
                high_pair[0]["time"]
            ),
            "start_price": (
                high_pair[0]["price"]
            ),
            "end_time": (
                high_pair[1]["time"]
            ),
            "end_price": (
                high_pair[1]["price"]
            ),
            "status": "active",
            "label": (
                "H1 swing-high trendline"
            ),
            "basis": (
                "EN: Deterministic line through two recent confirmed H1 swing highs.\n"
                "RU: Детерминированная линия через два последних подтверждённых максимума H1."
            ),
        })


    if len(low_pair) == 2:

        trendlines.append({
            "line_id": (
                "LOCAL_H1_SUPPORT"
            ),
            "kind": "support",
            "scenario": "primary",
            "timeframe": "H1",
            "start_time": (
                low_pair[0]["time"]
            ),
            "start_price": (
                low_pair[0]["price"]
            ),
            "end_time": (
                low_pair[1]["time"]
            ),
            "end_price": (
                low_pair[1]["price"]
            ),
            "status": "active",
            "label": (
                "H1 swing-low trendline"
            ),
            "basis": (
                "EN: Deterministic line through two recent confirmed H1 swing lows.\n"
                "RU: Детерминированная линия через два последних подтверждённых минимума H1."
            ),
        })


    channel = None


    if (
        len(high_pair) == 2
        and len(low_pair) == 2
    ):

        high_direction = (
            "down"
            if high_pair[1]["price"]
            < high_pair[0]["price"]
            else "up"
        )

        low_direction = (
            "down"
            if low_pair[1]["price"]
            < low_pair[0]["price"]
            else "up"
        )


        if (
            high_direction
            == low_direction
        ):

            channel = {
                "channel_id": (
                    "LOCAL_H1_CHANNEL"
                ),
                "kind": (
                    "descending_channel"
                    if high_direction == "down"
                    else "ascending_channel"
                ),
                "scenario": "primary",
                "timeframe": "H1",

                "upper_start_time": (
                    high_pair[0]["time"]
                ),
                "upper_start_price": (
                    high_pair[0]["price"]
                ),
                "upper_end_time": (
                    high_pair[1]["time"]
                ),
                "upper_end_price": (
                    high_pair[1]["price"]
                ),

                "lower_start_time": (
                    low_pair[0]["time"]
                ),
                "lower_start_price": (
                    low_pair[0]["price"]
                ),
                "lower_end_time": (
                    low_pair[1]["time"]
                ),
                "lower_end_price": (
                    low_pair[1]["price"]
                ),

                "status": "active",

                "breakout_time": "",
                "breakout_price": "",
                "reentry_time": "",
                "reentry_price": "",

                "label": (
                    "H1 local corrective channel"
                ),

                "basis": (
                    "EN: Channel is built locally from two recent H1 swing highs and two recent H1 swing lows.\n"
                    "RU: Канал построен локально по двум последним максимумам и двум последним минимумам H1."
                ),
            }

            channels.append(
                channel
            )


    pattern_text = str(
        analysis.get(
            "patterns"
        )
        or ""
    ).lower()


    if (
        channel
        and (
            "flag" in pattern_text
            or "флаг" in pattern_text
            or "wedge" in pattern_text
            or "клин" in pattern_text
        )
    ):

        times = [
            channel[
                "upper_start_time"
            ],
            channel[
                "upper_end_time"
            ],
            channel[
                "lower_start_time"
            ],
            channel[
                "lower_end_time"
            ],
        ]

        prices = [
            channel[
                "upper_start_price"
            ],
            channel[
                "upper_end_price"
            ],
            channel[
                "lower_start_price"
            ],
            channel[
                "lower_end_price"
            ],
        ]


        structure = _find_structure(
            visualization,
            "H1_abc",
        ) or {}


        h4_structure = _find_structure(
            visualization,
            "H4_W1_2",
        ) or {}


        confirmation = _price(
            structure.get(
                "confirmation_level"
            )
        )

        invalidation = _price(
            h4_structure.get(
                "invalidation_level"
            )
        )


        patterns.append({
            "pattern_id": (
                "LOCAL_H1_CORRECTION_PATTERN"
            ),
            "kind": (
                "flag"
                if (
                    "flag" in pattern_text
                    or "флаг" in pattern_text
                )
                else "wedge"
            ),
            "scenario": "primary",
            "timeframe": "H1",
            "start_time": min(
                times,
                key=_time_key,
            ),
            "end_time": max(
                times,
                key=_time_key,
            ),
            "price_low": min(
                prices
            ),
            "price_high": max(
                prices
            ),
            "status": "developing",
            "confirmation_level": (
                confirmation
                if confirmation
                is not None
                else ""
            ),
            "invalidation_level": (
                invalidation
                if invalidation
                is not None
                else ""
            ),
            "target_price": (
                confirmation
                if confirmation
                is not None
                else ""
            ),
            "label": (
                "H1 corrective flag/channel"
            ),
            "basis": (
                "EN: The validated analysis explicitly identifies a flag/wedge-like H1 correction; geometry is drawn only from real H1 swing bars.\n"
                "RU: Подтверждённый анализ прямо указывает на коррекцию типа флаг/клин H1; геометрия построена только по реальным swing-свечам H1."
            ),
        })


    return (
        _unique(
            trendlines,
            lambda item: str(
                item.get("line_id")
            ),
        )[:20],

        _unique(
            channels,
            lambda item: str(
                item.get("channel_id")
            ),
        )[:10],

        _unique(
            patterns,
            lambda item: str(
                item.get("pattern_id")
            ),
        )[:10],
    )


def enrich_market_visualization(
    analysis: dict,
    payload: dict,
) -> dict:
    """
    $0 deterministic chart enrichment.

    Does not change recommendation, entry, SL, TP,
    risk state or execution state.
    """

    visualization = (
        analysis.setdefault(
            "visualization",
            {},
        )
    )


    before = {
        key: len(
            visualization.get(key)
            or []
        )
        for key in (
            "wave_points",
            "wave_structures",
            "projected_waves",
            "levels",
            "zones",
            "scenario_paths",
            "trendlines",
            "channels",
            "pattern_shapes",
            "market_events",
        )
    }


    visualization[
        "levels"
    ] = _build_levels(
        visualization
    )

    visualization[
        "zones"
    ] = _build_fvg_zones(
        visualization,
        payload,
    )

    visualization[
        "market_events"
    ] = _build_events(
        analysis,
        visualization,
    )

    visualization[
        "scenario_paths"
    ] = _build_scenario_paths(
        visualization
    )


    (
        visualization[
            "trendlines"
        ],
        visualization[
            "channels"
        ],
        visualization[
            "pattern_shapes"
        ],
    ) = _build_geometry(
        analysis,
        visualization,
        payload,
    )


    warnings = (
        sanitize_visualization(
            analysis,
            payload,
        )
    )


    after = {
        key: len(
            visualization.get(key)
            or []
        )
        for key in before
    }


    comment = str(
        visualization.get(
            "chart_comment"
        )
        or ""
    ).strip()


    local_note = (
        "Local $0 visualization: "
        f"levels={after['levels']}, "
        f"zones={after['zones']}, "
        f"paths={after['scenario_paths']}, "
        f"trendlines={after['trendlines']}, "
        f"channels={after['channels']}, "
        f"patterns={after['pattern_shapes']}, "
        f"events={after['market_events']}."
    )


    if local_note not in comment:

        visualization[
            "chart_comment"
        ] = (
            f"{comment} {local_note}"
        ).strip()


    return {
        "before": before,
        "after": after,
        "warnings": list(
            warnings or []
        ),
    }

# ============================================================================
# V8.5.3 LOCAL SWING WAVE OVERLAY
# ============================================================================

_LOCAL_SWING_PREVIOUS_ENRICH = (
    enrich_market_visualization
)


def _local_wave_rows(
    payload: dict,
    timeframe: str,
) -> list[dict]:

    bars = _collect_bars(
        payload
    )

    rows = []


    for (
        bar_timeframe,
        time_value,
    ), bar in bars.items():

        if str(
            bar_timeframe
        ) != str(
            timeframe
        ):
            continue

        high = _price(
            bar.get("high")
        )

        low = _price(
            bar.get("low")
        )

        if (
            high is None
            or low is None
        ):
            continue

        rows.append({
            "time": str(
                time_value
            ),
            "high": high,
            "low": low,
        })


    rows.sort(
        key=lambda item: _time_key(
            item["time"]
        )
    )


    limits = {
        "D1": 220,
        "H4": 240,
        "H1": 260,
        "M15": 260,
        "M5": 300,
    }


    return rows[
        -limits.get(
            timeframe,
            240,
        ):
    ]


def _local_pivots(
    rows: list[dict],
    radius: int,
) -> list[dict]:

    if len(rows) < (
        radius * 2 + 3
    ):

        return []


    raw = []


    for index in range(
        radius,
        len(rows) - radius,
    ):

        current = rows[
            index
        ]

        window = rows[
            index - radius:
            index + radius + 1
        ]


        high_values = [
            item["high"]
            for item in window
        ]

        low_values = [
            item["low"]
            for item in window
        ]


        if (
            current["high"]
            == max(
                high_values
            )
            and high_values.count(
                current["high"]
            ) == 1
        ):

            raw.append({
                "kind": "high",
                "time": current[
                    "time"
                ],
                "price": current[
                    "high"
                ],
            })


        if (
            current["low"]
            == min(
                low_values
            )
            and low_values.count(
                current["low"]
            ) == 1
        ):

            raw.append({
                "kind": "low",
                "time": current[
                    "time"
                ],
                "price": current[
                    "low"
                ],
            })


    raw.sort(
        key=lambda item: (
            _time_key(
                item["time"]
            ),
            0
            if item["kind"] == "low"
            else 1,
        )
    )


    alternating = []


    for pivot in raw:

        if not alternating:

            alternating.append(
                pivot
            )

            continue


        previous = alternating[
            -1
        ]


        if (
            pivot["kind"]
            == previous["kind"]
        ):

            more_extreme = (
                pivot["price"]
                > previous["price"]
                if pivot["kind"] == "high"
                else pivot["price"]
                < previous["price"]
            )


            if more_extreme:

                alternating[
                    -1
                ] = pivot


            continue


        alternating.append(
            pivot
        )


    return alternating


def _local_swing_points_for_tf(
    payload: dict,
    timeframe: str,
) -> list[dict]:

    settings = {

        "D1": {
            "radius": 2,
            "maximum": 8,
        },

        "H4": {
            "radius": 2,
            "maximum": 10,
        },

        "H1": {
            "radius": 2,
            "maximum": 12,
        },

        "M15": {
            "radius": 3,
            "maximum": 12,
        },

        "M5": {
            "radius": 4,
            "maximum": 12,
        },
    }


    config = settings[
        timeframe
    ]


    rows = _local_wave_rows(
        payload,
        timeframe,
    )


    pivots = _local_pivots(
        rows,
        config[
            "radius"
        ],
    )


    # If a very smooth radius produced too few points,
    # fall back to the nearest-neighbour pivot map.
    if len(pivots) < 4:

        pivots = _local_pivots(
            rows,
            1,
        )


    pivots = pivots[
        -config["maximum"]:
    ]


    structure_id = (
        f"LOCAL_SWING_{timeframe}"
    )


    result = []


    for sequence, pivot in enumerate(
        pivots,
        1,
    ):

        result.append({

            "scenario": "primary",

            "degree": timeframe,

            "timeframe": timeframe,

            "sequence": sequence,

            "label": (
                f"S{sequence}"
            ),

            "time": pivot[
                "time"
            ],

            "price": pivot[
                "price"
            ],

            "status": "confirmed",

            "structure_id": (
                structure_id
            ),

            "parent_structure_id": "",

            "parent_wave_id": "",

            # Current market regime is corrective.
            # This is a chart swing overlay, NOT a new Elliott count.
            "wave_type": "correction",
        })


    return result


def _local_swing_structure(
    timeframe: str,
    points: list[dict],
) -> dict | None:

    if len(points) < 2:

        return None


    first_price = _price(
        points[0].get(
            "price"
        )
    )

    last_price = _price(
        points[-1].get(
            "price"
        )
    )


    if (
        first_price is None
        or last_price is None
    ):

        return None


    direction = (
        "up"
        if last_price >= first_price
        else "down"
    )


    return {

        "structure_id": (
            f"LOCAL_SWING_{timeframe}"
        ),

        "parent_structure_id": "",

        "parent_wave_id": "",

        "scenario": "primary",

        "degree": timeframe,

        "timeframe": timeframe,

        "label": (
            f"{timeframe} swing-wave map"
        ),

        "wave_type": "correction",

        "direction": direction,

        "status": "active",

        "current_phase": (
            "deterministic confirmed swing overlay"
        ),

        "confirmation_level": "",

        "invalidation_level": "",

        "summary": (
            "EN: Deterministic swing-wave overlay built only "
            "from confirmed candle highs/lows; it does not "
            "replace the Claude Elliott count.\n"
            "RU: Детерминированная swing-разметка построена "
            "только по подтверждённым максимумам/минимумам "
            "свечей и не заменяет Elliott-разметку Claude."
        ),
    }


def _add_local_swing_waves(
    analysis: dict,
    payload: dict,
) -> dict:

    visualization = (
        analysis.setdefault(
            "visualization",
            {},
        )
    )


    existing_points = list(
        visualization.get(
            "wave_points"
        )
        or []
    )


    existing_structures = list(
        visualization.get(
            "wave_structures"
        )
        or []
    )


    # Re-running enrichment must not duplicate old local swing objects.
    existing_points = [

        item

        for item in existing_points

        if not (
            isinstance(
                item,
                dict,
            )
            and str(
                item.get(
                    "structure_id"
                )
                or ""
            ).startswith(
                "LOCAL_SWING_"
            )
        )
    ]


    existing_structures = [

        item

        for item in existing_structures

        if not (
            isinstance(
                item,
                dict,
            )
            and str(
                item.get(
                    "structure_id"
                )
                or ""
            ).startswith(
                "LOCAL_SWING_"
            )
        )
    ]


    counts = {}


    for timeframe in (
        "D1",
        "H4",
        "H1",
        "M15",
        "M5",
    ):

        points = (
            _local_swing_points_for_tf(
                payload,
                timeframe,
            )
        )


        counts[
            timeframe
        ] = len(
            points
        )


        if len(points) < 2:

            continue


        structure = (
            _local_swing_structure(
                timeframe,
                points,
            )
        )


        existing_points.extend(
            points
        )


        if structure:

            existing_structures.append(
                structure
            )


    visualization[
        "wave_points"
    ] = existing_points


    visualization[
        "wave_structures"
    ] = existing_structures


    return counts


def enrich_market_visualization(
    analysis: dict,
    payload: dict,
) -> dict:

    report = (
        _LOCAL_SWING_PREVIOUS_ENRICH(
            analysis,
            payload,
        )
    )


    counts = (
        _add_local_swing_waves(
            analysis,
            payload,
        )
    )


    warnings = (
        sanitize_visualization(
            analysis,
            payload,
        )
    )


    visualization = (
        analysis.get(
            "visualization"
        )
        or {}
    )


    report[
        "local_swing_waves"
    ] = counts


    report[
        "warnings"
    ] = (
        list(
            report.get(
                "warnings"
            )
            or []
        )
        + list(
            warnings
            or []
        )
    )


    report[
        "after"
    ][
        "wave_points"
    ] = len(
        visualization.get(
            "wave_points"
        )
        or []
    )


    report[
        "after"
    ][
        "wave_structures"
    ] = len(
        visualization.get(
            "wave_structures"
        )
        or []
    )


    comment = str(
        visualization.get(
            "chart_comment"
        )
        or ""
    ).strip()


    swing_note = (
        "Local $0 swing waves: "
        + ", ".join(
            f"{tf}={count}"
            for tf, count
            in counts.items()
        )
        + "."
    )


    if swing_note not in comment:

        visualization[
            "chart_comment"
        ] = (
            f"{comment} {swing_note}"
        ).strip()


    return report
