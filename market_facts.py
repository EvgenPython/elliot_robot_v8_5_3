"""Deterministic market facts shared by Scout and FULL.

This module never labels Elliott waves and never makes a trading decision.
It calculates facts that should not be delegated to an LLM: closed-bar level
relations, broker tick-volume statistics and three-candle imbalances. Claude
must evaluate these FVG facts as one confluence/conflict input to its decision.
"""

from __future__ import annotations

from statistics import median


TIMEFRAMES = ("D1", "H4", "H1", "M30", "M15", "M5")
# M15/M5 imbalances created visual noise and have insufficient structural
# weight for this strategy. D1/H4 are structural; H1 remains an execution
# confluence. Filled candidates stay inactive and are never drawn.
FVG_TIMEFRAMES = ("D1", "H4", "H1")
MAX_LEVELS = 24
MAX_IMBALANCES_PER_TIMEFRAME = 4
MIN_FVG_ATR_FRACTION = 0.03
MIN_FVG_SPREAD_MULTIPLIER = 2.0


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _bars_from_snapshot(snapshot: dict, timeframe: str) -> list[dict]:
    source = snapshot.get("timeframes", {}).get(timeframe, {})
    bars = source.get("closed_bars")
    if bars is None:
        return []
    if hasattr(bars, "to_dict"):
        bars = bars.to_dict("records")
    result = []
    for item in bars if isinstance(bars, list) else []:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        if "time" not in normalized and "time_fp" in normalized:
            normalized["time"] = str(normalized["time_fp"])
        else:
            normalized["time"] = str(normalized.get("time", ""))
        result.append(normalized)
    return result


def _reference_levels(previous_reference: dict | None) -> list[dict]:
    if not isinstance(previous_reference, dict):
        return []
    analysis = previous_reference.get("analysis")
    if not isinstance(analysis, dict):
        return []
    visualization = analysis.get("visualization") or {}
    candidates = []
    for item in visualization.get("levels", []):
        if isinstance(item, dict):
            candidates.append({
                "price": item.get("price"),
                "kind": item.get("kind", "reference"),
                "label": item.get("label", ""),
                "timeframe": item.get("timeframe", "H1"),
            })
    for item in visualization.get("projected_waves", []):
        if not isinstance(item, dict):
            continue
        for field, kind in (("confirmation_level", "confirmation"),
                            ("invalidation_level", "invalidation")):
            candidates.append({
                "price": item.get(field), "kind": kind,
                "label": item.get("label", ""),
                "timeframe": item.get("timeframe", "H1"),
            })
    wave_count = analysis.get("wave_count") or {}
    candidates.append({
        "price": wave_count.get("invalidation_level"),
        "kind": "wave_invalidation", "label": "wave invalidation",
        "timeframe": "H1",
    })
    unique = {}
    for item in candidates:
        price = _number(item.get("price"))
        if price is None:
            continue
        key = (round(price, 8), str(item.get("kind")), str(item.get("label")))
        unique[key] = {**item, "price": price}
    return list(unique.values())[:MAX_LEVELS]


def _level_facts(snapshot: dict, previous_reference: dict | None) -> list[dict]:
    bars = _bars_from_snapshot(snapshot, "H1")
    if not bars:
        return []
    latest = bars[-1]
    previous = bars[-2] if len(bars) > 1 else None
    close = _number(latest.get("close"))
    high = _number(latest.get("high"))
    low = _number(latest.get("low"))
    previous_close = _number(previous.get("close")) if previous else None
    if close is None:
        return []
    result = []
    for item in _reference_levels(previous_reference):
        level = item["price"]
        relation = "above" if close > level else "below" if close < level else "at"
        prior_relation = None
        crossed = "none"
        if previous_close is not None:
            prior_relation = "above" if previous_close > level else "below" if previous_close < level else "at"
            if previous_close <= level < close:
                crossed = "closed_cross_up"
            elif previous_close >= level > close:
                crossed = "closed_cross_down"
        touched = bool(low is not None and high is not None and low <= level <= high)
        result.append({
            **item,
            "closed_time": latest.get("time"),
            "closed_h1_close": close,
            "relation": relation,
            "previous_relation": prior_relation,
            "cross_event": crossed,
            "touched_by_latest_h1": touched,
            "distance_price": round(close - level, 8),
            "confirmation": "one_closed_h1_only" if crossed != "none" else "not_newly_crossed",
        })
    return result


def _volume_facts(snapshot: dict) -> dict:
    bars = _bars_from_snapshot(snapshot, "H1")[-21:]
    volumes = []
    for bar in bars:
        value = _number(bar.get("tick_volume"))
        if value is not None:
            volumes.append(value)
    if not volumes:
        return {"available": False, "scope": "broker_tick_volume_only"}
    latest = volumes[-1]
    baseline_values = volumes[:-1] or volumes
    baseline = float(median(baseline_values))
    ratio = latest / baseline if baseline else None
    if ratio is None:
        classification = "unknown"
    elif ratio >= 2.0:
        classification = "exceptionally_high"
    elif ratio >= 1.35:
        classification = "high"
    elif ratio <= 0.5:
        classification = "low"
    else:
        classification = "normal"
    return {
        "available": True,
        "scope": "broker_tick_volume_only_not_centralized_order_flow",
        "latest_closed_h1_time": bars[-1].get("time"),
        "latest_tick_volume": latest,
        "baseline_median_previous_h1": baseline,
        "sample_size": len(baseline_values),
        "ratio_to_median": round(ratio, 4) if ratio is not None else None,
        "classification": classification,
    }


def _true_range(bar: dict, previous_close: float | None) -> float | None:
    high = _number(bar.get("high"))
    low = _number(bar.get("low"))
    if high is None or low is None:
        return None
    values = [high - low]
    if previous_close is not None:
        values.extend((abs(high - previous_close), abs(low - previous_close)))
    return max(values)


def _atr_at(bars: list[dict], index: int, period: int = 14) -> float | None:
    start = max(0, index - period + 1)
    ranges = []
    for cursor in range(start, index + 1):
        previous_close = (
            _number(bars[cursor - 1].get("close"))
            if cursor > 0
            else None
        )
        value = _true_range(bars[cursor], previous_close)
        if value is not None and value > 0:
            ranges.append(value)
    return sum(ranges) / len(ranges) if ranges else None


def _spread_price(snapshot: dict) -> float:
    tick = snapshot.get("tick") or {}
    try:
        value = float(tick.get("spread_price", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, value)


def _zone_status(
    direction: str,
    low: float,
    high: float,
    later: list[dict],
    latest_close: float | None,
) -> dict:
    size = high - low
    midpoint = low + size / 2.0
    first_mitigated_at = None
    midpoint_tested_at = None
    filled_at = None
    deepest = 0.0

    for bar in later:
        bar_low = _number(bar.get("low"))
        bar_high = _number(bar.get("high"))
        if bar_low is None or bar_high is None:
            continue

        if direction == "bullish":
            penetration = (high - min(high, max(low, bar_low))) / size
            fully_filled = bar_low <= low
        else:
            penetration = (min(high, max(low, bar_high)) - low) / size
            fully_filled = bar_high >= high

        penetration = max(0.0, min(1.0, penetration))
        if penetration > 0 and first_mitigated_at is None:
            first_mitigated_at = bar.get("time")
        if penetration >= 0.5 and midpoint_tested_at is None:
            midpoint_tested_at = bar.get("time")
        deepest = max(deepest, penetration)
        if fully_filled:
            filled_at = bar.get("time")
            deepest = 1.0
            break

    if filled_at:
        status = "filled_inactive"
    elif deepest > 0.5:
        status = "crossed_midpoint_weakened"
    elif deepest >= 0.5:
        rejected = (
            latest_close is not None
            and (
                (direction == "bullish" and latest_close > midpoint)
                or (direction == "bearish" and latest_close < midpoint)
            )
        )
        status = "midpoint_rejected" if rejected else "midpoint_tested"
    elif deepest > 0:
        status = "touched_before_midpoint"
    else:
        status = "untouched"

    # A midpoint test is partial mitigation, not a full fill. Claude may keep
    # the remaining part as relevant context, with the tested/weakened status
    # visible. A fully filled gap never becomes active again.
    active = deepest < 1.0
    if direction == "bullish":
        active_low = low
        active_high = high - (size * deepest)
    else:
        active_low = low + (size * deepest)
        active_high = high

    return {
        "status": status,
        "active": active,
        "active_price_low": round(active_low, 8) if active else None,
        "active_price_high": round(active_high, 8) if active else None,
        "first_mitigated_at": first_mitigated_at,
        "midpoint_tested_at": midpoint_tested_at,
        "filled_at": filled_at,
        "fill_fraction": round(deepest, 4),
        "midpoint": round(midpoint, 8),
    }


def _overlap_fraction(first: dict, second: dict) -> float:
    overlap = max(
        0.0,
        min(first["price_high"], second["price_high"])
        - max(first["price_low"], second["price_low"]),
    )
    smaller = min(first["size_price"], second["size_price"])
    return overlap / smaller if smaller > 0 else 0.0


def _deduplicate_fvg(items: list[dict]) -> list[dict]:
    selected = []
    for item in sorted(
        items,
        key=lambda value: (
            -float(value.get("quality_score", 0.0)),
            str(value.get("formed_at", "")),
        ),
    ):
        duplicate = any(
            item["direction"] == existing["direction"]
            and _overlap_fraction(item, existing) >= 0.8
            for existing in selected
        )
        if not duplicate:
            selected.append(item)
    return selected


def _imbalance_facts(snapshot: dict, timeframe: str) -> list[dict]:
    if timeframe not in FVG_TIMEFRAMES:
        return []

    bars = _bars_from_snapshot(snapshot, timeframe)
    latest_close = _number(bars[-1].get("close")) if bars else None
    spread = _spread_price(snapshot)
    found = []
    for index in range(2, len(bars)):
        first, middle, third = bars[index - 2], bars[index - 1], bars[index]
        first_high, first_low = _number(first.get("high")), _number(first.get("low"))
        third_high, third_low = _number(third.get("high")), _number(third.get("low"))
        middle_open = _number(middle.get("open"))
        middle_close = _number(middle.get("close"))
        if None in (
            first_high,
            first_low,
            third_high,
            third_low,
            middle_open,
            middle_close,
        ):
            continue
        direction = None
        low = high = None
        if third_low > first_high and middle_close > middle_open:
            direction, low, high = "bullish", first_high, third_low
        elif third_high < first_low and middle_close < middle_open:
            direction, low, high = "bearish", third_high, first_low
        if direction is None:
            continue

        # User-defined WaveFrame FVG: the wick-to-wick void must sit inside
        # the real body of the displacement candle.  This rejects the large
        # number of incidental three-candle gaps that cluttered V8.4 charts.
        body_low = min(middle_open, middle_close)
        body_high = max(middle_open, middle_close)
        if body_low > low or body_high < high:
            continue

        size = high - low
        atr = _atr_at(bars, index)
        minimum_size = max(
            spread * MIN_FVG_SPREAD_MULTIPLIER,
            (atr or 0.0) * MIN_FVG_ATR_FRACTION,
        )
        if size < minimum_size:
            continue

        later = bars[index + 1:]
        zone_state = _zone_status(
            direction,
            low,
            high,
            later,
            latest_close,
        )
        if latest_close is None:
            current_relation = "unknown"
            distance_to_zone = None
        elif latest_close < low:
            current_relation = "below"
            distance_to_zone = low - latest_close
        elif latest_close > high:
            current_relation = "above"
            distance_to_zone = latest_close - high
        else:
            current_relation = "inside"
            distance_to_zone = 0.0
        gap_atr_ratio = size / atr if atr else None
        body_size = body_high - body_low
        body_atr_ratio = body_size / atr if atr else None
        quality_score = (
            min(4.0, (gap_atr_ratio or 0.0) * 4.0)
            + min(3.0, (body_atr_ratio or 0.0) * 2.0)
            + (1.0 if zone_state["active"] else 0.0)
        )
        found.append({
            "id": f"fvg_{timeframe}_{third.get('time')}_{direction}",
            "timeframe": timeframe,
            "direction": direction,
            "start_time": first.get("time"),
            "formed_at": third.get("time"),
            "impulse_bar_time": middle.get("time"),
            "price_low": low,
            "price_high": high,
            "size_price": round(size, 8),
            **zone_state,
            "current_relation": current_relation,
            "distance_to_zone": round(distance_to_zone, 8) if distance_to_zone is not None else None,
            "atr_at_formation": round(atr, 8) if atr else None,
            "gap_atr_ratio": round(gap_atr_ratio, 4) if gap_atr_ratio is not None else None,
            "impulse_body_atr_ratio": round(body_atr_ratio, 4) if body_atr_ratio is not None else None,
            "quality_score": round(quality_score, 4),
            "definition": "three_closed_candle_wick_gap_inside_impulse_body",
            "interpretation_status": "python_candidate_requires_claude_validation",
            "display_policy": (
                "conditional_h1_setup_or_position"
                if timeframe == "M15"
                else "always_available_to_claude"
            ),
        })

    unique = _deduplicate_fvg(found)
    active = [item for item in unique if item["active"]]
    inactive = [item for item in unique if not item["active"]]

    active.sort(
        key=lambda item: (
            float("inf")
            if item.get("distance_to_zone") is None
            else float(item["distance_to_zone"])
            / max(float(item.get("atr_at_formation") or 1.0), 1e-12),
            -float(item.get("quality_score", 0.0)),
            str(item.get("formed_at", "")),
        )
    )
    selected = active[:MAX_IMBALANCES_PER_TIMEFRAME]

    # If fewer active candidates exist, one recent retired zone remains only
    # in Claude context for interpreting a completed midpoint reaction/fill.
    # Retired zones are never displayed as active support/resistance.
    if len(selected) < MAX_IMBALANCES_PER_TIMEFRAME and inactive:
        selected.append(
            max(
                inactive,
                key=lambda item: str(
                    item.get("filled_at")
                    or item.get("midpoint_tested_at")
                    or item.get("formed_at")
                    or ""
                ),
            )
        )

    return sorted(selected, key=lambda item: str(item.get("formed_at", "")))


def build_deterministic_market_facts(
    snapshot: dict,
    previous_reference: dict | None = None,
) -> dict:
    return {
        "contract": {
            "interpretation_owner": "Claude",
            "facts_owner": "Python",
            "level_relations_are_authoritative": True,
            "volume_statistics_are_authoritative_for_broker_tick_volume_only": True,
            "imbalances_are_authoritative_geometry": True,
            "imbalances_require_claude_relevance_validation": True,
            "imbalances_are_not_standalone_entry_signals": True,
            "fvg_timeframes": list(FVG_TIMEFRAMES),
            "m30_m15_m5_fvg_disabled": True,
            "m5_fvg_disabled": True,
            "fully_filled_fvg_is_inactive": True,
            "web_draws_only_unmitigated_remainder": True,
            "midpoint_test_is_partial_mitigation_not_full_fill": True,
            "web_must_show_only_claude_validated_fvg": True,
        },
        "reference_level_statuses": _level_facts(snapshot, previous_reference),
        "h1_tick_volume": _volume_facts(snapshot),
        "imbalances": {
            timeframe: _imbalance_facts(snapshot, timeframe)
            for timeframe in TIMEFRAMES
        },
    }
