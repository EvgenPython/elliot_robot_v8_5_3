import math


TIMEFRAMES = {"D1", "H4", "H1", "M30", "M15", "M5"}
TRADE_LEVEL_KINDS = {
    "entry",
    "entry_price",
    "stop",
    "stop_loss",
    "sl",
    "take_profit",
    "target",
    "tp",
}


def _is_price(value) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) > 0.0
    )


def _collect_bars(value, timeframe=None, result=None):
    if result is None:
        result = {}

    if isinstance(value, dict):
        if all(key in value for key in ("time", "open", "high", "low", "close")):
            time_value = str(value.get("time", ""))
            if timeframe in TIMEFRAMES and time_value:
                result[(timeframe, time_value)] = value

        for key, item in value.items():
            next_timeframe = key if key in TIMEFRAMES else timeframe
            _collect_bars(item, next_timeframe, result)

    elif isinstance(value, list):
        for item in value:
            _collect_bars(item, timeframe, result)

    return result


def _price_matches_ohlc(price: float, bar: dict, tolerance: float) -> bool:
    for key in ("open", "high", "low", "close"):
        value = bar.get(key)
        if _is_price(value) and abs(float(price) - float(value)) <= tolerance:
            return True
    return False


def _price_inside_bar(price: float, bar: dict, tolerance: float) -> bool:
    low = bar.get("low")
    high = bar.get("high")
    if not _is_price(low) or not _is_price(high):
        return False
    return float(low) - tolerance <= float(price) <= float(high) + tolerance


def _limit_list(value, maximum: int) -> list:
    return value[:maximum] if isinstance(value, list) else []


def _deterministic_fvg_candidates(payload: dict) -> list[dict]:
    facts = payload.get("deterministic_market_facts") or {}
    imbalances = facts.get("imbalances") or {}
    result = []
    if not isinstance(imbalances, dict):
        return result
    for timeframe, items in imbalances.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            if not item.get("id"):
                continue
            candidate = dict(item)
            candidate["timeframe"] = str(
                candidate.get("timeframe") or timeframe
            )
            result.append(candidate)
    return result


def _matching_fvg(
    item: dict,
    candidates: list[dict],
    tolerance: float,
) -> dict | None:
    timeframe = str(item.get("timeframe", ""))
    # Validation runs at stage assembly and again before archival. Retain
    # the original geometry on the second pass; displayed bounds may already
    # have been shortened by mitigation. Always match the Python candidate.
    validated_id = item.get("fvg_id") if item.get("python_geometry_validated") is True else None
    low = item.get("original_price_low", item.get("price_low")) if validated_id else item.get("price_low")
    high = item.get("original_price_high", item.get("price_high")) if validated_id else item.get("price_high")
    if not _is_price(low) or not _is_price(high):
        return None
    for candidate in candidates:
        if validated_id and candidate.get("id") != validated_id:
            continue
        if str(candidate.get("timeframe", "")) != timeframe:
            continue
        candidate_low = candidate.get("price_low")
        candidate_high = candidate.get("price_high")
        if not _is_price(candidate_low) or not _is_price(candidate_high):
            continue
        if (
            abs(float(low) - float(candidate_low)) <= tolerance
            and abs(float(high) - float(candidate_high)) <= tolerance
        ):
            return candidate
    return None


def sanitize_visualization(
    analysis: dict,
    payload: dict,
) -> list[str]:
    """
    Проверяет только chart metadata. Recommendation и любые торговые поля
    никогда не меняются. Некорректная геометрия удаляется, торговый анализ
    остаётся действительным.
    """
    warnings = []

    try:
        visualization = analysis.get("visualization")
        if not isinstance(visualization, dict):
            return ["visualization отсутствует или имеет неверный тип"]

        bars = _collect_bars(payload)
        point = (
            payload.get("cacheable_history", {})
            .get("symbol_specification", {})
            .get("point", 0.01)
        )
        tolerance = max(float(point or 0.01) * 0.51, 1e-8)

        valid_waves = []
        for item in _limit_list(visualization.get("wave_points"), 80):
            if not isinstance(item, dict):
                warnings.append("wave_point: неверный тип")
                continue
            timeframe = str(item.get("timeframe", ""))
            time_value = str(item.get("time", ""))
            price = item.get("price")
            bar = bars.get((timeframe, time_value))
            if (
                bar is None
                or not _is_price(price)
                or not _price_matches_ohlc(float(price), bar, tolerance)
            ):
                warnings.append(
                    f"wave_point удалён: {timeframe} {time_value} {price}"
                )
                continue
            valid_waves.append(item)
        visualization["wave_points"] = valid_waves

        recommendation = analysis.get("recommendation", {})
        valid_levels = []
        for item in _limit_list(visualization.get("levels"), 80):
            if not isinstance(item, dict) or not _is_price(item.get("price")):
                warnings.append("level удалён: неверная цена/структура")
                continue
            kind = str(item.get("kind", "")).strip().lower()
            if kind in TRADE_LEVEL_KINDS:
                continue
            valid_levels.append(item)

        if recommendation.get("action") in {"enter_long", "enter_short"}:
            canonical_levels = (
                ("entry", "entry_price", "Entry"),
                ("stop_loss", "stop_loss", "Stop Loss"),
                ("take_profit", "take_profit", "Take Profit"),
            )
            for kind, recommendation_key, label in canonical_levels:
                price = recommendation.get(recommendation_key)
                if _is_price(price):
                    valid_levels.append(
                        {
                            "kind": kind,
                            "scenario": "primary",
                            "timeframe": "H1",
                            "price": float(price),
                            "label": label,
                            "basis": "exact recommendation value",
                        }
                    )
        visualization["levels"] = valid_levels

        valid_zones = []
        fvg_candidates = _deterministic_fvg_candidates(payload)
        for item in _limit_list(visualization.get("zones"), 40):
            if not isinstance(item, dict):
                warnings.append("zone удалена: неверный тип")
                continue
            timeframe = str(item.get("timeframe", ""))
            start_time = str(item.get("start_time", ""))
            end_time = str(item.get("end_time", ""))
            low = item.get("price_low")
            high = item.get("price_high")
            if (
                (timeframe, start_time) not in bars
                or (timeframe, end_time) not in bars
                or not _is_price(low)
                or not _is_price(high)
                or float(low) > float(high)
            ):
                warnings.append(
                    f"zone удалена: {timeframe} {start_time}..{end_time}"
                )
                continue
            kind = str(item.get("kind", "")).strip().lower()
            if kind in {"fvg", "imbalance", "fair_value_gap"}:
                candidate = _matching_fvg(item, fvg_candidates, tolerance)
                if candidate is None:
                    warnings.append(
                        "FVG zone удалена: нет точного Python-кандидата "
                        f"{timeframe} {low}..{high}"
                    )
                    continue
                if candidate.get("active") is not True:
                    warnings.append(
                        f"FVG zone удалена: {candidate.get('id')} уже заполнен"
                    )
                    continue
                item = dict(item)
                item.update(
                    {
                        "kind": "fvg",
                        "fvg_id": candidate.get("id"),
                        "direction": candidate.get("direction"),
                        "status": candidate.get("status"),
                        "midpoint": candidate.get("midpoint"),
                        "fill_fraction": candidate.get("fill_fraction"),
                        "active": True,
                        "formed_at": candidate.get("formed_at"),
                        "start_time": candidate.get("start_time") or item.get("start_time"),
                        "original_price_low": candidate.get("price_low"),
                        "original_price_high": candidate.get("price_high"),
                        "price_low": candidate.get(
                            "active_price_low", candidate.get("price_low")
                        ),
                        "price_high": candidate.get(
                            "active_price_high", candidate.get("price_high")
                        ),
                        "decision_relevant": True,
                        "python_geometry_validated": True,
                    }
                )
            valid_zones.append(item)
        visualization["zones"] = valid_zones

        valid_paths = []
        for item in _limit_list(visualization.get("scenario_paths"), 10):
            if not isinstance(item, dict):
                warnings.append("scenario_path удалён: неверный тип")
                continue
            timeframe = str(item.get("timeframe", ""))
            anchor_time = str(item.get("anchor_time", ""))
            anchor_price = item.get("anchor_price")
            target_low = item.get("target_price_low")
            target_high = item.get("target_price_high")
            bar = bars.get((timeframe, anchor_time))
            if (
                bar is None
                or not _is_price(anchor_price)
                or not _price_inside_bar(float(anchor_price), bar, tolerance)
                or not _is_price(target_low)
                or not _is_price(target_high)
                or float(target_low) > float(target_high)
            ):
                warnings.append(
                    f"scenario_path удалён: {timeframe} {anchor_time}"
                )
                continue
            valid_paths.append(item)
        visualization["scenario_paths"] = valid_paths

        def valid_anchor(timeframe, time_value, price):
            bar = bars.get((str(timeframe), str(time_value)))
            return bool(
                bar is not None
                and _is_price(price)
                and _price_inside_bar(float(price), bar, tolerance)
            )

        valid_trendlines = []
        for item in _limit_list(visualization.get("trendlines"), 40):
            if not isinstance(item, dict) or not (
                valid_anchor(item.get("timeframe"), item.get("start_time"), item.get("start_price"))
                and valid_anchor(item.get("timeframe"), item.get("end_time"), item.get("end_price"))
            ):
                warnings.append("trendline удалена: неверные anchors")
                continue
            valid_trendlines.append(item)
        visualization["trendlines"] = valid_trendlines

        valid_channels = []
        for item in _limit_list(visualization.get("channels"), 20):
            timeframe = item.get("timeframe") if isinstance(item, dict) else None
            anchor_pairs = (
                ("upper_start_time", "upper_start_price"),
                ("upper_end_time", "upper_end_price"),
                ("lower_start_time", "lower_start_price"),
                ("lower_end_time", "lower_end_price"),
            )
            if not isinstance(item, dict) or not all(
                valid_anchor(timeframe, item.get(time_key), item.get(price_key))
                for time_key, price_key in anchor_pairs
            ):
                warnings.append("channel удалён: неверные anchors")
                continue
            optional_pairs = (("breakout_time", "breakout_price"), ("reentry_time", "reentry_price"))
            if any(
                item.get(time_key) and item.get(price_key) is not None
                and not valid_anchor(timeframe, item.get(time_key), item.get(price_key))
                for time_key, price_key in optional_pairs
            ):
                warnings.append("channel удалён: неверное breakout/reentry событие")
                continue
            valid_channels.append(item)
        visualization["channels"] = valid_channels

        valid_patterns = []
        for item in _limit_list(visualization.get("pattern_shapes"), 30):
            timeframe = str(item.get("timeframe", "")) if isinstance(item, dict) else ""
            if (
                not isinstance(item, dict)
                or (timeframe, str(item.get("start_time", ""))) not in bars
                or (timeframe, str(item.get("end_time", ""))) not in bars
                or not _is_price(item.get("price_low"))
                or not _is_price(item.get("price_high"))
                or float(item["price_low"]) > float(item["price_high"])
            ):
                warnings.append("pattern_shape удалена: неверная геометрия")
                continue
            valid_patterns.append(item)
        visualization["pattern_shapes"] = valid_patterns

        valid_events = []
        for item in _limit_list(visualization.get("market_events"), 50):
            if not isinstance(item, dict) or not valid_anchor(
                item.get("timeframe"), item.get("time"), item.get("price")
            ):
                warnings.append("market_event удалено: неверная координата")
                continue
            valid_events.append(item)
        visualization["market_events"] = valid_events

        valid_projections = []
        for item in _limit_list(visualization.get("projected_waves"), 20):
            if (
                not isinstance(item, dict)
                or not valid_anchor(item.get("timeframe"), item.get("anchor_time"), item.get("anchor_price"))
                or not _is_price(item.get("target_price_low"))
                or not _is_price(item.get("target_price_high"))
                or float(item["target_price_low"]) > float(item["target_price_high"])
            ):
                warnings.append("projected_wave удалена: неверная геометрия")
                continue
            valid_projections.append(item)
        visualization["projected_waves"] = valid_projections

        structures = []
        seen_structure_ids = set()
        for item in _limit_list(visualization.get("wave_structures"), 30):
            identifier = str(item.get("structure_id", "")).strip() if isinstance(item, dict) else ""
            if not identifier or identifier in seen_structure_ids:
                warnings.append("wave_structure удалена: пустой/повторный structure_id")
                continue
            seen_structure_ids.add(identifier)
            structures.append(item)
        known_ids = {str(item.get("structure_id")) for item in structures}
        visualization["wave_structures"] = [
            item for item in structures
            if not item.get("parent_structure_id")
            or str(item.get("parent_structure_id")) in known_ids
        ]
        if len(visualization["wave_structures"]) != len(structures):
            warnings.append("wave_structure удалена: неизвестный parent_structure_id")

        comment = str(visualization.get("chart_comment", "")).strip()
        if warnings:
            validation_note = (
                f"Python chart validation removed {len(warnings)} invalid object(s)."
            )
            visualization["chart_comment"] = (
                f"{comment} {validation_note}".strip()
            )
        else:
            visualization["chart_comment"] = comment

    except Exception as error:
        warnings.append(
            f"chart validation skipped: {type(error).__name__}: {error}"
        )

    return warnings
