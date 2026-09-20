import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from analysis_schedule import DAILY_BASELINE_FULL_CLOSE_HOUR
from prop_time import FUNDINGPIPS_TZ
from claude_payload import dataframe_to_raw_bars, prepare_current_bar
from entry_watch import load_entry_watch
from market_facts import build_deterministic_market_facts


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug"
DEBUG_SCOUT_PAYLOAD_PATH = DEBUG_DIR / "claude_scout_payload.json"


# ============================================================
# SMALL RAW CONTEXT POLICY
# ============================================================

# Scout НЕ принимает торговое решение.
# Его задача — с высокой чувствительностью определить, нужен ли глубокий FULL.
# Поэтому ему не нужен многомесячный raw history.
SCOUT_CLOSED_BAR_LIMITS = {
    "D1": 2,
    "H4": 5,
    # Покрывает все H1 после дневного baseline 08:00 FP до cutoff 23:00.
    "H1": 16,
    "M30": 24,
    # Младшие TF нужны для свежего setup/event, а не для повторного FULL.
    "M15": 24,
    "M5": 36,
}

TIMEFRAME_ORDER = ["D1", "H4", "H1", "M30", "M15", "M5"]


def _as_fp_datetime(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUNDINGPIPS_TZ)
    else:
        dt = dt.astimezone(FUNDINGPIPS_TZ)

    return dt


def _filter_since_reference(
    df: pd.DataFrame,
    reference_time: datetime | None,
    limit: int,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    result = df

    if reference_time is not None:
        times = pd.to_datetime(result["time_fp"])

        if getattr(times.dt, "tz", None) is None:
            times = times.dt.tz_localize(FUNDINGPIPS_TZ)
        else:
            times = times.dt.tz_convert(FUNDINGPIPS_TZ)

        after_reference = result.loc[times > reference_time]

        # Если после FULL прошло мало времени, добавляем небольшой tail
        # до reference. Это не интерпретация, а только raw continuity.
        if len(after_reference) < min(4, limit):
            result = result.tail(limit)
        else:
            result = after_reference.tail(limit)
    else:
        result = result.tail(limit)

    return result


def build_scout_payload(
    snapshot: dict,
    previous_reference: dict,
) -> dict:
    """
    Формирует компактный raw payload для дешёвого Scout.

    Python ничего не решает о рынке:
    - не строит swing;
    - не определяет regime;
    - не считает Elliott;
    - не создаёт уровни.

    Он только передаёт последний FULL-analysis как reference и
    ограниченный raw tape после/вокруг него.
    """

    if not isinstance(previous_reference, dict):
        raise ValueError("Scout требует предыдущий успешный FULL reference.")

    reference_time = None
    reference_snapshot_time = previous_reference.get(
        "market_snapshot_time_fp"
    )

    if reference_snapshot_time:
        reference_time = _as_fp_datetime(reference_snapshot_time)

    compact_timeframes = {}

    for timeframe_name in TIMEFRAME_ORDER:
        timeframe_data = snapshot["timeframes"][timeframe_name]
        limit = SCOUT_CLOSED_BAR_LIMITS[timeframe_name]

        compact_df = _filter_since_reference(
            timeframe_data["closed_bars"],
            reference_time=reference_time,
            limit=limit,
        )

        compact_timeframes[timeframe_name] = {
            "closed_bars": dataframe_to_raw_bars(compact_df),
            "current_unclosed_bar": prepare_current_bar(
                timeframe_data["current_bar"]
            ),
        }

    tick = snapshot["tick"]

    entry_watch = load_entry_watch()
    deterministic_facts = {
        "entry_projection": {"status": "none"},
        "market": build_deterministic_market_facts(
            snapshot,
            previous_reference,
        ),
    }
    if isinstance(entry_watch, dict):
        reference_timestamp = str(
            previous_reference.get("analysis", {}).get("timestamp") or ""
        )
        source_timestamp = str(entry_watch.get("source_analysis_timestamp") or "")
        if reference_timestamp and source_timestamp == reference_timestamp:
            deterministic_facts["entry_projection"] = {
                "status": str(entry_watch.get("status") or "unknown"),
                "source_analysis_timestamp": source_timestamp,
                "projection": entry_watch.get("projection"),
                "last_checked_bar": entry_watch.get("last_checked_bar"),
                "terminal_event": entry_watch.get("terminal_event"),
            }

    return {
        "instrument": snapshot.get("instrument", "XAUUSD"),
        "timestamp": snapshot.get("generated_at_fp"),
        "timezone": "FundingPips Platform Time",
        "task": "SCOUT_ONLY",
        "policy": {
            "may_open_trade": False,
            "may_define_entry_sl_tp": False,
            "scheduled_daily_full_close_hour_fp": (
                DAILY_BASELINE_FULL_CLOSE_HOUR
            ),
            "daily_reference_same_fp_day_only": True,
            "analysis_suspended_while_position_open": True,
            "goal": (
                "Detect whether the market materially changed or a possible "
                "trade setup may be forming. Escalate to FULL whenever unsure."
            ),
            "fail_open_to_full": True,
        },
        "previous_full_reference": {
            "saved_at_fp": previous_reference.get("saved_at_fp"),
            "market_snapshot_time_fp": previous_reference.get(
                "market_snapshot_time_fp"
            ),
            "h1_closed_bar_time_fp": previous_reference.get(
                "h1_closed_bar_time_fp"
            ),
            "analysis": _compact_reference_analysis(
                previous_reference.get("analysis", {})
            ),
        },
        "deterministic_reference_facts": deterministic_facts,
        "current_market": {
            "bid": float(tick["bid"]),
            "ask": float(tick["ask"]),
            "spread_price": float(tick["spread_price"]),
            "spread_points": int(tick["spread_points"]),
            "last_tick_time_fp": str(tick["time_fp"]),
            "raw_timeframes": compact_timeframes,
        },
        "data_capabilities": {
            "raw_mt5_ohlc": True,
            "broker_tick_volume": True,
            "centralized_order_flow": False,
            "external_news": False,
            "economic_calendar": False,
            "macro_cross_market": False,
        },
    }


def _compact_reference_analysis(analysis: dict) -> dict:
    """Keep the decisions and map needed by Scout without resending prose."""
    if not isinstance(analysis, dict):
        return {}
    result = {
        key: analysis.get(key)
        for key in (
            "timestamp", "instrument", "market_regime", "wave_count",
            "scenario_map", "recommendation", "data_quality",
        )
        if key in analysis
    }
    visualization = analysis.get("visualization")
    if isinstance(visualization, dict):
        result["visualization"] = {
            key: visualization.get(key, [])
            for key in (
                "wave_points", "levels", "zones", "trendlines", "channels",
                "pattern_shapes", "market_events", "projected_waves",
                "wave_structures",
            )
        }
    return result


def save_debug_scout_payload(payload: dict):
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    with open(
        DEBUG_SCOUT_PAYLOAD_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            payload,
            file,
            ensure_ascii=False,
            indent=2,
        )


def print_scout_payload_stats(payload: dict):
    raw = json.dumps(payload, ensure_ascii=False)

    print()
    print("=" * 80)
    print("CLAUDE SCOUT PAYLOAD")
    print("=" * 80)
    print(f"Instrument: {payload.get('instrument')}")
    print(f"FP Time:    {payload.get('timestamp')}")
    print(f"JSON size:  {len(raw.encode('utf-8')) / 1024:.2f} KB")
    print("Mode:       SCOUT ONLY — no Entry/SL/TP authority")
    print("=" * 80)
