import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from analysis_schedule import DAILY_BASELINE_FULL_CLOSE_HOUR
from market_facts import build_deterministic_market_facts
from prop_time import FUNDINGPIPS_TZ


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug"
DEBUG_PAYLOAD_PATH = DEBUG_DIR / "claude_market_payload.json"


# ============================================================
# RAW HISTORY POLICY
# ============================================================

# Эти количества относятся именно к НЕИЗМЕННОЙ исторической базе
# до 00:00 текущего FundingPips-day.
#
# Все бары текущих суток передаются Claude дополнительно как live delta.
# Поэтому каждый разрешённый FULL по-прежнему видит полный сырой
# контекст: historical base + все свежие бары текущего дня + текущие
# незакрытые свечи.
#
# Python ничего не размечает и не интерпретирует: никаких swing,
# трендов, волн, фигур, EMA, ATR, RSI и т.п.
CACHEABLE_BASE_COUNTS = {
    "D1": 250,
    "H4": 300,
    "H1": 360,
    "M30": 240,
    "M15": 192,
    "M5": 144,
}

TIMEFRAME_ORDER = [
    "D1",
    "H4",
    "H1",
    "M30",
    "M15",
    "M5",
]


# ============================================================
# HELPERS
# ============================================================

def _iso(value) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _to_fp_datetime(value) -> datetime:
    """Возвращает timezone-aware datetime в FundingPips Platform Time."""

    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUNDINGPIPS_TZ)
    else:
        dt = dt.astimezone(FUNDINGPIPS_TZ)

    return dt


def _day_start_from_snapshot(snapshot: dict) -> datetime:
    now = _to_fp_datetime(snapshot["generated_at_fp"])
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _raw_bar_from_row(row) -> dict:
    """
    Прямое представление MT5-бара.

    Никаких производных индикаторов или интерпретаций.
    Spread и real_volume включаются только если реально присутствуют
    в данных брокера.
    """

    result = {
        "time": _iso(row["time_fp"]),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "tick_volume": int(row["tick_volume"]),
    }

    if "spread" in row.index:
        result["spread_points"] = int(row["spread"])

    if "real_volume" in row.index:
        result["real_volume"] = int(row["real_volume"])

    return result


def dataframe_to_raw_bars(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []

    return [
        _raw_bar_from_row(row)
        for _, row in df.iterrows()
    ]


def prepare_current_bar(current_bar: dict) -> dict:
    result = {
        "time": str(current_bar["time_fp"]),
        "open": float(current_bar["open"]),
        "high": float(current_bar["high"]),
        "low": float(current_bar["low"]),
        "close": float(current_bar["close"]),
        "tick_volume": int(current_bar["tick_volume"]),
        "is_closed": False,
    }

    if "spread" in current_bar:
        result["spread_points"] = int(current_bar["spread"])

    if "real_volume" in current_bar:
        result["real_volume"] = int(current_bar["real_volume"])

    return result


def split_timeframe_at_day_start(
    timeframe_name: str,
    timeframe_data: dict,
    day_start: datetime,
) -> tuple[dict, dict]:
    """
    Делит сырой tape на две части:

    1) base_history — фиксированное число закрытых баров до 00:00 FP;
       эта часть одинакова для всех H1-анализов в течение суток и может
       безопасно кешироваться Anthropic.

    2) live_delta — все закрытые бары текущих суток + текущая незакрытая
       свеча; эта часть каждый запрос свежая.

    Это только детерминированное разделение по времени, без анализа рынка.
    """

    df = timeframe_data["closed_bars"].copy()

    if df.empty:
        base_df = df
        live_df = df
    else:
        times = pd.to_datetime(df["time_fp"])

        if getattr(times.dt, "tz", None) is None:
            times = times.dt.tz_localize(FUNDINGPIPS_TZ)
        else:
            times = times.dt.tz_convert(FUNDINGPIPS_TZ)

        pre_day_mask = times < day_start

        base_df = df.loc[pre_day_mask].tail(
            CACHEABLE_BASE_COUNTS[timeframe_name]
        )

        live_df = df.loc[~pre_day_mask]

    base_bars = dataframe_to_raw_bars(base_df)
    live_bars = dataframe_to_raw_bars(live_df)

    base = {
        "closed_bars_count": len(base_bars),
        "closed_bars": base_bars,
    }

    live = {
        "closed_bars_since_day_start_count": len(live_bars),
        "closed_bars_since_day_start": live_bars,
        "current_unclosed_bar": prepare_current_bar(
            timeframe_data["current_bar"]
        ),
    }

    return base, live


# ============================================================
# PAYLOAD
# ============================================================

def build_claude_payload(
    snapshot: dict,
    previous_reference: dict | None = None,
) -> dict:
    """
    Строит FULL RAW MARKET CONTEXT для Claude.

    Ключевой принцип проекта:
        Python доставляет объективные данные.
        Claude самостоятельно интерпретирует рынок.

    Поэтому здесь НЕТ вычисленных EMA/ATR/RSI/MACD/ADX,
    заранее найденных swing, уровней, волн, фигур или market regime.
    """

    day_start = _day_start_from_snapshot(snapshot)
    timeframes = snapshot["timeframes"]
    info = snapshot["symbol_info"]
    tick = snapshot["tick"]

    cacheable_history = {}
    live_timeframes = {}

    for timeframe_name in TIMEFRAME_ORDER:
        base, live = split_timeframe_at_day_start(
            timeframe_name=timeframe_name,
            timeframe_data=timeframes[timeframe_name],
            day_start=day_start,
        )

        cacheable_history[timeframe_name] = base
        live_timeframes[timeframe_name] = live

    payload = {
        "instrument": snapshot["instrument"],
        "timestamp": snapshot["generated_at_fp"],
        "timezone": "FundingPips Platform Time UTC+3",

        "analysis_policy": {
            "market_interpretation_owner": "Claude",
            "python_role": "raw_data_transport_only",
            "scheduled_daily_full_close_hour_fp": (
                DAILY_BASELINE_FULL_CLOSE_HOUR
            ),
            "additional_full_is_event_driven": True,
            "independent_full_reanalysis_when_full_runs": True,
            "scout_gates_intermediate_h1": True,
            "analysis_suspended_while_position_open": False,
            "open_position_monitoring": {
                "m15_mode": "cheap_scout_then_event_driven_deep_review",
                "h1_mode": "mandatory_deep_position_review",
                "wave_hierarchy": (
                    "H4_parent -> H1_trade_wave -> M30_decision_bridge -> "
                    "M15_children -> M5_trigger"
                ),
                "automatic_position_close": False,
                "automatic_sl_tightening": True,
                "automatic_fibonacci_tp_recalculation": True,
                "never_widen_stop": True,
            },
            "previous_analysis_is_reference_only": True,
            "important": (
                "Do not assume any previous analysis is correct. "
                "Reconstruct the current market view independently from "
                "the full raw market data supplied in this request."
            ),
        },

        "cache_partition": {
            "version": "daily_raw_history_base_v1",
            "base_cutoff_fp": day_start.isoformat(),
            "meaning": (
                "Legacy field name cacheable_history contains only raw closed "
                "MT5 bars strictly before the current FP day. In the daily "
                "baseline + event-driven policy, Anthropic prompt cache is "
                "disabled for sparse FULL calls; "
                "this partition remains only as a deterministic raw-data split. "
                "live_market contains all newer raw bars and current unclosed "
                "bars. Together they are the complete FULL input context."
            ),
        },

        # Историческая raw base не кешируется Anthropic: scheduled FULL один
        # в сутки, а event-driven FULL заранее не гарантирован в cache TTL.
        "cacheable_history": {
            "data_source": {
                "price_feed": "MetaTrader 5 broker feed",
                "instrument_description": info.get(
                    "description",
                    snapshot["instrument"],
                ),
                "currency_base": info.get("currency_base", ""),
                "currency_profit": info.get("currency_profit", ""),
            },
            "symbol_specification": {
                "digits": int(info.get("digits", 0)),
                "point": float(info.get("point", 0.0)),
                "trade_tick_size": float(info.get("trade_tick_size", 0.0)),
                "trade_tick_value": float(info.get("trade_tick_value", 0.0)),
                "contract_size": float(info.get("contract_size", 0.0)),
                "volume_min": float(info.get("volume_min", 0.0)),
                "volume_max": float(info.get("volume_max", 0.0)),
                "volume_step": float(info.get("volume_step", 0.0)),
                "trade_stops_level_points": int(
                    info.get("trade_stops_level", 0)
                ),
                "trade_freeze_level_points": int(
                    info.get("trade_freeze_level", 0)
                ),
            },
            "closed_market_history_before_day_start": cacheable_history,
        },

        # Эта секция всегда содержит свежую часть raw рынка.
        "live_market": {
            "generated_at_fp": snapshot["generated_at_fp"],
            "last_tick_time_fp": snapshot.get("last_tick_time_fp"),
            "current_price": {
                "bid": float(tick["bid"]),
                "ask": float(tick["ask"]),
                "spread_price": float(tick.get("spread_price", 0.0)),
                "spread_points": int(tick.get("spread_points", 0)),
            },
            "raw_timeframes_since_day_start": live_timeframes,
        },

        "data_capabilities": {
            "raw_ohlc_supplied": True,
            "broker_tick_volume_supplied": True,
            "broker_bar_spread_supplied_when_available": True,
            "broker_real_volume_supplied_when_available": True,
            "python_calculated_indicators_supplied": False,
            "python_detected_patterns_supplied": False,
            "python_detected_swings_supplied": False,
            "macro_cross_market_context_supplied": False,
            "economic_calendar_supplied": False,
            "news_context_supplied": False,
            "centralized_order_flow_supplied": False,
            "warning": (
                "Do not invent data that is not supplied. Tick volume and "
                "broker spread are broker-feed observations, not centralized "
                "exchange order flow."
            ),
        },
        "deterministic_market_facts": build_deterministic_market_facts(
            snapshot,
            previous_reference,
        ),
    }

    return payload


# ============================================================
# JSON SERIALIZATION
# ============================================================

def payload_to_json(payload: dict, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=False, indent=2)

    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def save_debug_payload(
    payload: dict,
    path: Path = DEBUG_PAYLOAD_PATH,
):
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as file:
        json.dump(
            payload,
            file,
            ensure_ascii=False,
            indent=2,
        )

    print("[OK] Claude payload сохранён:")
    print(f"     {path}")


# ============================================================
# CONSOLE STATS
# ============================================================

def print_payload_stats(payload: dict):
    print()
    print("=" * 80)
    print("CLAUDE PAYLOAD — FULL RAW + CACHEABLE HISTORY")
    print("=" * 80)

    print(f"Instrument: {payload['instrument']}")
    print(f"FP Time:    {payload['timestamp']}")
    print(
        "Base cutoff: "
        f"{payload['cache_partition']['base_cutoff_fp']}"
    )

    current_price = payload["live_market"]["current_price"]

    print()
    print(f"Bid:        {current_price['bid']}")
    print(f"Ask:        {current_price['ask']}")
    print(
        f"Spread:     {current_price['spread_price']} "
        f"({current_price['spread_points']} points)"
    )

    print()
    print("RAW BARS SENT TO CLAUDE")
    print("-" * 80)

    base = payload["cacheable_history"][
        "closed_market_history_before_day_start"
    ]
    live = payload["live_market"]["raw_timeframes_since_day_start"]

    for timeframe in TIMEFRAME_ORDER:
        base_count = base[timeframe]["closed_bars_count"]
        live_count = live[timeframe][
            "closed_bars_since_day_start_count"
        ]

        print(
            f"{timeframe}: base={base_count}, "
            f"today_closed={live_count}, current_unclosed=1"
        )

    compact_json = payload_to_json(payload, pretty=False)
    json_kb = len(compact_json.encode("utf-8")) / 1024.0

    cacheable_json = json.dumps(
        payload["cacheable_history"],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    cacheable_kb = len(cacheable_json.encode("utf-8")) / 1024.0

    live_json = json.dumps(
        payload["live_market"],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    live_kb = len(live_json.encode("utf-8")) / 1024.0

    print()
    print(f"Полный payload JSON:   {json_kb:.2f} KB")
    print(f"Cacheable raw history: {cacheable_kb:.2f} KB")
    print(f"Fresh live delta:      {live_kb:.2f} KB")
    print()
    print("[POLICY] Python не рассчитывает торговые индикаторы и не размечает рынок.")
    print("[POLICY] Каждый разрешённый FULL строится независимо по полным raw data.")
    print("=" * 80)
