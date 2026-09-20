"""Two-stage FULL analysis for Claude.

The trading strategy is intentionally not implemented here.  This module
only changes how one frozen FULL market snapshot is analysed:

* FULL_MAP independently reconstructs D1/H4/H1 structure and Elliott waves;
* FULL_DECISION receives the validated map plus H1/M30/M15/M5 execution context,
  explicitly nests M15 waves inside the active H1 wave and returns the
  existing recommendation contract;
* POSITION_REVIEW continues the same nested analysis while a managed position
  is open, but has no authority to send, close or modify an order;
* Python deterministically assembles the same final object that Risk Manager
  and Executor already consume.

Each stage has its own durable retry cycle in ``main.py``.  A lost decision
stream therefore never causes the already validated market map to be bought
again.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
from instruments import active_instrument
from response_contract import named_row, plan_repair, merge_repair
from observability import observe, emit, artifact

SYMBOL = active_instrument()

import anthropic

from chart_contract import sanitize_visualization
from claude_stream_recovery import consume_structured_stream
from claude_client import (
    CLAUDE_RESPONSE_SCHEMA,
    ClaudeInvalidResponseError,
    ClaudeIncompleteResponseError,
    ClaudePermanentRequestError,
    ClaudeRefusalResponseError,
    ClaudeRequestError,
    ClaudeRequestOutcomeUnknownError,
    ClaudeTransientRequestError,
    _anthropic_error_request_id,
    _anthropic_retry_after_seconds,
    _compact_json,
    _translate_api_status_error,
    build_transport_payload,
    create_anthropic_client,
    extract_text_response,
    get_effective_timeout_seconds,
    get_effort,
    get_claude_api_retry_context,
    get_model,
    get_usage_stats,
    load_anthropic_config,
    validate_analysis_contract,
    validate_stop_reason,
    validate_trade_levels,
)
from single_instance import SingleInstanceError, SingleInstanceLock


BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug"
DEBUG_STAGE_ATTEMPTS_DIR = DEBUG_DIR / "claude_staged_attempts"
_COMMON_LOCK_ROOT = Path(os.environ.get("PROGRAMDATA", str(BASE_DIR)))
_LOCK_SYMBOL = "".join(
    character if character.isalnum() else "_" for character in SYMBOL
)
CLAUDE_API_LOCK_PATH = (
    _COMMON_LOCK_ROOT / "WaveFrame" / f"claude_api_{_LOCK_SYMBOL}.lock"
)

STAGED_ANALYSIS_VERSION = "full_staged_v8_5_2_trade_lifecycle_verified"
# Claude Sonnet 5 counts adaptive thinking and final JSON against the same
# max_tokens ceiling.  Two paid production-like tests proved that MAX effort
# can consume the *entire* ceiling (first 48k, then 128k) without emitting a
# single complete Structured Output.  Anthropic's supported control for that
# failure mode is a lower effort level.  We therefore keep the same model,
# complete raw inputs, prompts, schema and validators, but bound reasoning with
# MEDIUM effort so a completed validated answer has priority over runaway
# hidden thinking.  max_tokens remains a ceiling, not a target.
MAP_EFFORT = "medium"
DECISION_EFFORT = "medium"
REPAIR_EFFORT = "medium"
MAP_MAX_TOKENS = 64_000
DECISION_MAX_TOKENS = 48_000
MAP_REPAIR_MAX_TOKENS = 32_000
DECISION_REPAIR_MAX_TOKENS = 24_000
POSITION_REVIEW_MAX_TOKENS = 40_000
POSITION_REVIEW_EFFORT = "medium"
# Stage 1 has already analysed all 360 H1 bars.  Stage 2 receives the latest
# five trading days of H1 (plus the full M30/M15/M5 windows) to verify
# execution timing without buying the same long H1 history twice.
DECISION_H1_CLOSED_BARS = 120

MAP_TIMEFRAMES = ("D1", "H4", "H1")
DECISION_TIMEFRAMES = ("H1", "M30", "M15", "M5")
POSITION_REVIEW_TIMEFRAMES = ("D1", "H4", "H1", "M30", "M15", "M5")

_LAST_STAGE_USAGE: dict[str, dict] = {}
_LAST_STAGE_DIAGNOSTICS: dict[str, dict] = {}
_LAST_WIRE_NORMALIZATION_WARNINGS: list[str] = []


def _schema_properties(*names: str) -> dict:
    source = CLAUDE_RESPONSE_SCHEMA["properties"]
    return {name: copy.deepcopy(source[name]) for name in names}


WAVE_REVISION_SCHEMA = {
    "type": "object",
    "properties": {
        "mode": {
            "type": "string",
            "enum": ["initialize", "unchanged", "extend", "recount"],
        },
        "preserved_anchor_ids": {
            "type": "array",
            "items": {"type": "string"},
        },
        "invalidated_anchor_ids": {
            "type": "array",
            "items": {"type": "string"},
        },
        "reason": {"type": "string"},
    },
    "required": [
        "mode",
        "preserved_anchor_ids",
        "invalidated_anchor_ids",
        "reason",
    ],
    "additionalProperties": False,
}


MARKET_MAP_SCHEMA = {
    "type": "object",
    "properties": {
        **_schema_properties(
            "timestamp",
            "instrument",
            "market_regime",
            "timeframe_analysis",
            "price_structure",
            "patterns",
            "wave_count",
            "higher_timeframe_context",
            "scenario_map",
            "visualization",
            "data_quality",
        ),
        "wave_revision": copy.deepcopy(WAVE_REVISION_SCHEMA),
    },
    "required": [
        "timestamp",
        "instrument",
        "market_regime",
        "timeframe_analysis",
        "price_structure",
        "patterns",
        "wave_count",
        "higher_timeframe_context",
        "scenario_map",
        "visualization",
        "data_quality",
        "wave_revision",
    ],
    "additionalProperties": False,
}


TRADE_DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        **_schema_properties(
            "timestamp",
            "instrument",
            "visualization",
            "recommendation",
            "data_quality",
        ),
        "h1_execution_context": {"type": "string"},
        "microstructure_and_patterns": {"type": "string"},
        "multi_timeframe_relationship": {"type": "string"},
    },
    "required": [
        "timestamp",
        "instrument",
        "h1_execution_context",
        "microstructure_and_patterns",
        "multi_timeframe_relationship",
        "visualization",
        "recommendation",
        "data_quality",
    ],
    "additionalProperties": False,
}


# Flat named string fields keep the provider grammar simple while requiring
# every cell. Local semantic/numeric validation still controls trading.
WIRE_STRING_ROW_SCHEMA = {
    "type": "array",
    "items": {"type": "string"},
}

# Legacy arrays are accepted only when they can be mapped unambiguously.
DATA_QUALITY_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "sufficient": {"type": "string"},
        "issues": {"type": "string"},
    },
    "required": ["sufficient", "issues"],
    "additionalProperties": False,
}

MARKET_REGIME_COLUMNS = (
    "primary_regime",
    "direction",
    "current_phase",
    "phase_status",
    "maturity",
    "location",
    "summary",
)
TIMEFRAME_ANALYSIS_COLUMNS = ("D1", "H4", "H1", "relationship", "summary")
PRICE_STRUCTURE_COLUMNS = (
    "structure_state",
    "swing_structure",
    "key_levels",
    "liquidity_context",
    "summary",
)
WAVE_COUNT_COLUMNS = (
    "structure_type",
    "direction",
    "current_label",
    "current_phase",
    "invalidation_level",
    "alternate_count",
    "summary",
)
HIGHER_TIMEFRAME_CONTEXT_COLUMNS = (
    "d1_trend",
    "d1_wave_context",
    "h4_trend",
    "h4_wave_context",
    "alignment",
    "summary",
)
SCENARIO_MAP_COLUMNS = (
    "primary_scenario",
    "alternate_scenario",
    "expected_path",
    "current_opportunity",
    "next_opportunity",
    "regime_change_trigger",
)
DATA_QUALITY_COLUMNS = ("sufficient", "issues")
RECOMMENDATION_COLUMNS = (
    "action",
    "setup_type",
    "trade_horizon",
    "setup_quality",
    "entry_quality",
    "order_type",
    "entry_price",
    "stop_loss",
    "take_profit",
    "invalidation_level",
    "confidence",
    "why_now",
    "structural_stop_basis",
    "target_basis",
    "reasoning",
    "invalidation_reason",
    "fvg_role",
    "fvg_ids",
    "fvg_basis",
)
WAVE_POINT_COLUMNS = (
    "scenario",
    "degree",
    "timeframe",
    "sequence",
    "label",
    "time",
    "price",
    "status",
    "structure_id",
    "parent_structure_id",
    "parent_wave_id",
    "wave_type",
)
LEVEL_COLUMNS = ("kind", "scenario", "timeframe", "price", "label", "basis")
ZONE_COLUMNS = (
    "kind",
    "scenario",
    "timeframe",
    "start_time",
    "end_time",
    "price_low",
    "price_high",
    "label",
)
SCENARIO_PATH_COLUMNS = (
    "scenario",
    "timeframe",
    "anchor_time",
    "anchor_price",
    "direction",
    "target_price_low",
    "target_price_high",
    "label",
)
TRENDLINE_COLUMNS = (
    "line_id", "kind", "scenario", "timeframe", "start_time",
    "start_price", "end_time", "end_price", "status", "label", "basis",
)
CHANNEL_COLUMNS = (
    "channel_id", "kind", "scenario", "timeframe",
    "upper_start_time", "upper_start_price", "upper_end_time", "upper_end_price",
    "lower_start_time", "lower_start_price", "lower_end_time", "lower_end_price",
    "status", "breakout_time", "breakout_price", "reentry_time", "reentry_price",
    "label", "basis",
)
PATTERN_SHAPE_COLUMNS = (
    "pattern_id", "kind", "scenario", "timeframe", "start_time", "end_time",
    "price_low", "price_high", "status", "confirmation_level",
    "invalidation_level", "target_price", "label", "basis",
)
MARKET_EVENT_COLUMNS = (
    "event_id", "kind", "scenario", "timeframe", "time", "price",
    "status", "label", "basis",
)
PROJECTED_WAVE_COLUMNS = (
    "projection_id", "structure_id", "parent_structure_id", "parent_wave_id",
    "scenario", "degree", "timeframe", "label", "wave_type", "direction",
    "anchor_time", "anchor_price", "target_price_low", "target_price_high",
    "confirmation_level", "invalidation_level", "status", "basis",
)
WAVE_STRUCTURE_COLUMNS = (
    "structure_id", "parent_structure_id", "parent_wave_id", "scenario",
    "degree", "timeframe", "label", "wave_type", "direction", "status",
    "current_phase", "confirmation_level", "invalidation_level", "summary",
)

COMPACT_VISUALIZATION_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "wave_points": {"type": "array", "items": named_row(WAVE_POINT_COLUMNS)},
        "levels": {"type": "array", "items": named_row(LEVEL_COLUMNS)},
        "zones": {"type": "array", "items": named_row(ZONE_COLUMNS)},
        "scenario_paths": {"type": "array", "items": named_row(SCENARIO_PATH_COLUMNS)},
        "trendlines": {"type": "array", "items": named_row(TRENDLINE_COLUMNS)},
        "channels": {"type": "array", "items": named_row(CHANNEL_COLUMNS)},
        "pattern_shapes": {"type": "array", "items": named_row(PATTERN_SHAPE_COLUMNS)},
        "market_events": {"type": "array", "items": named_row(MARKET_EVENT_COLUMNS)},
        "projected_waves": {"type": "array", "items": named_row(PROJECTED_WAVE_COLUMNS)},
        "wave_structures": {"type": "array", "items": named_row(WAVE_STRUCTURE_COLUMNS)},
        "chart_comment": {"type": "string"},
    },
    "required": ['wave_points', 'levels', 'zones', 'scenario_paths', 'trendlines', 'channels', 'pattern_shapes', 'market_events', 'projected_waves', 'wave_structures', 'chart_comment'],
    "additionalProperties": False,
}


POSITION_MANAGEMENT_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": [
                "hold",
                "tighten_stop",
                "recalculate_target",
                "tighten_stop_and_recalculate_target",
                "manual_review",
            ],
        },
        "structure_confirmed": {
            "type": "string",
            "enum": ["true", "false"],
        },
        "position_direction": {
            "type": "string",
            "enum": ["bullish", "bearish", "unclear"],
        },
        "current_m15_wave": {
            "type": "string",
            "enum": [
                "1", "2", "3", "4", "5",
                "A", "B", "C", "W", "X", "Y", "unclear",
            ],
        },
        "stop_reference_wave": {
            "type": "string",
            "enum": ["1", "3", "A", "W", "none"],
        },
        "stop_wave_start_time": {"type": "string"},
        "stop_wave_end_time": {"type": "string"},
        "stop_wave_status": {"type": "string", "enum": ["completed", "unclear", "none"]},
        "stop_anchor_time": {"type": "string"},
        "stop_anchor_price": {"type": "string"},
        "stop_anchor_kind": {
            "type": "string",
            "enum": ["low", "high", "none"],
        },
        "fib_method": {
            "type": "string",
            "enum": [
                "wave3_extension",
                "wave5_projection",
                "correction_b_retracement",
                "correction_c_projection",
                "wxy_y_projection",
                "diagonal_projection",
                "none",
            ],
        },
        "fib_timeframe": {
            "type": "string",
            "enum": ["H1", "M15", "none"],
        },
        "fib_ratio": {"type": "string"},
        "fib_leg_start_time": {"type": "string"},
        "fib_leg_start_price": {"type": "string"},
        "fib_leg_start_kind": {
            "type": "string",
            "enum": ["open", "high", "low", "close", "none"],
        },
        "fib_leg_end_time": {"type": "string"},
        "fib_leg_end_price": {"type": "string"},
        "fib_leg_end_kind": {
            "type": "string",
            "enum": ["open", "high", "low", "close", "none"],
        },
        "fib_projection_time": {"type": "string"},
        "fib_projection_price": {"type": "string"},
        "fib_projection_kind": {
            "type": "string",
            "enum": ["open", "high", "low", "close", "none"],
        },
        "management_reason": {"type": "string"},
    },
    "required": [
        "action",
        "structure_confirmed",
        "position_direction",
        "current_m15_wave",
        "stop_reference_wave",
        "stop_wave_start_time",
        "stop_wave_end_time",
        "stop_wave_status",
        "stop_anchor_time",
        "stop_anchor_price",
        "stop_anchor_kind",
        "fib_method",
        "fib_timeframe",
        "fib_ratio",
        "fib_leg_start_time",
        "fib_leg_start_price",
        "fib_leg_start_kind",
        "fib_leg_end_time",
        "fib_leg_end_price",
        "fib_leg_end_kind",
        "fib_projection_time",
        "fib_projection_price",
        "fib_projection_kind",
        "management_reason",
    ],
    "additionalProperties": False,
}


# POSITION_REVIEW is kept separate from TRADE_DECISION_SCHEMA. It may propose
# protection for the one existing managed position, but it still cannot open,
# add, reverse or close a position. A deterministic Python gate validates every
# raw-bar anchor before any SL/TP request can reach MT5.
POSITION_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "string"},
        "instrument": {"type": "string", "enum": [SYMBOL]},
        "review_trigger": {
            "type": "string",
            "enum": ["position_start", "h1_close", "m15_event"],
        },
        "position_ticket": {"type": "string"},
        "position_status": {
            "type": "string",
            "enum": [
                "healthy",
                "weakened",
                "thesis_invalidated",
                "target_near",
                "unclear",
            ],
        },
        "confidence": {
            "type": "string",
            "enum": ["low", "medium", "high"],
        },
        "h4_structure_and_patterns": {"type": "string"},
        "h1_parent_wave": {"type": "string"},
        "m15_child_structure": {"type": "string"},
        "m5_microstructure": {"type": "string"},
        "support_resistance_by_timeframe": {"type": "string"},
        "patterns_by_timeframe": {"type": "string"},
        "fvg_and_liquidity": {"type": "string"},
        "thesis_health": {"type": "string"},
        "advisory_action": {
            "type": "string",
            "enum": ["hold", "watch_closely", "manual_review", "no_assessment"],
        },
        "position_management": copy.deepcopy(
            POSITION_MANAGEMENT_WIRE_SCHEMA
        ),
        "next_checkpoint": {"type": "string"},
        "summary": {"type": "string"},
        "visualization": copy.deepcopy(COMPACT_VISUALIZATION_WIRE_SCHEMA),
        "data_quality": copy.deepcopy(DATA_QUALITY_WIRE_SCHEMA),
    },
    "required": [
        "timestamp",
        "instrument",
        "review_trigger",
        "position_ticket",
        "position_status",
        "confidence",
        "h4_structure_and_patterns",
        "h1_parent_wave",
        "m15_child_structure",
        "m5_microstructure",
        "support_resistance_by_timeframe",
        "patterns_by_timeframe",
        "fvg_and_liquidity",
        "thesis_health",
        "advisory_action",
        "position_management",
        "next_checkpoint",
        "summary",
        "visualization",
        "data_quality",
    ],
    "additionalProperties": False,
}


def _wire_row_schema() -> dict:
    return copy.deepcopy(WIRE_STRING_ROW_SCHEMA)


MARKET_MAP_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "string"},
        "instrument": {"type": "string"},
        "market_regime": named_row(MARKET_REGIME_COLUMNS),
        "timeframe_analysis": named_row(TIMEFRAME_ANALYSIS_COLUMNS),
        "price_structure": named_row(PRICE_STRUCTURE_COLUMNS),
        "patterns": {"type": "string"},
        "wave_count": named_row(WAVE_COUNT_COLUMNS),
        "higher_timeframe_context": named_row(HIGHER_TIMEFRAME_CONTEXT_COLUMNS),
        "scenario_map": named_row(SCENARIO_MAP_COLUMNS),
        "visualization": copy.deepcopy(COMPACT_VISUALIZATION_WIRE_SCHEMA),
        "data_quality": copy.deepcopy(DATA_QUALITY_WIRE_SCHEMA),
        "wave_revision": copy.deepcopy(WAVE_REVISION_SCHEMA),
    },
    "required": list(MARKET_MAP_SCHEMA["required"]),
    "additionalProperties": False,
}


TRADE_DECISION_WIRE_SCHEMA = {
    "type": "object",
    "properties": {
        "timestamp": {"type": "string"},
        "instrument": {"type": "string"},
        "h1_execution_context": {"type": "string"},
        "microstructure_and_patterns": {"type": "string"},
        "multi_timeframe_relationship": {"type": "string"},
        "visualization": copy.deepcopy(COMPACT_VISUALIZATION_WIRE_SCHEMA),
        "recommendation": named_row(RECOMMENDATION_COLUMNS),
        "data_quality": copy.deepcopy(DATA_QUALITY_WIRE_SCHEMA),
    },
    "required": list(TRADE_DECISION_SCHEMA["required"]),
    "additionalProperties": False,
}


VISUALIZATION_WIRE_INSTRUCTIONS = """
ТЕХНИЧЕСКИЙ COMPACT WIRE-ФОРМАТ V8.5.1 — ИМЕНОВАННЫЕ ПОЛЯ
Каждая запись — JSON object с именами полей из schema. Позиционные массивы
ячеек запрещены. visualization содержит массивы таких объектов.
Все поля записи обязательны. Числа — десятичные строки без единиц и
разделителей тысяч. Для nullable числа используй пустую строку "".
Не сокращай анализ ради формата. Python восстановит числовые типы и проверит
цены по исходным свечам. Не выдумывай отсутствующие факты или координаты.
""".strip()


MARKET_MAP_WIRE_INSTRUCTIONS = f"""
{VISUALIZATION_WIRE_INSTRUCTIONS}

Обязательные именованные поля объектов MARKET MAP (порядок ключей не важен):
market_regime: [{','.join(MARKET_REGIME_COLUMNS)}]
timeframe_analysis: [{','.join(TIMEFRAME_ANALYSIS_COLUMNS)}]
price_structure: [{','.join(PRICE_STRUCTURE_COLUMNS)}]
wave_count: [{','.join(WAVE_COUNT_COLUMNS)}]
higher_timeframe_context: [{','.join(HIGHER_TIMEFRAME_CONTEXT_COLUMNS)}]
scenario_map: [{','.join(SCENARIO_MAP_COLUMNS)}]
data_quality: {{"sufficient":"true","issues":"единая строка"}}.
Если проблем несколько, объедини их внутри ОДНОЙ строки issues через "; ".
wave_count.invalidation_level — десятичная строка или "" для null.
timestamp, instrument, patterns и wave_revision остаются обычными полями.

ОБЯЗАТЕЛЬНЫЙ ЯЗЫКОВОЙ КОНТРАКТ. Каждое объяснение должно содержать обе
полноценные версии `EN: ...\nRU: ...`, включая summary, D1/H4/H1/relationship,
swing_structure, key_levels, liquidity_context, patterns, alternate_count,
все 6 scenario_map, wave_revision.reason, chart_comment и basis/summary всех
графических объектов. Нельзя оставлять русскую часть пустой и нельзя копировать
английский текст после RU. Enum, ID, timestamp, price и короткие wave labels
не переводятся.
""".strip()


TRADE_DECISION_WIRE_INSTRUCTIONS = f"""
{VISUALIZATION_WIRE_INSTRUCTIONS}

recommendation — JSON object со всеми именованными полями:
[{','.join(RECOMMENDATION_COLUMNS)}]
entry_price, stop_loss, take_profit и invalidation_level — десятичные строки
или "" для null. data_quality:
{{"sufficient":"true","issues":"единая строка"}}. Если проблем несколько,
объедини их внутри ОДНОЙ строки issues через "; ". Остальные top-level поля
остаются обычными строками.

ОБЯЗАТЕЛЬНЫЙ ЯЗЫКОВОЙ КОНТРАКТ. Полные `EN: ...\nRU: ...` обязательны в
h1_execution_context, microstructure_and_patterns,
multi_timeframe_relationship, why_now, structural_stop_basis, target_basis,
reasoning, invalidation_reason, chart_comment и basis/summary всех графических
объектов. Русская часть должна быть настоящим переводом той же мысли, а не
пустой строкой и не копией английской. Enum, ID, timestamp, price и wave labels
не переводятся.
""".strip()


POSITION_REVIEW_WIRE_INSTRUCTIONS = f"""
{VISUALIZATION_WIRE_INSTRUCTIONS}

data_quality имеет вид
{{"sufficient":"true","issues":"единая строка"}}. Если проблем несколько,
объедини их в одной строке issues через "; ". Все остальные top-level поля
POSITION_REVIEW являются обычными строками/enums из schema.

position_management — доказательный план сопровождения, а не приказ MT5.
Все price/ratio-поля в нём — десятичные строки без единиц; если соответствующее
действие не требуется, используй пустую строку "", kind/timeframe=none и
fib_method=none. Для переноса стопа укажи stop_wave_start_time, stop_wave_end_time и
stop_wave_status=completed для ПРЕДЫДУЩЕЙ волны (3 -> 1, 5 -> 3, C -> A, Y -> W).
Обе границы должны быть закрытыми M15 свечами. stop_anchor — минимум (BUY)
или максимум (SELL) всего этого отрезка. Если волна не завершена или границы
не подтверждены, action=hold, status=unclear, объясни недостающие признаки.
Время anchor должно точно совпадать со временем закрытой raw
свечи. Цена должна точно совпадать с указанным OHLC этой свечи.

Полные `EN: ...\nRU: ...` обязательны в h4_structure_and_patterns,
h1_parent_wave, m15_child_structure, m5_microstructure,
support_resistance_by_timeframe, patterns_by_timeframe, fvg_and_liquidity,
thesis_health, position_management.management_reason, next_checkpoint,
summary, chart_comment и basis/summary всех графических объектов. Enum, ID,
timestamp, price и wave labels не переводятся.
""".strip()


MARKET_MAP_SYSTEM_PROMPT = """
Ты — первый этап профессионального анализа XAUUSD: MARKET MAP.
Ты НЕ принимаешь торговое решение и не предлагаешь Entry/SL/TP. Твоя задача
— независимо восстановить по полным свежим raw MT5 данным D1/H4/H1 текущий
режим, price action, структуру, ликвидность, паттерны, волны Эллиотта,
основной/альтернативный сценарии и точные координаты структурной карты.

КАЧЕСТВО И ИСТОЧНИК ИСТИНЫ
1. Raw candles текущего frozen snapshot — единственный источник истины.
2. Таблицы bars переданы как columns + rows: порядок значений каждой строки
   точно соответствует columns; ни одна свеча и ни одно число не удалены.
3. Сначала полностью и независимо проанализируй D1 -> H4 -> H1. Только после
   этого сравни результат с previous_confirmed_wave_anchors.
4. Предыдущий анализ не авторитетен и не заменяет свежий анализ. Его можно
   сохранить только там, где текущие raw данные независимо подтверждают его.
5. current_unclosed_bar — незакрытая свеча: используй как текущий контекст,
   но не объявляй её неподтверждённым закрытым сигналом.
6. Tick volume и spread — данные брокерского feed, не централизованный поток.
   Не выдумывай индикаторы, новости, календарь или order flow, которых нет.
7. deterministic_market_facts Python приоритетны. Для каждого уровня используй
   отдельные relation/cross_event/touch; общий статус уровней через `/` запрещён.
8. Объём оценивай только по готовым ratio_to_median/classification; не сравнивай
   незакрытый бар с закрытыми и не придумывай норму.
9. imbalances содержит уже отфильтрованные Python-кандидаты FVG только по
   закрытым свечам. Их геометрия авторитетна, но рыночная значимость ещё не
   подтверждена. Подтверди или отвергни каждый кандидат в контексте Elliott,
   Fibonacci, структуры, ликвидности и паттернов. FVG сам по себе не вход.

ПРОФЕССИОНАЛЬНЫЙ АНАЛИЗ
- Определи primary_regime только из: trend, correction, range, breakout,
  reversal, transition, unclear; direction только bullish, bearish, neutral,
  mixed, unclear.
- phase_status только developing, mature, completing, completed,
  transitioning, failed, unclear. Не смешивай долгосрочный режим и текущую
  фазу: тренд может находиться в коррекции, range может готовить breakout.
- Разбери swings HH/HL/LH/LL, BOS/CHOCH, импульс/коррекцию, поддержки,
  сопротивления, зоны реакции, liquidity sweep/false breakout, зрелость и
  положение цены. Учитывай compression/expansion, relative tick-volume,
  spread, obvious equal highs/lows и локальные/значимые swing extrema.
- Ищи только реально читаемые модели: flag/pennant/channel/triangle,
  breakout+retest, double top/bottom, head-and-shoulders, wedge/diagonal,
  failed breakout, rectangle/range, three-leg или complex correction.
  Паттерн без геометрии и контекста не существует.
- Elliott — важная, но не единственная часть анализа. Дай primary и alternate
  count, текущую волну/фазу и объективную инвалидацию. Не подгоняй count под
  желаемую сделку. Проверяй правила impulse/diagonal, zigzag/flat/triangle,
  W-X-Y/combination, alternation и незавершённость последней волны. Fibonacci
  — только подтверждение уже читаемой структуры, не способ придумать count.
- Построй непротиворечивую связь D1/H4/H1 и primary/alternate scenario map.
  Отличай реальный конфликт TF от нормальной вложенной коррекции.
- Выполни явный checklist на каждом D1/H4/H1: channel, trendline, range,
  triangle, wedge/diagonal, flag/pennant, double top/bottom,
  head-and-shoulders, breakout+retest, false breakout+reentry и liquidity
  sweep. Не заставляй паттерн существовать, но в patterns укажи, что
  проверено, что подтверждено и что отвергнуто.
- Выполни явный imbalance/FVG checklist на D1/H4/H1. Статусы untouched,
  touched_before_midpoint, midpoint_tested/rejected,
  crossed_midpoint_weakened и filled_inactive рассчитаны Python. 50% —
  частичное перекрытие. Оцени реакцию на середину; пройденную середину не
  называй свежей зоной. filled_inactive не рисуй. Python оставит на графике
  лишь неперекрытый участок.
  Оцени, находится ли цена внутри/рядом и есть ли confluence с окончанием
  волны, Fibonacci, ретестом, support/resistance или liquidity sweep.
- Для подтверждённого канала проверь опорные касания обеих границ, параллель,
  breakout, false breakout и возврат внутрь канала.
- Построй иерархию wave_structures сверху вниз. Дочерний structure_id должен
  ссылаться на parent_structure_id и parent_wave_id. Объясни, какие младшие
  1-5 или A-B-C формируют текущую старшую волну. Не применяй механическое
  «любая коррекция = три волны»: проверяй внутреннюю форму каждой ноги —
  zigzag 5-3-5, flat 3-3-5, triangle 3-3-3-3-3, diagonal или W-X-Y.

ПЕРЕНОС ПОДТВЕРЖДЁННЫХ ВОЛН
Каждый previous anchor_id классифицируй ровно один раз: preserved или
invalidated. Preserved не дублируй в wave_points — Python перенесёт его.
Invalidated объясни и при необходимости верни новую точку. mode:
initialize=нет истории, unchanged=без изменений, extend=новые точки,
recount=объективная переразметка. Ошибочную карту не сохраняй.

ВИЗУАЛИЗАЦИЯ
Верни только объекты, вытекающие из анализа. initialize/recount требуют полной
актуальной разметки; unchanged/extend — новых/изменённых точек. Wave point:
реальный timestamp и точный OHLC. Entry/SL/TP здесь запрещены.

WAVE/FIB: label только I..V/A..C для primary, (I)..(V)/(A)..(C) для
intermediate, i..v/a..c для minor/micro; без слов Wave/Sub-wave. Fibonacci —
только подтверждение последней значимой ноги. В levels верни лишь использованные
fib_retracement/fib_extension: label=ratio, bilingual basis=точные anchors и
связь с target/invalidation. Декоративная полная сетка запрещена.

ГРАФИЧЕСКИЙ КОНТРАКТ:
- wave_structures: иерархия D1/H4/H1, короткие стабильные IDs;
- wave_points: родительские IDs, wave_type impulse/correction/diagonal;
- projected_waves: только обоснованная C/5, target/confirmation/invalidation;
- линии, каналы, фигуры и события используют точные свечи/цены;
- события: breakout/retest/false_breakout/reentry/sweep/BOS/CHOCH;
- только подтверждённые и действительно значимые active=true FVG верни как
  zones с kind=fvg, точными price_low/price_high Python-кандидата и отличимым
  label с timeframe; неподтверждённые кандидаты не рисуй;
- basis кратко объясняет практический смысл.

Все narrative-поля верни двуязычно в одной строке строго как
EN: <English>\nRU: <Русский>. Числа, enum, time, IDs и wave labels не переводи.
Обе части описывают одну оценку; второго анализа для перевода нет.
Русская часть должна быть естественной профессиональной речью. Запрещены
непереведённые служебные слова stay_out, reference, structure_state,
relationship, raw tape, swing high/low и буквальные машинные enum. Используй:
«вне рынка», «предыдущая карта», «состояние структуры», «связь таймфреймов»,
«сырые рыночные данные», «максимум/минимум колебания».

Ответ — только Structured JSON по заданной schema. Поля должны быть содержательны,
но без повторения одного и того же объяснения в нескольких разделах.
""".strip()
MARKET_MAP_SYSTEM_PROMPT = MARKET_MAP_SYSTEM_PROMPT.replace("XAUUSD", SYMBOL)


TRADE_DECISION_SYSTEM_PROMPT = """
Ты — второй этап профессионального анализа XAUUSD: TRADE DECISION.
Ты получаешь (1) validated_market_map, построенную первым этапом по полным
D1/H4/H1 raw данным того же frozen snapshot, и (2) raw H1/M30/M15/M5 данные для
точного исполнения. Твоя задача — проверить точку входа на младших TF и выдать
тот же строгий торговый контракт, который использует действующая стратегия.

deterministic_market_facts имеют приоритет для статусов уровней, относительного
тикового объёма и геометрии FVG. FVG оценивай только на D1/H4/H1;
M30/M15/M5 FVG отключены как слишком шумные для этой стратегии.
FVG участвует в решении как confluence,
entry zone, target, invalidation или conflict, но не создаёт сделку в одиночку.
В recommendation.fvg_ids и zones разрешены только кандидаты active=true.
midpoint_tested/rejected — частично перекрытые зоны, crossed_midpoint_weakened
требует отдельного обоснования актуальности. filled_inactive не рисуй.
Верни исходные price_low/price_high кандидата без изменения: Python сам
сократит прямоугольник до неперекрытого остатка и сохранит исходную середину.
Каждый confirmation/invalidation
уровень описывай отдельно: касание, закрытие, закрепление и ретест — разные
события.

КОНТЕКСТ И КАЧЕСТВО
1. validated_market_map — обязательный старший контекст этого же snapshot.
   Не начинай независимую альтернативную стратегию и не переписывай карту без
   данных. M30/M15/M5 могут подтвердить вход, ухудшить entry_quality или привести
   к stay_out, но не должны искусственно менять D1/H4 структуру.
2. Raw таблицы bars имеют формат columns + rows. Все числа сохранены точно.
3. current_unclosed_bar не является закрытым подтверждением. Не выдумывай
   отсутствующие индикаторы, новости, календарь или централизованный order flow.
4. Стабильность не означает обязательную сделку: качественный stay_out лучше
   слабого, позднего или плохо защищённого входа.

ОЦЕНКА SETUP
- Сопоставь D1/H4/H1 map с H1/M30/M15/M5: импульс/коррекция, swings, слом/защита
  структуры, pattern, liquidity/false breakout, текущая цена и spread.
- Используй M30 как промежуточный decision bridge между H1 и M15: оцени на
  нём support/resistance, фигуры, BOS/CHOCH, breakout/retest и зрелость setup.
- Разложи текущую родительскую H1-волну на дочернюю структуру M15, а M15 — на
  M5 только там, где pivots объективно читаются. Не начинай независимый count
  младшего TF без parent_structure_id и parent_wave_id.
- Выполни отдельный checklist на H1, M30, M15 и M5: support/resistance, trendline,
  channel, range/rectangle, triangle, flag/pennant, wedge/diagonal,
  double top/bottom, head-and-shoulders, breakout+retest, false breakout,
  reentry, BOS/CHOCH и liquidity sweep. В microstructure_and_patterns явно
  раздели выводы по H1, M30, M15 и M5; не смешивай масштабы в одну фразу.
- Для каждой реально подтверждённой модели укажи timeframe, геометрию,
  текущий статус, confirmation, invalidation и достижимую цель. Если фигура
  была проверена, но отвергнута, не рисуй её и коротко объясни отказ.
- Допустимые setup_type: trend_pullback, wave3_continuation,
  wave5_continuation, correction_a_leg, correction_b_leg, correction_c_leg,
  correction_completion, range_long, range_short, range_breakout,
  breakout_retest, false_breakout_reversal, trend_reversal,
  diagonal_reversal, pattern_continuation, pattern_reversal,
  transition_trade, other, no_trade.
- trade_horizon: intraday, swing, multi_day, unclear.
- setup_quality: weak, acceptable, good, excellent. entry_quality: poor, fair,
  good, excellent. confidence: low, medium, high.
- Не запрещай торговлю коррекции или range автоматически. Различай активную
  фазу и её завершение. correction_a/b/c_leg нельзя торговать, если map
  phase_status completed/failed; correction_completion допустим только при
  completing/completed/transitioning.
- H1-сделка против D1/H4 допустима только как ясно читаемая коррекционная или
  разворотная фаза с хорошей location, близкой объективной инвалидацией и
  реалистичной целью. В range ищи границы, false breakout или подтверждённый
  breakout/retest; середина range без отдельного сильного edge => stay_out.
- Оцени current setup отдельно от next opportunity. Не входи поздно только
  потому, что направление верно. weak setup или poor entry => stay_out.
- setup_quality оценивает весь edge: режим/фазу, maturity, structure/pattern,
  TF alignment, location, stop geometry, достижимый target, фактический
  reward/risk, volatility/spread, alternate scenario и data quality.
- recommendation.fvg_role обязателен: confirmation, entry_zone, target,
  invalidation, conflict, neutral или no_relevant_fvg. В fvg_ids перечисли
  точные deterministic IDs через запятую, а в bilingual fvg_basis объясни,
  как FVG повлиял на вход/отказ. Если FVG конфликтует с направлением или делает
  вход поздним, action должен быть stay_out. Каждый перечисленный ID обязан
  быть нарисован одной zone kind=fvg с точными Python-границами. Не выдумывай
  FVG сверх Python facts и не показывай отвергнутые Python-кандидаты.

ACTION И УРОВНИ
- action только enter_long, enter_short или stay_out.
- Для stay_out обязательно: setup_type=no_trade, order_type=none,
  entry_price=null, stop_loss=null, take_profit=null,
  invalidation_level=null. Чётко объясни, чего не хватает и что ждать.
- Для входа order_type только market, limit или stop; все Entry/SL/TP и
  invalidation_level обязательны. LONG: SL < Entry < TP. SHORT: TP < Entry < SL.
- market — вход около текущего Bid/Ask; limit — более выгодный откат/ретест;
  stop — вход только после пробоя/подтверждения. Не путай их геометрию.
- Stop Loss ставь за объективной структурной инвалидацией, а не по удобному
  расстоянию и не внутри структуры ради меньшего риска. Учитывай tick size,
  broker stop constraints и spread. Take Profit — у достижимой цели именно
  торгуемой фазы: structure/liquidity/range boundary/wave projection.
- recommendation.invalidation_level — уровень, после которого торговая идея
  неверна; он может совпадать со SL, но не выбирается произвольно.
- Не рассчитывай lot size, FundingPips лимиты или денежный риск: после ответа
  неизменённый Python Risk Manager решит, разрешена ли сделка.

ВИЗУАЛИЗАЦИЯ
Верни только новые execution/micro wave_points и актуальные M30/M15/M5 levels,
zones, paths, trendlines, channels, pattern_shapes, market_events,
projected_waves и wave_structures. Не копируй preserved старшие точки из map: Python объединит их
детерминированно. Wave point обязан ссылаться на существующую raw свечу и иметь
цену ровно одного из её OHLC. Не придумывай координаты. Торговые уровни должны
в точности совпадать с recommendation; Python дополнительно канонизирует их.
Для каждого M15 и M5: если существует объективно читаемая execution/micro
волновая структура, верни не одиночный pivot, а минимум две связанные точки
одного degree/scenario с последовательными sequence. Если честного count нет,
верни для этого timeframe ноль wave_points и явно объясни это в chart_comment;
никогда не создавай недостающие точки ради заполнения графика. Даже без
волнового count верни подтверждённые структурные levels/zones/paths, если они
реально следуют из raw данных.
Использованный для Entry/SL/TP Fib верни в levels как fib_retracement или
fib_extension: label=ratio, bilingual basis=anchors и связь с уровнем; без сетки.

Все narrative-поля верни двуязычно в одной строке строго как
EN: <English>\nRU: <Русский>. Числа, enum, time, IDs и wave labels не переводи.
Обе части передают одно решение; перевод не является вторым анализом.

Ответ — только Structured JSON по schema. reasoning должен быть глубоким и
связным, но без дублирования уже принятой validated_market_map.
""".strip()
TRADE_DECISION_SYSTEM_PROMPT = TRADE_DECISION_SYSTEM_PROMPT.replace("XAUUSD", SYMBOL)


MARKET_MAP_REPAIR_SYSTEM_PROMPT = """
Ты — строго ограниченный REPAIR-этап MARKET MAP для XAUUSD.

Первичный анализ уже выполнен и оплачен. Не выполняй новый независимый
анализ и не меняй торговую стратегию. Исправь только перечисленную локальную
ошибку контракта/семантической проверки в supplied_invalid_result.

Обязательные правила:
- сохрани рыночную оценку, режим, направление, wave count и сценарии первичного
  результата, если конкретная validation_error не требует их исправления;
- instrument и timestamp возьми из immutable_facts;
- каждый previous anchor классифицируй ровно один раз как preserved или
  invalidated; неизвестные anchor_id запрещены;
- не добавляй Entry/SL/TP и не создавай новую торговую рекомендацию;
- верни полный MARKET_MAP строго по приложенной COMPACT WIRE schema;
- если исправление требует выбора, используй наиболее консервативный вариант,
  который не выдумывает отсутствующие рыночные данные.

Сохрани bilingual narrative: EN: <English>\nRU: <Русский>.
Ответ — только Structured JSON.
""".strip()


TRADE_DECISION_REPAIR_SYSTEM_PROMPT = """
Ты — строго ограниченный REPAIR-этап TRADE DECISION для XAUUSD.

Первичный анализ уже выполнен и оплачен. Сохрани validated_market_map и
логику действующей стратегии. Исправь только validation_error первичного
  trade decision, используя supplied_invalid_result и компактные неизменяемые
  факты последних закрытых свечей того же frozen snapshot. Не выполняй новый
  полный анализ истории.

Обязательные правила:
- не меняй направление/идею без необходимости, прямо вызванной ошибкой;
- не придумывай сделку ради заполнения полей: если безопасно исправить Entry,
  SL, TP или контракт нельзя, верни полноценный stay_out по исходной schema;
- для входа сохрани строгую геометрию LONG/SHORT и структурное основание SL/TP;
- current_unclosed_bar не является закрытым подтверждением;
- верни полный TRADE_DECISION строго по приложенной COMPACT WIRE schema.

Сохрани bilingual narrative: EN: <English>\nRU: <Русский>.
Ответ — только Structured JSON.
""".strip()


ENTRY_CHECK_SYSTEM_PROMPT = """
Ты — короткий финальный ENTRY CHECK для XAUUSD. Полный D1/H4/H1 анализ и
условный план уже оплачены и переданы в validated_market_map. Не перестраивай
старшую карту и не запускай новый широкий анализ. По свежим закрытым H1/M30/M15/M5
проверь только фактическое срабатывание ранее заданного триггера, качество
текущего входа, сохранность структурной инвалидации и достижимость цели.

Разреши enter_long/enter_short только если триггер подтверждён закрытой
свечой, план не устарел, цена не ушла слишком далеко, stop остаётся за
структурой, а reward/risk не ухудшился. Иначе верни stay_out и чётко объясни
причину. Не создавай новую идею, направление или старшую волновую карту.
Верни свежие M30/M15/M5 events и точки только если они объективно подтверждаются
raw OHLC. Все narrative-поля строго EN: <English>\nRU: <Русский>.
Ответ — только Structured JSON по заданной schema.
""".strip()

ENTRY_CHECK_MAX_TOKENS = 24_000
ENTRY_CHECK_EFFORT = "medium"


H1_DECISION_REFRESH_SYSTEM_PROMPT = """
Ты — профессиональный H1 DECISION REFRESH для XAUUSD. Полная D1/H4/H1
рыночная карта текущего торгового дня уже проверена и передана в
validated_market_map. Не перестраивай старшие D1/H4 волны без отдельного FULL,
но на каждой новой закрытой H1 заново оцени, возник ли сейчас исполнимый setup.

Используй свежие закрытые H1/M30/M15/M5 свечи. M30 — дополнительный decision
clock между часовыми закрытиями; H1 остаётся структурным владельцем идеи.
Разрешено сформировать новую
enter_long/enter_short идею, если она согласуется с primary или явно указанным
alternate scenario старшей карты и имеет объективные confirmation,
invalidation, Entry, SL и достижимый TP. Это самостоятельная проверка текущего
входа: отсутствие старого conditional trigger не запрещает новый setup.

Проверь trend pullback, начало/продолжение wave 3 или 5, завершение коррекции,
активную A/B/C-волну, границы range, breakout/retest, false breakout,
liquidity sweep, подтверждённые фигуры и актуальные D1/H4/H1 FVG. Не выдумывай
фигуры и координаты. M5 только уточняет timing. Не входи в середине шума или
после уже ушедшего движения. Если условия не готовы, верни stay_out и укажи
конкретное ожидаемое событие, чтобы следующая H1 могла проверить его снова.

Не предпочитай stay_out ради осторожного стиля ответа и не создавай сделку
ради частоты. Решение должно следовать данным и одинаковым правилам на каждой
H1. Все narrative-поля строго EN: <English>\nRU: <Русский>.
Ответ — только Structured JSON по заданной schema.
""".strip()

H1_DECISION_REFRESH_MAX_TOKENS = 32_000
H1_DECISION_REFRESH_EFFORT = "medium"


M30_DECISION_REFRESH_SYSTEM_PROMPT = """
Ты — профессиональный M30 DECISION REFRESH для XAUUSD на DEMO-счёте.
Validated D1/H4/H1 market map уже построена. H1 остаётся владельцем структуры,
а только что закрытая промежуточная M30 даёт дополнительную возможность
проверить вход между часовыми закрытиями.

Не объявляй незакрытую H1 подтверждённой. Используй её лишь как live context.
По закрытым M30/M15/M5 проверь location, поддержку/сопротивление, BOS/CHOCH,
ликвидность, фигуры продолжения/разворота, breakout/retest, false breakout и
вложенную M15-структуру текущей H1-волны. M5 только уточняет trigger/timing.
FVG разрешены как confluence только на D1/H4/H1; M30/M15/M5 FVG не считай.

Разреши enter_long/enter_short лишь при объективных Entry/SL/TP,
структурной инвалидации, не ушедшей цене и приемлемом reward/risk. Отсутствие
старого conditional trigger не запрещает новый качественный setup. Если
условия не готовы, верни полноценный schema-valid stay_out и конкретное
следующее подтверждение. Не выбирай stay_out просто из общей осторожности и
не создавай сделку ради частоты.

Все narrative-поля строго EN: <English>\nRU: <Русский>. Ответ — только
Structured JSON по заданной schema.
""".strip().replace("XAUUSD", SYMBOL)

M30_DECISION_REFRESH_MAX_TOKENS = 28_000
M30_DECISION_REFRESH_EFFORT = "medium"


POSITION_REVIEW_SYSTEM_PROMPT = """
Ты — POSITION REVIEW аналитического торгового робота XAUUSD. В MT5 уже есть
реальная управляемая позиция. Твоя задача — продолжать профессионально читать
рынок, проверять исходную торговую гипотезу и формировать доказательный план
защиты уже открытой позиции. Ты НЕ имеешь права создавать новую сделку,
доливаться, разворачивать или закрывать позицию. Ты не вызываешь Executor:
Python отдельно проверит каждую координату и только затем сможет подтянуть SL
или пересчитать TP. Advisory остаётся hold, watch_closely, manual_review или
no_assessment.

ИЕРАРХИЯ ВОЛН — ОБЯЗАТЕЛЬНО
1. D1 задаёт широкий фон, H4 — родительский контекст для рабочей H1-идеи.
2. Сначала установи, какая конкретная H1-волна является родительской для
   движения, которое торгуется сейчас, и сохрани её связь с H4.
3. Затем разложи ТУ ЖЕ H1-волну на дочерние волны M15. Если H1-волна
   импульсная, проверь честную последовательность 1-2-3-4-5. Если она
   коррекционная, не применяй механическое «везде три волны»: учитывай
   zigzag 5-3-5, flat 3-3-5, triangle 3-3-3-3-3, diagonal и W-X-Y.
4. M5 используй только для уточнения текущей M15-подволны и ближайшего
   подтверждения/инвалидации. Шум одной M5-свечи не отменяет H1 без структуры.
5. Не создавай независимый M15/M5 count: каждая младшая structure/wave должна
   иметь parent_structure_id и parent_wave_id. Если pivots не читаются честно,
   прямо укажи неопределённость и не дорисовывай недостающие волны.

ПРОФЕССИОНАЛЬНЫЙ CHECKLIST ПО КАЖДОМУ ТАЙМФРЕЙМУ
Отдельно на H4, H1 и M15 проверь и опиши:
- фактические support/resistance, swing highs/lows, равные вершины/минимумы;
- trendline, parallel channel, range/rectangle, compression/expansion;
- triangle, flag, pennant, wedge/diagonal;
- double top/bottom, head-and-shoulders и inverse H&S;
- breakout, close beyond level, retest, false breakout, reentry, BOS/CHOCH,
  liquidity sweep и реакцию;
- D1/H4/H1 FVG; M30/M15/M5 FVG не считай;
- tick-volume только как относительную активность broker feed, не order flow.
Python уже передаёт точную геометрию и статус FVG. Выбирай только active=true
и явно обоснуй актуальность частично перекрытых зон. Тест/отбой от середины
не равен полному заполнению; crossed_midpoint_weakened не является свежим
FVG. filled_inactive доступен лишь как история и на график не возвращается.
Верни исходные границы кандидата; Python нарисует неперекрытый остаток.
Не заставляй фигуру существовать. Для каждой подтверждённой модели укажи
геометрию, статус, confirmation, invalidation и достижимую цель. Отдельно
назови проверенные, но отвергнутые модели, если это важно для вывода.

ПРОВЕРКА ОТКРЫТОЙ ПОЗИЦИИ
- Сравни свежие закрытые свечи с original_trade_thesis, фактическими
  Entry/SL/TP и последним position review.
- position_status=healthy: структура и ожидаемая вложенность сохраняются.
- weakened: гипотеза ещё жива, но появились объективные противоречия.
- thesis_invalidated: закрытая свеча/структура объективно разрушила идею;
  advisory_action должен быть manual_review, но это НЕ команда закрытия.
- target_near: цена вошла в целевую/терминальную область и младшая структура
  показывает завершение; это также только уведомление.
- unclear: данных недостаточно или counts равновероятны.

СОПРОВОЖДЕНИЕ ПО ВОЛНАМ И ФИБОНАЧЧИ
- Используй только подтверждённую структуру закрытых H1/M15 свечей. Незакрытая
  свеча может быть контекстом, но не подтверждает новую волну и не разрешает
  изменение защиты.
- Правило владельца стратегии для импульса: когда развивается M15 wave 3,
  защитный anchor — экстремум завершённой wave 1; когда развивается wave 5 —
  экстремум завершённой wave 3. Для bullish это минимум соответствующей волны,
  для bearish — максимум. Для correction C используй экстремум A, для Y —
  экстремум W. На 1/2/4/A/B/W/X или при спорном count автоматический перенос
  стопа не предлагай.
- stop_anchor_time обязан быть точной закрытой M15 свечой, а stop_anchor_price
  — ровно её low для bullish или high для bearish. Стоп ставится сразу за
  этим экстремумом предыдущей волны; ATR/spread-запас запрещён. Python добавит
  только один минимальный шаг цены и учтёт обязательную дистанцию брокера.
- TP рассчитывай по реальной Elliott/Fibonacci геометрии, а не по ближайшему
  красивому числу. Допустимые планы: wave3_extension (1.0/1.618/2.618),
  wave5_projection (0.618/1.0/1.618), correction_b_retracement
  (0.382/0.5/0.618/0.786), correction_c_projection (1.0/1.272/1.618),
  wxy_y_projection (1.0/1.618), diagonal_projection (0.618/1.0).
- Для Fib укажи три точных raw anchors: начало и конец измеряемой ноги и pivot,
  от которого делается проекция. Python сам вычислит target как длину ноги,
  умноженную на ratio, от projection pivot по направлению позиции.
- structure_confirmed=true разрешено только при однозначной вложенности H1 ->
  M15, точных raw anchors и confidence=high. При конфликте H4/H1/M15,
  альтернативном count той же вероятности, недостатке истории или сомнительной
  фигуре верни hold/manual_review и пустые координаты.
- Даже подтверждённый план никогда не разрешает расширить первоначальный риск:
  Python примет только более защитный SL. Не предлагай удаление SL/TP.

ВИЗУАЛИЗАЦИЯ
Верни только актуальные и подтверждённые H4/H1/M30/M15/M5 объекты: связанные
wave_structures и wave_points, уровни, зоны/FVG, trendlines, channels,
pattern_shapes, market_events и projected_waves. Все координаты должны
совпадать с реальными raw OHLC. Новый SL/TP не рисуй отдельными торговыми
уровнями: доказательные anchors уже находятся в position_management.

Каждое аналитическое текстовое поле должно содержать полноценные версии
строго `EN: <English>\nRU: <Русский>`. В support_resistance_by_timeframe и
patterns_by_timeframe явно используй секции H4, H1, M15. Ответ — только
Structured JSON. Полнота проверки важнее красивой уверенности.
""".strip()
POSITION_REVIEW_SYSTEM_PROMPT = POSITION_REVIEW_SYSTEM_PROMPT.replace(
    "XAUUSD", SYMBOL
)


def _filtered_raw_payload(
    payload: dict,
    timeframes: tuple[str, ...],
    h1_closed_limit: int | None = None,
) -> dict:
    """Returns a raw-data copy for one stage without mutating the archive."""
    selected = copy.deepcopy(payload)
    # Deterministic facts are attached once, explicitly, at the stage root.
    # Keeping another full copy inside transported raw data wastes tokens and
    # exposes lower-timeframe FVG candidates to stages that must not use them.
    selected.pop("deterministic_market_facts", None)

    history = selected.get("cacheable_history")
    if not isinstance(history, dict):
        history = {}
        selected["cacheable_history"] = history
    history_by_tf = history.get("closed_market_history_before_day_start")
    if not isinstance(history_by_tf, dict):
        history_by_tf = {}

    live = selected.get("live_market")
    if not isinstance(live, dict):
        live = {}
        selected["live_market"] = live
    live_by_tf = live.get("raw_timeframes_since_day_start")
    if not isinstance(live_by_tf, dict):
        live_by_tf = {}

    allowed = set(timeframes)
    history_by_tf = {
        str(name): value
        for name, value in history_by_tf.items()
        if name in allowed and isinstance(value, dict)
    }
    live_by_tf = {
        str(name): value
        for name, value in live_by_tf.items()
        if name in allowed and isinstance(value, dict)
    }

    if h1_closed_limit is not None and "H1" in allowed:
        limit = max(1, int(h1_closed_limit))
        live_h1 = live_by_tf.get("H1")
        if isinstance(live_h1, dict):
            live_bars = live_h1.get("closed_bars_since_day_start")
            if not isinstance(live_bars, list):
                live_bars = []
            live_bars = live_bars[-limit:]
            live_h1["closed_bars_since_day_start"] = live_bars
            live_h1["closed_bars_since_day_start_count"] = len(live_bars)
        else:
            live_bars = []

        remaining = max(0, limit - len(live_bars))
        history_h1 = history_by_tf.get("H1")
        if isinstance(history_h1, dict):
            history_bars = history_h1.get("closed_bars")
            if not isinstance(history_bars, list):
                history_bars = []
            history_bars = history_bars[-remaining:] if remaining else []
            history_h1["closed_bars"] = history_bars
            history_h1["closed_bars_count"] = len(history_bars)

    history["closed_market_history_before_day_start"] = history_by_tf
    live["raw_timeframes_since_day_start"] = live_by_tf
    return selected


def _filtered_deterministic_market_facts(
    payload: dict,
    fvg_timeframes: tuple[str, ...],
) -> dict:
    facts = copy.deepcopy(payload.get("deterministic_market_facts") or {})
    if not isinstance(facts, dict):
        return {}
    imbalances = facts.get("imbalances")
    if isinstance(imbalances, dict):
        allowed = set(fvg_timeframes)
        facts["imbalances"] = {
            str(timeframe): value
            for timeframe, value in imbalances.items()
            if str(timeframe) in allowed and isinstance(value, list)
        }
    return facts


def _anchor_id(point: dict) -> str:
    identity = [
        str(point.get("scenario", "")),
        str(point.get("degree", "")),
        str(point.get("timeframe", "")),
        int(point.get("sequence", 0) or 0),
        str(point.get("label", "")),
        str(point.get("time", "")),
        point.get("price"),
    ]
    digest = hashlib.sha256(_compact_json(identity).encode("utf-8")).hexdigest()
    return f"wave_{digest[:20]}"


def build_previous_confirmed_anchor_reference(
    previous_reference: dict | None,
) -> dict:
    """Builds compact stable IDs from the last successful FULL chart."""
    result = {
        "reference_available": False,
        "saved_at_fp": None,
        "market_snapshot_time_fp": None,
        "h1_closed_bar_time_fp": None,
        "previous_map_summary": None,
        "anchors": [],
    }
    if not isinstance(previous_reference, dict):
        return result

    analysis = previous_reference.get("analysis")
    if not isinstance(analysis, dict):
        return result

    visualization = analysis.get("visualization")
    if not isinstance(visualization, dict):
        visualization = {}

    anchors = []
    seen = set()
    for point in visualization.get("wave_points", []):
        if not isinstance(point, dict):
            continue
        if str(point.get("status", "")).strip().lower() != "confirmed":
            continue
        identifier = _anchor_id(point)
        if identifier in seen:
            continue
        seen.add(identifier)
        anchors.append({"anchor_id": identifier, "point": copy.deepcopy(point)})

    result.update(
        {
            "reference_available": True,
            "saved_at_fp": previous_reference.get("saved_at_fp"),
            "market_snapshot_time_fp": previous_reference.get(
                "market_snapshot_time_fp"
            ),
            "h1_closed_bar_time_fp": previous_reference.get(
                "h1_closed_bar_time_fp"
            ),
            "previous_map_summary": {
                name: copy.deepcopy(analysis.get(name))
                for name in (
                    "market_regime",
                    "timeframe_analysis",
                    "price_structure",
                    "patterns",
                    "wave_count",
                    "higher_timeframe_context",
                    "scenario_map",
                )
            },
            "anchors": anchors,
        }
    )
    return result


def build_market_map_stage_payload(
    payload: dict,
    previous_reference: dict | None,
) -> dict:
    raw = _filtered_raw_payload(payload, MAP_TIMEFRAMES)
    return {
        "stage": "FULL_MAP",
        "staged_analysis_version": STAGED_ANALYSIS_VERSION,
        "frozen_snapshot_timestamp": payload.get("timestamp"),
        "raw_market_d1_h4_h1": build_transport_payload(raw),
        "deterministic_market_facts": _filtered_deterministic_market_facts(
            payload,
            ("D1", "H4", "H1"),
        ),
        "previous_confirmed_wave_anchors": (
            build_previous_confirmed_anchor_reference(previous_reference)
        ),
    }


def build_trade_decision_stage_payload(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
) -> dict:
    raw = _filtered_raw_payload(
        payload,
        DECISION_TIMEFRAMES,
        h1_closed_limit=DECISION_H1_CLOSED_BARS,
    )
    complete_map = copy.deepcopy(market_map)
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    previous_by_id = {
        str(item["anchor_id"]): copy.deepcopy(item["point"])
        for item in previous_anchors.get("anchors", [])
        if isinstance(item, dict) and item.get("anchor_id")
    }
    revision = complete_map.get("wave_revision") or {}
    preserved = set(revision.get("preserved_anchor_ids") or [])
    invalidated = set(revision.get("invalidated_anchor_ids") or [])
    invalidated_keys = {
        _wave_key(previous_by_id[identifier])
        for identifier in invalidated
        if identifier in previous_by_id
    }
    visualization = complete_map.get("visualization")
    if not isinstance(visualization, dict):
        visualization = {
            "wave_points": [],
            "levels": [],
            "zones": [],
            "scenario_paths": [],
            "trendlines": [],
            "channels": [],
            "pattern_shapes": [],
            "market_events": [],
            "projected_waves": [],
            "wave_structures": [],
            "chart_comment": "",
        }
        complete_map["visualization"] = visualization
    wave_candidates = [
        previous_by_id[identifier]
        for identifier in previous_by_id
        if identifier in preserved
    ]
    wave_candidates.extend(visualization.get("wave_points") or [])
    wave_candidates = [
        point
        for point in wave_candidates
        if isinstance(point, dict) and _wave_key(point) not in invalidated_keys
    ]
    visualization["wave_points"] = _merge_unique(wave_candidates, _wave_key)

    return {
        "stage": "FULL_DECISION",
        "staged_analysis_version": STAGED_ANALYSIS_VERSION,
        "frozen_snapshot_timestamp": payload.get("timestamp"),
        "validated_market_map": complete_map,
        "raw_execution_market_h1_m30_m15_m5": build_transport_payload(raw),
        "deterministic_market_facts": _filtered_deterministic_market_facts(
            payload,
            ("H1",),
        ),
    }


def _position_reference_summary(previous_reference: dict | None) -> dict:
    if not isinstance(previous_reference, dict):
        return {}
    analysis = previous_reference.get("analysis")
    if not isinstance(analysis, dict):
        return {}
    result = {
        "saved_at_fp": previous_reference.get("saved_at_fp"),
        "market_snapshot_time_fp": previous_reference.get(
            "market_snapshot_time_fp"
        ),
        "h1_closed_bar_time_fp": previous_reference.get(
            "h1_closed_bar_time_fp"
        ),
        "analysis": {
            key: copy.deepcopy(analysis.get(key))
            for key in (
                "timestamp",
                "instrument",
                "market_regime",
                "timeframe_analysis",
                "price_structure",
                "patterns",
                "wave_count",
                "higher_timeframe_context",
                "scenario_map",
                "recommendation",
            )
        },
    }
    visualization = analysis.get("visualization")
    if isinstance(visualization, dict):
        result["analysis"]["visualization"] = {
            key: copy.deepcopy(visualization.get(key, []))
            for key in (
                "wave_points",
                "levels",
                "zones",
                "trendlines",
                "channels",
                "pattern_shapes",
                "market_events",
                "projected_waves",
                "wave_structures",
            )
        }
    return result


def build_position_review_stage_payload(
    payload: dict,
    *,
    review_trigger: str,
    position_context: dict,
    previous_reference: dict | None,
    previous_monitor_result: dict | None = None,
) -> dict:
    """Build a frozen multi-timeframe position review input."""
    raw = _filtered_raw_payload(payload, POSITION_REVIEW_TIMEFRAMES)
    return {
        "stage": "POSITION_REVIEW",
        "staged_analysis_version": STAGED_ANALYSIS_VERSION,
        "frozen_snapshot_timestamp": payload.get("timestamp"),
        "review_trigger": str(review_trigger),
        "safety_policy": {
            "may_open_or_add_position": False,
            "may_close_position": False,
            "may_propose_stop_loss_tightening": True,
            "may_propose_take_profit_recalculation": True,
            "may_call_executor": False,
            "python_validates_raw_anchors": True,
            "never_widen_existing_stop": True,
            "stop_rule": (
                "M15 wave 3 -> beyond wave 1 extreme; wave 5 -> beyond "
                "wave 3 extreme; correction C -> beyond A; Y -> beyond W."
            ),
            "meaning_of_manual_review": (
                "Notification to the human only; never an execution command."
            ),
        },
        "open_position_context": copy.deepcopy(position_context),
        "original_trade_thesis": _position_reference_summary(
            previous_reference
        ),
        "previous_position_review": copy.deepcopy(
            previous_monitor_result
            if isinstance(previous_monitor_result, dict)
            else {}
        ),
        "raw_market_d1_h4_h1_m30_m15_m5": build_transport_payload(raw),
        "deterministic_market_facts": _filtered_deterministic_market_facts(
            payload,
            ("D1", "H4", "H1"),
        ),
    }


def _stage_raw_response(response, stage: str, request_id=None) -> Path:
    DEBUG_STAGE_ATTEMPTS_DIR.mkdir(parents=True, exist_ok=True)
    content = []
    for block in getattr(response, "content", []) or []:
        block_type = getattr(block, "type", None)
        item = {"type": block_type}
        if block_type == "text":
            item["text"] = getattr(block, "text", "")
        elif block_type in {"thinking", "redacted_thinking"}:
            item["thinking_present"] = True
        content.append(item)

    usage = get_usage_stats(response)
    raw = {
        "stage": stage,
        "request_id": str(request_id) if request_id not in (None, "") else None,
        "id": getattr(response, "id", None),
        "model": getattr(response, "model", None),
        "stop_reason": getattr(response, "stop_reason", None),
        "content": content,
        "usage": usage,
    }
    identifier = raw.get("id") or raw.get("request_id") or "unknown_response"
    safe_identifier = "".join(
        character
        for character in str(identifier)
        if character.isalnum() or character in {"-", "_"}
    )
    path = DEBUG_STAGE_ATTEMPTS_DIR / f"{stage.lower()}_{safe_identifier}.json"
    path.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    latest = DEBUG_DIR / f"claude_{stage.lower()}_raw_response.json"
    latest.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def _count_stage_tokens(client, model, system_prompt, content, effort, schema):
    counted = client.messages.count_tokens(
        model=model,
        system=system_prompt,
        messages=[{"role": "user", "content": content}],
        output_config={
            "effort": effort,
            "format": {"type": "json_schema", "schema": schema},
        },
    )
    return int(counted.input_tokens)


def _completion_reserve_instruction(max_tokens: int) -> str:
    response_reserve_tokens = min(
        32_000,
        max(8_000, int(max_tokens) // 3),
    )
    return (
        "КРИТИЧЕСКОЕ ПРАВИЛО ЗАВЕРШЕНИЯ: max_tokens включает одновременно "
        "внутреннее thinking и финальный Structured JSON. Заверши thinking "
        f"заранее и сохрани не менее {response_reserve_tokens:,} токенов "
        "доступного бюджета для полного JSON. Полный schema-valid ответ "
        "важнее дополнительного рассуждения у границы бюджета. Никогда не "
        "расходуй весь лимит, не завершив финальный JSON."
    )


@observe("api", artifacts=True)
def _request_structured_stage_unlocked(
    *,
    stage: str,
    stage_payload: dict,
    system_prompt: str,
    schema: dict,
    max_tokens: int,
    effort_override: str | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    normalized_stage = str(stage).upper()
    _LAST_STAGE_USAGE.pop(normalized_stage, None)
    _LAST_STAGE_DIAGNOSTICS.pop(normalized_stage, None)

    config = load_anthropic_config()
    model = get_model(config)
    retry_context = get_claude_api_retry_context()
    previous_failure_class = str(
        retry_context.get("previous_failure_class") or ""
    ).upper()
    effort = (
        str(effort_override).strip().lower()
        if effort_override not in (None, "")
        else get_effort(config)
    )
    client = create_anthropic_client(config)

    # Sonnet 5 has no strict adaptive-thinking token budget.  Effort is the
    # provider-supported control; this instruction is an additional soft
    # completion guard, not a replacement for the bounded effort above.
    reliability_instruction = (
        "КОНТЕКСТ НАДЁЖНОСТИ: это исследовательский анализ рыночных данных "
        "на DEMO-счёте. Ты не имеешь доступа к брокеру и не исполняешь "
        "транзакции. Python отдельно применяет риск-правила и решает, можно "
        "ли передавать какой-либо ордер. Если торгового основания нет, "
        "верни schema-valid stay_out; отказ или текст вместо JSON не является "
        "допустимым результатом этапа."
    )
    recovery_instruction = ""
    if previous_failure_class == "REFUSAL_RESPONSE":
        recovery_instruction = (
            "\n\nRECOVERY ПОСЛЕ REFUSAL: предыдущий ответ этого же frozen "
            "этапа был отказом. Выполни только объективную классификацию "
            "предоставленных OHLC и верни заданный Structured JSON. При любом "
            "сомнении используй stay_out внутри schema, а не отказ."
        )
    elif previous_failure_class == "INCOMPLETE_RESPONSE":
        recovery_instruction = (
            "\n\nRECOVERY ПОСЛЕ НЕПОЛНОГО ОТВЕТА: сократи пояснения, не "
            "повторяй выводы и зарезервируй приоритет для полного JSON со "
            "всеми обязательными полями."
        )
        # A prior max-token/parse failure needs completion, not another long
        # hidden reasoning pass over the unchanged frozen snapshot.
        effort = "low"
    system_prompt = (
        f"{system_prompt}\n\n{reliability_instruction}"
        f"{recovery_instruction}\n\n"
        f"{_completion_reserve_instruction(max_tokens)}"
    )

    content = [
        {
            "type": "text",
            "text": (
                f"{normalized_stage} FROZEN INPUT.\n"
                "Use every supplied raw candle according to the stage "
                "instructions.\n\n"
                f"<{normalized_stage.lower()}_input>\n"
                f"{_compact_json(stage_payload)}\n"
                f"</{normalized_stage.lower()}_input>"
            ),
        }
    ]

    request_fingerprint = {
        "stage": normalized_stage,
        "model": model,
        "effort": effort,
        "max_tokens": int(max_tokens),
        "system": system_prompt,
        "content": content,
        "schema": schema,
    }
    transport_path = artifact(normalized_stage + "_transport", request_fingerprint)
    emit("api", "transport_prepared", data={"stage": normalized_stage, "model": model,
         "effort": effort, "max_tokens": max_tokens, "transport_artifact": transport_path})
    encoded_payload = _compact_json(stage_payload).encode("utf-8")
    # The durable guard compares frozen market input, not wording.  A retry
    # may intentionally add the recovery instruction above while the exact
    # market payload must remain byte-for-byte identical.
    payload_sha256 = hashlib.sha256(encoded_payload).hexdigest()
    request_sha256 = hashlib.sha256(
        _compact_json(request_fingerprint).encode("utf-8")
    ).hexdigest()

    print()
    print("=" * 80)
    print(f"ANTHROPIC API — {normalized_stage}")
    print("=" * 80)
    print(f"Модель:       {model}")
    print(f"Max tokens:   {max_tokens}")
    print(f"Effort:       {effort}")
    print("Transport:    SSE streaming")
    print(f"Timeout:      {get_effective_timeout_seconds(config):.0f} sec")
    print("SDK retries:  OFF; retries journaled per stage")
    print(f"Stage input:  {len(encoded_payload) / 1024:.1f} KB")
    print("[INFO] Считаем входные токены этапа...")

    try:
        token_count = _count_stage_tokens(
            client, model, system_prompt, content, effort, schema
        )
    except anthropic.AuthenticationError as error:
        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API при подсчёте токенов.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error
    except anthropic.PermissionDeniedError as error:
        raise ClaudePermanentRequestError(
            "API key не имеет доступа к модели при подсчёте токенов.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error
    except (anthropic.APITimeoutError, anthropic.APIConnectionError) as error:
        raise ClaudeTransientRequestError(
            "Не удалось выполнить подсчёт входных токенов; платный "
            "Messages-запрос ещё не отправлялся.",
            request_id=_anthropic_error_request_id(error),
        ) from error
    except anthropic.APIStatusError as error:
        raise _translate_api_status_error(error, f"{normalized_stage} counting")

    diagnostics = {
        "model": model,
        "input_tokens": token_count,
        "transport_payload_bytes": len(encoded_payload),
        "payload_sha256": payload_sha256,
        "request_sha256": request_sha256,
        "retry_recovery_from": previous_failure_class or None,
        "request_id": None,
        "response_id": None,
        "stop_reason": None,
        "response_received": False,
        "usage": None,
    }
    _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
    if callable(on_preflight):
        on_preflight(dict(diagnostics))

    print(f"[INFO] Входных токенов {normalized_stage}: {token_count:,}")
    print(f"[INFO] Отправляем {normalized_stage} Claude...")

    request_id = None
    recovered_result = None

    def record_stream_progress(values: dict):
        diagnostics.update(values)
        _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
        if callable(on_response):
            try:
                on_response(dict(diagnostics))
            except Exception as callback_error:
                # Telemetry must never break an already running paid stream.
                print(
                    "[API JOURNAL WARNING] Не удалось записать stream "
                    f"progress: {type(callback_error).__name__}: "
                    f"{callback_error}"
                )

    try:
        with client.messages.stream(
            model=model,
            max_tokens=int(max_tokens),
            system=system_prompt,
            messages=[{"role": "user", "content": content}],
            output_config={
                "effort": effort,
                "format": {"type": "json_schema", "schema": schema},
            },
        ) as stream:
            stream_result = consume_structured_stream(
                stream,
                stage=normalized_stage,
                schema=schema,
                payload_sha256=payload_sha256,
                on_progress=record_stream_progress,
            )

            diagnostics.update(stream_result.get("diagnostics") or {})
            request_id = diagnostics.get("request_id")
            recovered_result = stream_result.get("recovered_result")
            stream_error = stream_result.get("error")
            response = stream_result.get("response")

            if recovered_result is None and stream_error is not None:
                raise ClaudeRequestOutcomeUnknownError(
                    f"{normalized_stage}: SSE оборвался после открытия "
                    "потока; полный schema-valid JSON не восстановлен. "
                    "Исход генерации неизвестен и возможна тарификация.",
                    request_id=request_id,
                    diagnostics=diagnostics,
                ) from stream_error
    except anthropic.AuthenticationError as error:
        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API. Точный повтор не поможет.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error
    except anthropic.PermissionDeniedError as error:
        raise ClaudePermanentRequestError(
            "API key не имеет доступа к модели. Точный повтор не поможет.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error
    except anthropic.RateLimitError as error:
        raise ClaudeTransientRequestError(
            "Превышен rate limit Anthropic API; controlled retry разрешён.",
            request_id=_anthropic_error_request_id(error),
            status_code=429,
            retry_after_seconds=_anthropic_retry_after_seconds(error),
        ) from error
    except anthropic.APITimeoutError as error:
        raise ClaudeRequestOutcomeUnknownError(
            f"{normalized_stage}: timeout после отправки; исход генерации "
            "неизвестен и возможна тарификация.",
            request_id=_anthropic_error_request_id(error),
            diagnostics=diagnostics,
        ) from error
    except anthropic.APIConnectionError as error:
        raise ClaudeRequestOutcomeUnknownError(
            f"{normalized_stage}: SSE соединение оборвалось; исход генерации "
            "неизвестен и возможна тарификация.",
            request_id=_anthropic_error_request_id(error),
            diagnostics=diagnostics,
        ) from error
    except anthropic.APIStatusError as error:
        raise _translate_api_status_error(
            error, f"{normalized_stage} Messages streaming"
        ) from error
    except ClaudeRequestError:
        raise
    except Exception as error:
        raise ClaudeRequestOutcomeUnknownError(
            f"{normalized_stage}: непредвиденная ошибка SSE; исход генерации "
            "неизвестен.",
            request_id=_anthropic_error_request_id(error),
            diagnostics=diagnostics,
        ) from error

    if recovered_result is not None:
        diagnostics.update(
            {
                "request_id": request_id,
                "response_id": None,
                "stop_reason": "recovered_complete_json_without_message_stop",
                "response_received": True,
                "usage": None,
                "delivery_recovered": True,
                "billing_status": "UNKNOWN_MAY_BE_BILLED",
            }
        )
        _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
        if callable(on_response):
            try:
                on_response(dict(diagnostics))
            except Exception as callback_error:
                print(
                    "[API JOURNAL WARNING] Восстановленный ответ уже "
                    "получен, но journal не обновлён: "
                    f"{type(callback_error).__name__}: {callback_error}"
                )
        print(
            f"[DELIVERY RECOVERED] {normalized_stage}: полный JSON "
            "восстановлен из локального SSE journal; новый запрос не нужен."
        )
        return recovered_result

    usage = get_usage_stats(response)
    _LAST_STAGE_USAGE[normalized_stage] = dict(usage)
    diagnostics.update(
        {
            "request_id": (
                str(request_id) if request_id not in (None, "") else None
            ),
            "response_id": getattr(response, "id", None),
            "stop_reason": getattr(response, "stop_reason", None),
            "response_received": True,
            "usage": dict(usage),
            "delivery_recovered": False,
            "billing_status": "USAGE_AVAILABLE",
        }
    )
    _LAST_STAGE_DIAGNOSTICS[normalized_stage] = dict(diagnostics)
    try:
        raw_path = _stage_raw_response(response, normalized_stage, request_id)
    except Exception as raw_error:
        # The paid response is already in memory.  A debug-file failure must
        # never throw it away and trigger another paid generation.
        raw_path = DEBUG_DIR / f"claude_{normalized_stage.lower()}_raw_response.json"
        print(
            "[RAW RESPONSE WARNING] Не удалось сохранить debug response, "
            "но полученный ответ продолжает обрабатываться: "
            f"{type(raw_error).__name__}: {raw_error}"
        )

    if callable(on_response):
        try:
            on_response(dict(diagnostics))
        except Exception as callback_error:
            print(
                "[API JOURNAL WARNING] Ответ этапа уже получен, но journal "
                f"не обновлён: {type(callback_error).__name__}: {callback_error}"
            )

    print(f"[INFO] {normalized_stage} response получен; request_id={request_id}")
    print(
        f"[TOKENS] input={usage['input_tokens']:,}; "
        f"output={usage['output_tokens']:,}; "
        f"thinking={usage.get('thinking_tokens', 0):,}"
    )

    try:
        if getattr(response, "stop_reason", None) == "model_context_window_exceeded":
            raise ClaudePermanentRequestError(
                f"{normalized_stage}: context window exceeded; точный повтор "
                "не исправит вход.",
                request_id=request_id,
            )
        validate_stop_reason(response)
        response_text = extract_text_response(response)
        if not response_text:
            raise ClaudeIncompleteResponseError(
                f"{normalized_stage}: Structured Output отсутствует.",
                request_id=request_id,
                diagnostics=diagnostics,
            )
        try:
            result = json.loads(response_text)
        except json.JSONDecodeError as parse_error:
            raise ClaudeIncompleteResponseError(
                f"{normalized_stage}: Structured JSON не завершён: "
                f"{parse_error}.",
                request_id=request_id,
                diagnostics=diagnostics,
            ) from parse_error
        if not isinstance(result, dict) or not result:
            raise ClaudeIncompleteResponseError(
                f"{normalized_stage}: Structured Output должен быть "
                "непустым object.",
                request_id=request_id,
                diagnostics=diagnostics,
            )
    except ClaudeRequestError:
        raise
    except Exception as error:
        raise ClaudeInvalidResponseError(
            f"{normalized_stage} тарифицирован, но ответ не прошёл разбор: "
            f"{type(error).__name__}: {error}. Raw: {raw_path}",
            request_id=request_id,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error

    return result


def _request_structured_stage(**kwargs) -> dict:
    """Serialize all Anthropic calls and obey V8.5.3 global AI breaker."""

    from claude_resilient_pipeline import (
        ensure_ai_request_allowed,
        register_permanent_ai_failure,
    )

    ensure_ai_request_allowed()
    lock = SingleInstanceLock(CLAUDE_API_LOCK_PATH)
    try:
        lock.acquire()
    except SingleInstanceError as error:
        raise ClaudeTransientRequestError(
            "Другой процесс уже выполняет Claude API stage; новый платный "
            "запрос не отправлен."
        ) from error
    try:
        try:
            return _request_structured_stage_unlocked(**kwargs)
        except ClaudePermanentRequestError as error:
            register_permanent_ai_failure(
                str(kwargs.get("stage") or "STRUCTURED_STAGE"), error
            )
            raise
    finally:
        lock.release()


def get_last_stage_usage(stage: str) -> dict | None:
    value = _LAST_STAGE_USAGE.get(str(stage).upper())
    return dict(value) if isinstance(value, dict) else None


def get_last_stage_diagnostics(stage: str) -> dict | None:
    value = _LAST_STAGE_DIAGNOSTICS.get(str(stage).upper())
    return dict(value) if isinstance(value, dict) else None


def _record_wire_warning(message: str) -> None:
    _LAST_WIRE_NORMALIZATION_WARNINGS.append(str(message))


def get_last_wire_normalization_warnings() -> list[str]:
    return list(_LAST_WIRE_NORMALIZATION_WARNINGS)


def _wire_row_to_object(
    value,
    columns: tuple[str, ...],
    path: str,
    *,
    allow_extra: bool = False,
    optional_trailing: int = 0,
) -> dict:
    if isinstance(value, dict):
        if set(value) != set(columns):
            missing = sorted(set(columns) - set(value))
            extra = sorted(set(value) - set(columns))
            raise ValueError(f"{path}: missing={missing}; extra={extra}.")
        if any(not isinstance(item, str) for item in value.values()):
            raise ValueError(f"{path}: каждое именованное wire-поле должно быть string.")
        return dict(value)
    if not isinstance(value, list):
        raise ValueError(f"{path} должен быть named object (legacy: complete array).")
    if len(value) > len(columns) and allow_extra:
        _record_wire_warning(
            f"{path}: удалено лишних trailing-ячеек: {len(value) - len(columns)}."
        )
        value = value[: len(columns)]
    minimum = len(columns) - max(0, int(optional_trailing))
    if minimum <= len(value) < len(columns):
        _record_wire_warning(
            f"{path}: восстановлено пустых trailing-ячеек: {len(columns) - len(value)}."
        )
        value = list(value) + [""] * (len(columns) - len(value))
    if len(value) != len(columns):
        raise ValueError(
            f"{path}: ожидалось {len(columns)} ячеек, получено {len(value)}."
        )
    if any(not isinstance(item, str) for item in value):
        raise ValueError(f"{path}: каждая wire-ячейка должна быть string.")
    return dict(zip(columns, value))


def _expand_wire_data_quality(value, path: str = "data_quality") -> dict:
    """Expands current strict quality object and legacy paid array responses.

    Legacy compatibility is deliberately limited to this one field.  The
    first cell remains the boolean flag and every later string is an issue;
    joining those issue strings is lossless and does not alter market logic.
    Other positional rows remain exact-length and fail closed.
    """
    if isinstance(value, dict):
        if set(value) != {"sufficient", "issues"}:
            raise ValueError(f"{path}: wire object contract неверен.")
        if not isinstance(value["sufficient"], str):
            raise ValueError(f"{path}.sufficient должен быть string.")
        if not isinstance(value["issues"], str):
            raise ValueError(f"{path}.issues должен быть string.")
        result = dict(value)
    elif isinstance(value, list):
        if len(value) < 2:
            raise ValueError(
                f"{path}: ожидалось минимум 2 legacy-ячейки, "
                f"получено {len(value)}."
            )
        if any(not isinstance(item, str) for item in value):
            raise ValueError(f"{path}: каждая legacy wire-ячейка должна быть string.")
        issues = [item.strip() for item in value[1:] if item.strip()]
        result = {
            "sufficient": value[0],
            "issues": "; ".join(issues) if issues else "none",
        }
    else:
        raise ValueError(f"{path} должен быть strict object или legacy array.")

    result["sufficient"] = _wire_boolean(
        result["sufficient"], f"{path}.sufficient"
    )
    return result


def _wire_required_float(value: str, path: str) -> float:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{path}: обязательное число пусто.")
    try:
        number = float(text)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{path}: неверная decimal string {value!r}.") from error
    if not math.isfinite(number):
        raise ValueError(f"{path}: число должно быть finite.")
    return number


def _wire_optional_float(value: str, path: str) -> float | None:
    if not str(value).strip():
        return None
    return _wire_required_float(value, path)


def _validate_fvg_selection(analysis: dict, payload: dict) -> None:
    recommendation = analysis.get("recommendation") or {}
    role = str(recommendation.get("fvg_role") or "")
    selected_ids = {
        item.strip()
        for item in str(recommendation.get("fvg_ids") or "").split(",")
        if item.strip()
    }
    facts = payload.get("deterministic_market_facts") or {}
    imbalances = facts.get("imbalances") or {}
    available_ids = {
        str(item.get("id"))
        for items in imbalances.values()
        if isinstance(items, list)
        for item in items
        if isinstance(item, dict) and item.get("id") and item.get("active") is True
    } if isinstance(imbalances, dict) else set()
    unknown = selected_ids - available_ids
    if unknown:
        raise ValueError(
            "recommendation.fvg_ids содержит несуществующие Python IDs: "
            + ", ".join(sorted(unknown))
        )
    if role == "no_relevant_fvg" and selected_ids:
        raise ValueError("no_relevant_fvg несовместим с непустым fvg_ids.")
    if role not in {"", "no_relevant_fvg", "neutral"} and not selected_ids:
        raise ValueError(f"FVG role {role} требует хотя бы один точный fvg_id.")

    drawn_ids = {
        str(item.get("fvg_id"))
        for item in (analysis.get("visualization") or {}).get("zones", [])
        if isinstance(item, dict)
        and str(item.get("kind", "")).lower() == "fvg"
        and item.get("fvg_id")
    }
    missing_on_chart = selected_ids - drawn_ids
    if missing_on_chart:
        raise ValueError(
            "Подтверждённые recommendation.fvg_ids не имеют точной "
            "валидированной zone на графике: "
            + ", ".join(sorted(missing_on_chart))
        )


def _wire_integer(value: str, path: str) -> int:
    text = str(value).strip()
    try:
        number = int(text)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{path}: неверная integer string {value!r}.") from error
    if str(number) != text and f"+{number}" != text:
        raise ValueError(f"{path}: integer должен быть записан без дробной части.")
    return number


def _wire_boolean(value: str, path: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f'{path}: ожидалась строка "true" или "false".')


def _expand_wire_visualization(value) -> dict:
    if not isinstance(value, dict):
        raise ValueError("visualization должен быть compact wire object.")
    missing = set(COMPACT_VISUALIZATION_WIRE_SCHEMA["required"]) - set(value)
    extra = set(value) - set(COMPACT_VISUALIZATION_WIRE_SCHEMA["required"])
    if missing:
        _record_wire_warning(
            "visualization: восстановлены отсутствующие поля: "
            + ", ".join(sorted(missing))
            + "."
        )
    if extra:
        _record_wire_warning(
            "visualization: проигнорированы лишние поля: "
            + ", ".join(sorted(extra))
            + "."
        )

    def safe_rows(name, columns, required_prices=(), optional_prices=(), optional_trailing=0):
        result = []
        rows = value.get(name, [])
        if not isinstance(rows, list):
            _record_wire_warning(f"visualization.{name}: не-array отброшен.")
            return result
        for index, row in enumerate(rows):
            path = f"visualization.{name}[{index}]"
            try:
                item = _wire_row_to_object(
                    row,
                    columns,
                    path,
                    allow_extra=True,
                    optional_trailing=optional_trailing,
                )
                for field in required_prices:
                    item[field] = _wire_required_float(item[field], f"{path}.{field}")
                for field in optional_prices:
                    item[field] = _wire_optional_float(item[field], f"{path}.{field}")
                result.append(item)
            except (TypeError, ValueError) as error:
                _record_wire_warning(f"{path}: объект отброшен ({error}).")
        return result

    wave_points = safe_rows("wave_points", WAVE_POINT_COLUMNS, ("price",))
    valid_wave_points = []
    for index, item in enumerate(wave_points):
        try:
            item["sequence"] = _wire_integer(
                item["sequence"], f"visualization.wave_points[{index}].sequence"
            )
            valid_wave_points.append(item)
        except ValueError as error:
            _record_wire_warning(
                f"visualization.wave_points[{index}]: объект отброшен ({error})."
            )
    wave_points = valid_wave_points

    levels = safe_rows("levels", LEVEL_COLUMNS, ("price",))
    zones = safe_rows("zones", ZONE_COLUMNS, ("price_low", "price_high"))
    scenario_paths = safe_rows(
        "scenario_paths", SCENARIO_PATH_COLUMNS,
        ("anchor_price", "target_price_low", "target_price_high"),
    )

    trendlines = safe_rows(
        "trendlines", TRENDLINE_COLUMNS, ("start_price", "end_price")
    )
    channels = safe_rows(
        "channels",
        CHANNEL_COLUMNS,
        (
            "upper_start_price", "upper_end_price",
            "lower_start_price", "lower_end_price",
        ),
        ("breakout_price", "reentry_price"),
        optional_trailing=4,
    )
    pattern_shapes = safe_rows(
        "pattern_shapes",
        PATTERN_SHAPE_COLUMNS,
        ("price_low", "price_high"),
        ("confirmation_level", "invalidation_level", "target_price"),
    )
    market_events = safe_rows(
        "market_events", MARKET_EVENT_COLUMNS, ("price",)
    )
    projected_waves = safe_rows(
        "projected_waves",
        PROJECTED_WAVE_COLUMNS,
        ("anchor_price", "target_price_low", "target_price_high"),
        ("confirmation_level", "invalidation_level"),
    )
    wave_structures = safe_rows(
        "wave_structures",
        WAVE_STRUCTURE_COLUMNS,
        (),
        ("confirmation_level", "invalidation_level"),
    )

    chart_comment = value.get("chart_comment")
    if not isinstance(chart_comment, str):
        _record_wire_warning("visualization.chart_comment восстановлен пустой строкой.")
        chart_comment = ""
    return {
        "wave_points": wave_points,
        "levels": levels,
        "zones": zones,
        "scenario_paths": scenario_paths,
        "trendlines": trendlines,
        "channels": channels,
        "pattern_shapes": pattern_shapes,
        "market_events": market_events,
        "projected_waves": projected_waves,
        "wave_structures": wave_structures,
        "chart_comment": chart_comment,
    }


def _expand_market_map_wire_result(wire_result: dict) -> dict:
    _LAST_WIRE_NORMALIZATION_WARNINGS.clear()
    if not isinstance(wire_result, dict):
        raise ValueError("FULL_MAP wire response должен быть object.")
    if set(wire_result) != set(MARKET_MAP_WIRE_SCHEMA["required"]):
        raise ValueError("FULL_MAP wire top-level contract неверен.")

    result = copy.deepcopy(wire_result)
    result["market_regime"] = _wire_row_to_object(
        result["market_regime"], MARKET_REGIME_COLUMNS, "market_regime"
    )
    result["timeframe_analysis"] = _wire_row_to_object(
        result["timeframe_analysis"],
        TIMEFRAME_ANALYSIS_COLUMNS,
        "timeframe_analysis",
    )
    result["price_structure"] = _wire_row_to_object(
        result["price_structure"], PRICE_STRUCTURE_COLUMNS, "price_structure"
    )
    result["wave_count"] = _wire_row_to_object(
        result["wave_count"], WAVE_COUNT_COLUMNS, "wave_count"
    )
    result["wave_count"]["invalidation_level"] = _wire_optional_float(
        result["wave_count"]["invalidation_level"],
        "wave_count.invalidation_level",
    )
    result["higher_timeframe_context"] = _wire_row_to_object(
        result["higher_timeframe_context"],
        HIGHER_TIMEFRAME_CONTEXT_COLUMNS,
        "higher_timeframe_context",
    )
    result["scenario_map"] = _wire_row_to_object(
        result["scenario_map"], SCENARIO_MAP_COLUMNS, "scenario_map",
        allow_extra=True,
    )
    result["visualization"] = _expand_wire_visualization(
        result["visualization"]
    )
    result["data_quality"] = _expand_wire_data_quality(
        result["data_quality"]
    )
    return result


def _expand_trade_decision_wire_result(wire_result: dict) -> dict:
    _LAST_WIRE_NORMALIZATION_WARNINGS.clear()
    if not isinstance(wire_result, dict):
        raise ValueError("FULL_DECISION wire response должен быть object.")
    if set(wire_result) != set(TRADE_DECISION_WIRE_SCHEMA["required"]):
        raise ValueError("FULL_DECISION wire top-level contract неверен.")

    result = copy.deepcopy(wire_result)
    result["visualization"] = _expand_wire_visualization(
        result["visualization"]
    )
    result["recommendation"] = _wire_row_to_object(
        result["recommendation"], RECOMMENDATION_COLUMNS, "recommendation",
        allow_extra=True,
        optional_trailing=1,
    )
    for name in (
        "entry_price",
        "stop_loss",
        "take_profit",
        "invalidation_level",
    ):
        result["recommendation"][name] = _wire_optional_float(
            result["recommendation"][name], f"recommendation.{name}"
        )
    result["data_quality"] = _expand_wire_data_quality(
        result["data_quality"]
    )
    return result


def _expand_position_management_wire(value) -> dict:
    if not isinstance(value, dict):
        raise ValueError("position_management должен быть object.")
    required = set(POSITION_MANAGEMENT_WIRE_SCHEMA["required"])
    if set(value) != required:
        raise ValueError("position_management contract неверен.")

    result = copy.deepcopy(value)
    result["structure_confirmed"] = _wire_boolean(
        result["structure_confirmed"],
        "position_management.structure_confirmed",
    )
    numeric_fields = (
        "stop_anchor_price",
        "fib_ratio",
        "fib_leg_start_price",
        "fib_leg_end_price",
        "fib_projection_price",
    )
    for name in numeric_fields:
        result[name] = _wire_optional_float(
            result[name], f"position_management.{name}"
        )

    action = result["action"]
    stop_requested = action in {
        "tighten_stop",
        "tighten_stop_and_recalculate_target",
    }
    target_requested = action in {
        "recalculate_target",
        "tighten_stop_and_recalculate_target",
    }
    if stop_requested:
        if (
            not result["stop_anchor_time"]
            or result["stop_anchor_price"] is None
            or result["stop_anchor_kind"] not in {"low", "high"}
            or result["stop_reference_wave"] == "none"
        ):
            raise ValueError("Stop action не содержит полный M15 anchor.")
    if target_requested:
        required_target_values = (
            result["fib_method"] != "none",
            result["fib_timeframe"] in {"H1", "M15"},
            result["fib_ratio"] is not None,
            bool(result["fib_leg_start_time"]),
            result["fib_leg_start_price"] is not None,
            result["fib_leg_start_kind"] != "none",
            bool(result["fib_leg_end_time"]),
            result["fib_leg_end_price"] is not None,
            result["fib_leg_end_kind"] != "none",
            bool(result["fib_projection_time"]),
            result["fib_projection_price"] is not None,
            result["fib_projection_kind"] != "none",
        )
        if not all(required_target_values):
            raise ValueError("Target action не содержит полный Fibonacci plan.")
    if action in {"hold", "manual_review"} and (
        stop_requested or target_requested
    ):
        raise ValueError("Hold/manual_review не может изменять protection.")
    return result


def validate_market_map_result(
    result: dict,
    payload: dict,
    previous_anchor_reference: dict,
) -> None:
    if result.get("instrument") != SYMBOL:
        raise ValueError("FULL_MAP вернул неожиданный instrument.")
    if set(result) != set(MARKET_MAP_SCHEMA["required"]):
        raise ValueError("FULL_MAP top-level contract не совпадает со schema.")

    revision = result.get("wave_revision")
    if not isinstance(revision, dict):
        raise ValueError("FULL_MAP не содержит wave_revision.")
    mode = revision.get("mode")
    if mode not in {"initialize", "unchanged", "extend", "recount"}:
        raise ValueError(f"Неизвестный wave revision mode: {mode}.")

    available = {
        str(item.get("anchor_id"))
        for item in previous_anchor_reference.get("anchors", [])
        if isinstance(item, dict) and item.get("anchor_id")
    }
    preserved = [str(value) for value in revision.get("preserved_anchor_ids", [])]
    invalidated = [
        str(value) for value in revision.get("invalidated_anchor_ids", [])
    ]
    if len(preserved) != len(set(preserved)):
        raise ValueError("wave_revision содержит повтор preserved id.")
    if len(invalidated) != len(set(invalidated)):
        raise ValueError("wave_revision содержит повтор invalidated id.")
    preserved_set = set(preserved)
    invalidated_set = set(invalidated)
    if preserved_set & invalidated_set:
        raise ValueError("Один anchor одновременно preserved и invalidated.")
    if (preserved_set | invalidated_set) != available:
        missing = sorted(available - preserved_set - invalidated_set)
        unknown = sorted((preserved_set | invalidated_set) - available)
        raise ValueError(
            "Каждый previous anchor должен быть явно классифицирован. "
            f"missing={missing}; unknown={unknown}."
        )
    if not available and mode not in {"initialize", "recount"}:
        raise ValueError("Без previous anchors FULL_MAP должен initialize карту.")
    if not str(revision.get("reason", "")).strip():
        raise ValueError("wave_revision.reason пуст.")

    quality = result.get("data_quality")
    if not isinstance(quality, dict) or not isinstance(
        quality.get("sufficient"), bool
    ):
        raise ValueError("FULL_MAP data_quality неверен.")

    # Reuse the legacy professional-trader semantic validator before the map
    # is marked as a durable winner.  A malformed regime must be retried at
    # FULL_MAP, not discovered after paying for FULL_DECISION.
    semantic_probe = copy.deepcopy(result)
    semantic_probe["recommendation"] = {
        "action": "stay_out",
        "setup_type": "no_trade",
        "trade_horizon": "unclear",
        "setup_quality": "weak",
        "entry_quality": "poor",
        "order_type": "none",
        "entry_price": None,
        "stop_loss": None,
        "take_profit": None,
        "invalidation_level": None,
        "confidence": "low",
        "why_now": "Map validation only.",
        "structural_stop_basis": "Not applicable.",
        "target_basis": "Not applicable.",
        "reasoning": "Map validation only.",
        "invalidation_reason": "No trade decision at FULL_MAP.",
        "fvg_role": "no_relevant_fvg",
        "fvg_ids": "",
        "fvg_basis": "EN: No trade decision is made at the market-map stage.\nRU: На этапе карты рынка торговое решение не принимается.",
    }
    validate_trade_levels(semantic_probe)
    validate_analysis_contract(semantic_probe)

    # The sanitizer validates only chart metadata and never changes analysis.
    sanitize_visualization(result, payload)


def analyze_market_map(
    payload: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    stage_payload = build_market_map_stage_payload(payload, previous_reference)
    wire_result = _request_structured_stage(
        stage="FULL_MAP",
        stage_payload=stage_payload,
        system_prompt=(
            f"{MARKET_MAP_SYSTEM_PROMPT}\n\n{MARKET_MAP_WIRE_INSTRUCTIONS}"
        ),
        schema=MARKET_MAP_WIRE_SCHEMA,
        max_tokens=MAP_MAX_TOKENS,
        effort_override=MAP_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    invalid_result = wire_result
    try:
        result = _expand_market_map_wire_result(wire_result)
        invalid_result = result
        validate_market_map_result(
            result,
            payload,
            stage_payload["previous_confirmed_wave_anchors"],
        )
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_MAP тарифицирован, но не прошёл локальную проверку: "
            f"{type(error).__name__}: {error}.",
            invalid_result=invalid_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    return result


def analyze_trade_decision(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    stage_payload = build_trade_decision_stage_payload(
        payload,
        market_map,
        previous_reference=previous_reference,
    )
    wire_result = _request_structured_stage(
        stage="FULL_DECISION",
        stage_payload=stage_payload,
        system_prompt=(
            f"{TRADE_DECISION_SYSTEM_PROMPT}\n\n"
            f"{TRADE_DECISION_WIRE_INSTRUCTIONS}"
        ),
        schema=TRADE_DECISION_WIRE_SCHEMA,
        max_tokens=DECISION_MAX_TOKENS,
        effort_override=DECISION_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(wire_result)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_DECISION тарифицирован, но compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != SYMBOL:
        raise ClaudeInvalidResponseError(
            "FULL_DECISION вернул неожиданный instrument.",
            invalid_result=result,
            validation_error="Unexpected instrument.",
        )
    if set(result) != set(TRADE_DECISION_SCHEMA["required"]):
        raise ClaudeInvalidResponseError(
            "FULL_DECISION top-level contract не совпадает со schema.",
            invalid_result=result,
            validation_error="Top-level contract does not match schema.",
        )
    return result


def analyze_entry_check(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Small decision-only confirmation; never rebuilds the paid market map."""
    stage_payload = build_trade_decision_stage_payload(
        payload, market_map, previous_reference=previous_reference
    )
    stage_payload["stage"] = "ENTRY_CHECK"
    wire_result = _request_structured_stage(
        stage="ENTRY_CHECK",
        stage_payload=stage_payload,
        system_prompt=(
            f"{ENTRY_CHECK_SYSTEM_PROMPT}\n\n{TRADE_DECISION_WIRE_INSTRUCTIONS}"
        ),
        schema=TRADE_DECISION_WIRE_SCHEMA,
        max_tokens=ENTRY_CHECK_MAX_TOKENS,
        effort_override=ENTRY_CHECK_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(wire_result)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "ENTRY_CHECK тарифицирован, но compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != SYMBOL or set(result) != set(
        TRADE_DECISION_SCHEMA["required"]
    ):
        raise ClaudeInvalidResponseError(
            "ENTRY_CHECK contract неверен.",
            invalid_result=result,
            validation_error="ENTRY_CHECK contract mismatch.",
        )
    return result


def analyze_h1_decision_refresh(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Fresh H1 entry decision without buying/rebuilding the D1/H4 map."""
    stage_payload = build_trade_decision_stage_payload(
        payload, market_map, previous_reference=previous_reference
    )
    stage_payload["stage"] = "H1_DECISION"
    wire_result = _request_structured_stage(
        stage="H1_DECISION",
        stage_payload=stage_payload,
        system_prompt=(
            f"{H1_DECISION_REFRESH_SYSTEM_PROMPT}\n\n"
            f"{TRADE_DECISION_WIRE_INSTRUCTIONS}"
        ),
        schema=TRADE_DECISION_WIRE_SCHEMA,
        max_tokens=H1_DECISION_REFRESH_MAX_TOKENS,
        effort_override=H1_DECISION_REFRESH_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(wire_result)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "H1_DECISION тарифицирован, но compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != SYMBOL or set(result) != set(
        TRADE_DECISION_SCHEMA["required"]
    ):
        raise ClaudeInvalidResponseError(
            "H1_DECISION contract неверен.",
            invalid_result=result,
            validation_error="H1_DECISION contract mismatch.",
        )
    return result


def analyze_m30_decision_refresh(
    payload: dict,
    market_map: dict,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """One extra decision at the intermediate closed M30 bar."""

    stage_payload = build_trade_decision_stage_payload(
        payload, market_map, previous_reference=previous_reference
    )
    stage_payload["stage"] = "M30_DECISION"
    stage_payload["decision_clock"] = {
        "timeframe": "M30",
        "role": "intermediate_entry_decision_inside_h1_structure",
        "unclosed_h1_is_confirmation": False,
    }
    wire_result = _request_structured_stage(
        stage="M30_DECISION",
        stage_payload=stage_payload,
        system_prompt=(
            f"{M30_DECISION_REFRESH_SYSTEM_PROMPT}\n\n"
            f"{TRADE_DECISION_WIRE_INSTRUCTIONS}"
        ),
        schema=TRADE_DECISION_WIRE_SCHEMA,
        max_tokens=M30_DECISION_REFRESH_MAX_TOKENS,
        effort_override=M30_DECISION_REFRESH_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(wire_result)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "M30_DECISION тарифицирован, но compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != SYMBOL or set(result) != set(
        TRADE_DECISION_SCHEMA["required"]
    ):
        raise ClaudeInvalidResponseError(
            "M30_DECISION contract неверен.",
            invalid_result=result,
            validation_error="M30_DECISION contract mismatch.",
        )
    return result


def analyze_position_review(
    payload: dict,
    *,
    review_trigger: str,
    position_context: dict,
    previous_reference: dict | None = None,
    previous_monitor_result: dict | None = None,
    api_stage: str = "POSITION_REVIEW",
    on_preflight=None,
    on_response=None,
) -> dict:
    """Deep review and evidence-only protection plan for one position."""
    stage_payload = build_position_review_stage_payload(
        payload,
        review_trigger=review_trigger,
        position_context=position_context,
        previous_reference=previous_reference,
        previous_monitor_result=previous_monitor_result,
    )
    wire_result = _request_structured_stage(
        stage=str(api_stage),
        stage_payload=stage_payload,
        system_prompt=(
            f"{POSITION_REVIEW_SYSTEM_PROMPT}\n\n"
            f"{POSITION_REVIEW_WIRE_INSTRUCTIONS}"
        ),
        schema=POSITION_REVIEW_SCHEMA,
        max_tokens=POSITION_REVIEW_MAX_TOKENS,
        effort_override=POSITION_REVIEW_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    invalid_result = wire_result
    try:
        if not isinstance(wire_result, dict):
            raise ValueError("POSITION_REVIEW wire response должен быть object.")
        if set(wire_result) != set(POSITION_REVIEW_SCHEMA["required"]):
            raise ValueError("POSITION_REVIEW top-level contract неверен.")

        result = copy.deepcopy(wire_result)
        invalid_result = result
        result["visualization"] = _expand_wire_visualization(
            result["visualization"]
        )
        result["data_quality"] = _expand_wire_data_quality(
            result["data_quality"]
        )
        result["position_management"] = _expand_position_management_wire(
            result["position_management"]
        )

        if result.get("instrument") != SYMBOL:
            raise ValueError("POSITION_REVIEW вернул неожиданный instrument.")
        if str(result.get("timestamp")) != str(payload.get("timestamp")):
            raise ValueError("POSITION_REVIEW изменил frozen timestamp.")
        if str(result.get("review_trigger")) != str(review_trigger):
            raise ValueError("POSITION_REVIEW изменил review_trigger.")

        expected_ticket = str(
            position_context.get("primary_position_ticket") or ""
        )
        if expected_ticket and str(result.get("position_ticket")) != expected_ticket:
            raise ValueError("POSITION_REVIEW изменил position_ticket.")

        narrative_fields = (
            "h4_structure_and_patterns",
            "h1_parent_wave",
            "m15_child_structure",
            "m5_microstructure",
            "support_resistance_by_timeframe",
            "patterns_by_timeframe",
            "fvg_and_liquidity",
            "thesis_health",
            "position_management.management_reason",
            "next_checkpoint",
            "summary",
        )
        missing_bilingual = []
        for name in narrative_fields:
            if name == "position_management.management_reason":
                text = str(
                    result["position_management"].get("management_reason")
                    or ""
                )
            else:
                text = str(result.get(name) or "")
            if not text.startswith("EN:") or "\nRU:" not in text:
                missing_bilingual.append(name)
        if missing_bilingual:
            raise ValueError(
                "POSITION_REVIEW bilingual contract нарушен: "
                + ", ".join(missing_bilingual)
            )

        chart_warnings = sanitize_visualization(result, payload)
        if chart_warnings:
            issues = str(result["data_quality"].get("issues") or "").strip()
            warning_text = "chart: " + "; ".join(chart_warnings)
            result["data_quality"]["issues"] = (
                f"{issues}; {warning_text}" if issues and issues != "none"
                else warning_text
            )
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "POSITION_REVIEW тарифицирован, но не прошёл локальную проверку: "
            f"{type(error).__name__}: {error}.",
            invalid_result=invalid_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    return result


def repair_market_map(
    payload: dict,
    invalid_result: dict,
    validation_error: str,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Repair one known invalid map without buying the raw MAX map again."""
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    original, repair_schema = plan_repair(invalid_result, MARKET_MAP_WIRE_SCHEMA, validation_error)
    emit("validation", "map_repair_scope", data={"replace_sections": repair_schema["required"],
         "validation_error": validation_error})
    repair_payload = {
        "replace_sections": repair_schema["required"],
        "instruction": "Return ONLY replace_sections. All other sections are frozen. Never guess missing facts.",
        "stage": "FULL_MAP_REPAIR",
        "repair_scope": "contract_and_reported_validation_error_only",
        "validation_error": str(validation_error),
        "immutable_facts": {
            "instrument": SYMBOL,
            "timestamp": str(payload.get("timestamp")),
            "timezone": payload.get("timezone"),
            "symbol_specification": (
                payload.get("cacheable_history", {}).get(
                    "symbol_specification", {}
                )
                if isinstance(payload.get("cacheable_history"), dict)
                else {}
            ),
            "previous_confirmed_wave_anchors": previous_anchors,
        },
        "supplied_invalid_result": copy.deepcopy(invalid_result),
    }
    wire_result = _request_structured_stage(
        stage="FULL_MAP_REPAIR",
        stage_payload=repair_payload,
        system_prompt=(
            f"{MARKET_MAP_REPAIR_SYSTEM_PROMPT}\n\n"
            f"{MARKET_MAP_WIRE_INSTRUCTIONS}\nReturn only the sections required by the supplied repair schema."
        ),
        schema=repair_schema,
        max_tokens=MAP_REPAIR_MAX_TOKENS,
        effort_override=REPAIR_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    invalid_repair_result = wire_result
    try:
        result = _expand_market_map_wire_result(merge_repair(original, wire_result, repair_schema, MARKET_MAP_WIRE_SCHEMA))
        invalid_repair_result = result
        validate_market_map_result(result, payload, previous_anchors)
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_MAP_REPAIR не прошёл локальную проверку: "
            f"{type(error).__name__}: {error}.",
            invalid_result=invalid_repair_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    return result


def repair_trade_decision(
    payload: dict,
    market_map: dict,
    invalid_result: dict,
    validation_error: str,
    previous_reference: dict | None = None,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Repair one decision contract using only its existing execution scope."""
    live_market = payload.get("live_market") or {}
    live_by_tf = live_market.get("raw_timeframes_since_day_start") or {}
    compact_bars = {}
    for timeframe in DECISION_TIMEFRAMES:
        source = live_by_tf.get(timeframe) or {}
        closed = source.get("closed_bars_since_day_start") or []
        compact_bars[timeframe] = {
            "latest_closed_bars": copy.deepcopy(closed[-8:]),
            "current_unclosed_bar": copy.deepcopy(
                source.get("current_unclosed_bar")
            ),
        }
    original, repair_schema = plan_repair(invalid_result, TRADE_DECISION_WIRE_SCHEMA, validation_error)
    emit("validation", "decision_repair_scope", data={"replace_sections": repair_schema["required"],
         "validation_error": validation_error})
    repair_payload = {
        "replace_sections": repair_schema["required"],
        "instruction": "Return ONLY replace_sections. All other sections are frozen. Never guess missing facts.",
        "stage": "FULL_DECISION_REPAIR",
        "repair_scope": "contract_and_reported_validation_error_only",
        "validation_error": str(validation_error),
        "supplied_invalid_result": copy.deepcopy(invalid_result),
        # REPAIR must not buy the full H1/M30/M15/M5 history again. The already
        # paid result contains the analysis; these immutable facts are enough
        # to fix a local contract/translation/level error or choose stay_out.
        "compact_immutable_context": {
            "frozen_snapshot_timestamp": payload.get("timestamp"),
            "current_price": copy.deepcopy(live_market.get("current_price")),
            "validated_market_map": copy.deepcopy(market_map),
            "latest_execution_bars": compact_bars,
        },
    }
    wire_result = _request_structured_stage(
        stage="FULL_DECISION_REPAIR",
        stage_payload=repair_payload,
        system_prompt=(
            f"{TRADE_DECISION_REPAIR_SYSTEM_PROMPT}\n\n"
            f"{TRADE_DECISION_WIRE_INSTRUCTIONS}\nReturn only the sections required by the supplied repair schema."
        ),
        schema=repair_schema,
        max_tokens=DECISION_REPAIR_MAX_TOKENS,
        effort_override=REPAIR_EFFORT,
        on_preflight=on_preflight,
        on_response=on_response,
    )
    try:
        result = _expand_trade_decision_wire_result(merge_repair(original, wire_result, repair_schema, TRADE_DECISION_WIRE_SCHEMA))
    except Exception as error:
        raise ClaudeInvalidResponseError(
            "FULL_DECISION_REPAIR compact wire не развёрнут: "
            f"{type(error).__name__}: {error}.",
            invalid_result=wire_result,
            validation_error=f"{type(error).__name__}: {error}",
        ) from error
    if result.get("instrument") != SYMBOL:
        raise ClaudeInvalidResponseError(
            "FULL_DECISION_REPAIR вернул неожиданный instrument.",
            invalid_result=result,
            validation_error="Unexpected instrument after repair.",
        )
    if set(result) != set(TRADE_DECISION_SCHEMA["required"]):
        raise ClaudeInvalidResponseError(
            "FULL_DECISION_REPAIR top-level contract не совпадает со schema.",
            invalid_result=result,
            validation_error="Top-level contract mismatch after repair.",
        )
    return result


def _wave_key(point: dict) -> tuple:
    return (
        str(point.get("scenario", "")),
        str(point.get("degree", "")),
        str(point.get("timeframe", "")),
        int(point.get("sequence", 0) or 0),
        str(point.get("label", "")),
        str(point.get("time", "")),
        point.get("price"),
    )


def _merge_unique(items: list[dict], key_builder) -> list[dict]:
    ordered = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        ordered[key_builder(item)] = copy.deepcopy(item)
    return list(ordered.values())


def _generic_visual_key(item: dict) -> str:
    return _compact_json(item)


def _bilingual_parts(value) -> tuple[str, str]:
    text = str(value or "").strip()
    if text.startswith("EN:") and "\nRU:" in text:
        english, russian = text[3:].split("\nRU:", 1)
        return english.strip(), russian.strip()
    if text.startswith("EN:") and " RU:" in text:
        english, russian = text[3:].split(" RU:", 1)
        return english.strip(), russian.strip()
    return text, text


def _merge_bilingual_text(*values, english: str = "", russian: str = "") -> str:
    english_parts = []
    russian_parts = []
    for value in values:
        en_part, ru_part = _bilingual_parts(value)
        if en_part:
            english_parts.append(en_part)
        if ru_part:
            russian_parts.append(ru_part)
    if english:
        english_parts.append(english.strip())
    if russian:
        russian_parts.append(russian.strip())
    return f"EN: {' '.join(english_parts)}\nRU: {' '.join(russian_parts)}"


def assemble_staged_analysis(
    *,
    payload: dict,
    market_map: dict,
    trade_decision: dict,
    previous_reference: dict | None,
) -> dict:
    """Assembles and validates the original FULL business contract."""
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    previous_by_id = {
        str(item["anchor_id"]): copy.deepcopy(item["point"])
        for item in previous_anchors.get("anchors", [])
        if isinstance(item, dict) and item.get("anchor_id")
    }
    revision = market_map["wave_revision"]
    preserved_ids = set(revision.get("preserved_anchor_ids", []))
    invalidated_ids = set(revision.get("invalidated_anchor_ids", []))
    invalidated_keys = {
        _wave_key(previous_by_id[identifier])
        for identifier in invalidated_ids
        if identifier in previous_by_id
    }

    map_visual = market_map.get("visualization") or {}
    decision_visual = trade_decision.get("visualization") or {}
    wave_candidates = [
        previous_by_id[identifier]
        for identifier in previous_by_id
        if identifier in preserved_ids
    ]
    wave_candidates.extend(map_visual.get("wave_points") or [])
    wave_candidates.extend(decision_visual.get("wave_points") or [])
    wave_candidates = [
        point
        for point in wave_candidates
        if isinstance(point, dict) and _wave_key(point) not in invalidated_keys
    ]

    visualization = {
        "wave_points": _merge_unique(wave_candidates, _wave_key),
        "levels": _merge_unique(
            list(map_visual.get("levels") or [])
            + list(decision_visual.get("levels") or []),
            _generic_visual_key,
        ),
        "zones": _merge_unique(
            list(map_visual.get("zones") or [])
            + list(decision_visual.get("zones") or []),
            _generic_visual_key,
        ),
        "scenario_paths": _merge_unique(
            list(map_visual.get("scenario_paths") or [])
            + list(decision_visual.get("scenario_paths") or []),
            _generic_visual_key,
        ),
        "trendlines": _merge_unique(
            list(map_visual.get("trendlines") or [])
            + list(decision_visual.get("trendlines") or []),
            _generic_visual_key,
        ),
        "channels": _merge_unique(
            list(map_visual.get("channels") or [])
            + list(decision_visual.get("channels") or []),
            _generic_visual_key,
        ),
        "pattern_shapes": _merge_unique(
            list(map_visual.get("pattern_shapes") or [])
            + list(decision_visual.get("pattern_shapes") or []),
            _generic_visual_key,
        ),
        "market_events": _merge_unique(
            list(map_visual.get("market_events") or [])
            + list(decision_visual.get("market_events") or []),
            _generic_visual_key,
        ),
        "projected_waves": _merge_unique(
            list(map_visual.get("projected_waves") or [])
            + list(decision_visual.get("projected_waves") or []),
            _generic_visual_key,
        ),
        "wave_structures": _merge_unique(
            list(map_visual.get("wave_structures") or [])
            + list(decision_visual.get("wave_structures") or []),
            _generic_visual_key,
        ),
        "chart_comment": _merge_bilingual_text(
            map_visual.get("chart_comment"),
            decision_visual.get("chart_comment"),
            english=(
                f"Wave revision={revision.get('mode')}; "
                f"preserved={len(preserved_ids)}; "
                f"invalidated={len(invalidated_ids)}."
            ),
            russian=(
                f"Ревизия волн={revision.get('mode')}; "
                f"сохранено={len(preserved_ids)}; "
                f"отменено={len(invalidated_ids)}."
            ),
        ),
    }

    map_tf = copy.deepcopy(market_map["timeframe_analysis"])
    map_tf["H1"] = _merge_bilingual_text(
        map_tf["H1"], trade_decision["h1_execution_context"]
    )
    map_tf["relationship"] = _merge_bilingual_text(
        map_tf["relationship"], trade_decision["multi_timeframe_relationship"]
    )

    map_quality = market_map.get("data_quality") or {}
    decision_quality = trade_decision.get("data_quality") or {}
    quality_issues = []
    for value in (map_quality.get("issues"), decision_quality.get("issues")):
        normalized = str(value or "").strip()
        if normalized and normalized.lower() not in {"none", "no issues"}:
            if normalized not in quality_issues:
                quality_issues.append(normalized)

    analysis = {
        "timestamp": str(payload.get("timestamp") or trade_decision["timestamp"]),
        "instrument": SYMBOL,
        "market_regime": copy.deepcopy(market_map["market_regime"]),
        "timeframe_analysis": map_tf,
        "price_structure": copy.deepcopy(market_map["price_structure"]),
        "patterns": _merge_bilingual_text(
            market_map["patterns"], trade_decision["microstructure_and_patterns"]
        ),
        "wave_count": copy.deepcopy(market_map["wave_count"]),
        "higher_timeframe_context": copy.deepcopy(
            market_map["higher_timeframe_context"]
        ),
        "scenario_map": copy.deepcopy(market_map["scenario_map"]),
        "visualization": visualization,
        "recommendation": copy.deepcopy(trade_decision["recommendation"]),
        "data_quality": {
            "sufficient": bool(map_quality.get("sufficient"))
            and bool(decision_quality.get("sufficient")),
            "issues": "; ".join(quality_issues) if quality_issues else "none",
        },
    }

    if set(analysis) != set(CLAUDE_RESPONSE_SCHEMA["required"]):
        raise ValueError("Assembled FULL top-level contract изменён.")
    validate_trade_levels(analysis)
    validate_analysis_contract(analysis)
    sanitize_visualization(analysis, payload)
    _validate_fvg_selection(analysis, payload)
    return analysis


def combine_stage_usage(
    map_usage: dict | None,
    decision_usage: dict | None,
) -> dict:
    map_usage = dict(map_usage) if isinstance(map_usage, dict) else {}
    decision_usage = (
        dict(decision_usage) if isinstance(decision_usage, dict) else {}
    )
    fields = {
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "thinking_tokens",
    }
    totals = {
        field: int(map_usage.get(field, 0) or 0)
        + int(decision_usage.get(field, 0) or 0)
        for field in sorted(fields)
    }
    return {
        "staged_analysis_version": STAGED_ANALYSIS_VERSION,
        "market_map": map_usage,
        "trade_decision": decision_usage,
        "totals": totals,
    }
