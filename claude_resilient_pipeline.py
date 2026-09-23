"""WaveFrame V8.5.3 resilient sequential Claude pipeline.

Goals:
- small strict schemas instead of one giant compiled grammar;
- strictly sequential paid requests;
- durable checkpoint after every complete field;
- repair only missing fields;
- resume after process restart without repurchasing validated fields;
- global circuit breaker for permanent API/request/configuration errors;
- final legacy market-map/trade-decision contracts stay unchanged downstream.

Risk Manager and Executor are intentionally not imported here.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from instruments import active_instrument, symbol_state_path
from response_contract import named_row
from chart_contract import sanitize_visualization
from claude_stream_recovery import validate_closed_json_schema
from claude_client import (
    ClaudeInvalidResponseError,
    ClaudePermanentRequestError,
    build_transport_payload,
    claude_api_retry_context,
    get_error_failure_class,
    get_error_request_id,
    get_error_retry_after_seconds,
    get_model,
    is_outcome_unknown_error,
    is_retryable_claude_error,
    load_anthropic_config,
)
from claude_staged_client import (
    _request_structured_stage,
    _filtered_raw_payload,
    _filtered_deterministic_market_facts,
    _expand_market_map_wire_result,
    _expand_trade_decision_wire_result,
    build_previous_confirmed_anchor_reference,
    validate_market_map_result,
    MARKET_REGIME_COLUMNS,
    TIMEFRAME_ANALYSIS_COLUMNS,
    PRICE_STRUCTURE_COLUMNS,
    WAVE_COUNT_COLUMNS,
    HIGHER_TIMEFRAME_CONTEXT_COLUMNS,
    SCENARIO_MAP_COLUMNS,
    RECOMMENDATION_COLUMNS,
    WAVE_POINT_COLUMNS,
    LEVEL_COLUMNS,
    ZONE_COLUMNS,
    SCENARIO_PATH_COLUMNS,
    TRENDLINE_COLUMNS,
    CHANNEL_COLUMNS,
    PATTERN_SHAPE_COLUMNS,
    MARKET_EVENT_COLUMNS,
    PROJECTED_WAVE_COLUMNS,
    WAVE_STRUCTURE_COLUMNS,
    TRADE_DECISION_SCHEMA,
    POSITION_REVIEW_SCHEMA,
    POSITION_MANAGEMENT_WIRE_SCHEMA,
    _expand_wire_visualization,
    _expand_wire_data_quality,
    _expand_position_management_wire,
)
from api_costs import estimate_cost


SYMBOL = active_instrument()
PIPELINE_VERSION = "v8_5_3_micro_stage_v1"
STATE_PATH = symbol_state_path("claude_resilient_pipeline.json")
PAYLOAD_DIR = STATE_PATH.parent / "claude_pipeline_payloads"
MAX_SAVED_PIPELINES = 80

TRANSIENT_DELAYS_SECONDS = (5, 20, 60, 180)
MAX_TRANSIENT_ATTEMPTS_PER_STAGE = 4
MAX_OUTCOME_UNKNOWN_ATTEMPTS_PER_STAGE = 2
MAX_INVALID_ATTEMPTS_PER_STAGE = 3


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compact(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _atomic_write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(value, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _empty_state() -> dict:
    return {
        "version": PIPELINE_VERSION,
        "updated_at_utc": None,
        "breaker": {
            "active": False,
            "fingerprint": None,
            "blocked_at_utc": None,
            "stage": None,
            "failure_class": None,
            "status_code": None,
            "request_id": None,
            "error": None,
        },
        "runtime_status": {
            "status": "IDLE",
            "message_ru": "ИИ-анализ ожидает нового рыночного цикла.",
            "updated_at_utc": _utc_now(),
        },
        "pipelines": {},
    }


def _load_state() -> dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as fh:
            value = json.load(fh)
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return _empty_state()
    if not isinstance(value, dict) or not isinstance(value.get("pipelines"), dict):
        return _empty_state()
    value.setdefault("breaker", _empty_state()["breaker"])
    value.setdefault("runtime_status", _empty_state()["runtime_status"])
    value["version"] = PIPELINE_VERSION
    return value


def _payload_path(key: str) -> Path:
    return PAYLOAD_DIR / f"{key}.json"


def _freeze_payload(key: str, payload: dict) -> tuple[dict, str, str]:
    PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
    path = _payload_path(key)
    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            frozen = json.load(fh)
        if not isinstance(frozen, dict):
            raise RuntimeError("Frozen Claude pipeline payload is not an object.")
    else:
        frozen = copy.deepcopy(payload)
        _atomic_write(path, frozen)
    encoded = _compact(frozen).encode("utf-8")
    return frozen, hashlib.sha256(encoded).hexdigest(), str(path)


def _load_pipeline_payload(pipeline: dict, fallback: dict) -> dict:
    path_value = pipeline.get("payload_path")
    if path_value:
        try:
            with open(Path(str(path_value)), "r", encoding="utf-8") as fh:
                value = json.load(fh)
            if isinstance(value, dict):
                digest = hashlib.sha256(_compact(value).encode("utf-8")).hexdigest()
                expected = pipeline.get("payload_sha256")
                if expected and digest != expected:
                    raise RuntimeError("Frozen Claude pipeline payload hash mismatch.")
                return value
        except FileNotFoundError:
            pass
    # Only old/migrated records may reach this path. Freeze the supplied
    # fallback immediately so the next retry becomes restart-stable.
    key = str(pipeline.get("key") or "")
    value, digest, path = _freeze_payload(key, fallback)
    pipeline["payload_sha256"] = digest
    pipeline["payload_path"] = path
    return value


def _save_state(state: dict) -> None:
    state["version"] = PIPELINE_VERSION
    state["updated_at_utc"] = _utc_now()
    pipelines = state.get("pipelines") or {}
    if len(pipelines) > MAX_SAVED_PIPELINES:
        ordered = sorted(
            pipelines.items(),
            key=lambda item: str(item[1].get("updated_at_utc") or ""),
            reverse=True,
        )
        state["pipelines"] = dict(ordered[:MAX_SAVED_PIPELINES])
    _atomic_write(STATE_PATH, state)
    # Keep frozen inputs only for pipelines retained in durable state.
    try:
        PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        keep = {f"{key}.json" for key in (state.get("pipelines") or {})}
        for item in PAYLOAD_DIR.glob("*.json"):
            if item.name not in keep:
                item.unlink(missing_ok=True)
    except OSError:
        pass


def get_ai_runtime_status() -> dict:
    value = _load_state().get("runtime_status")
    return copy.deepcopy(value) if isinstance(value, dict) else {}


def clear_ai_circuit_breaker(*, reason: str) -> dict:
    """Explicit operator reset after credentials/billing/code were repaired.

    Existing durable micro-stage checkpoints are preserved. Only the global
    permanent-error breaker is cleared.
    """
    normalized = str(reason or "").strip()
    if not normalized:
        raise ValueError("Circuit-breaker reset requires an operator reason.")
    state = _load_state()
    previous = copy.deepcopy(state.get("breaker") or {})
    state["breaker"] = _empty_state()["breaker"]
    state["runtime_status"] = {
        "status": "IDLE",
        "message_ru": (
            "Защитная блокировка ИИ снята оператором. Новый платный запрос "
            "будет разрешён только при следующем нормальном рыночном цикле."
        ),
        "reset_reason": normalized,
        "previous_breaker": previous,
        "updated_at_utc": _utc_now(),
    }
    _save_state(state)
    return copy.deepcopy(state["runtime_status"])


def _set_runtime(
    state: dict,
    status: str,
    message_ru: str,
    *,
    family: str | None = None,
    stage: str | None = None,
    stage_index: int | None = None,
    stage_total: int | None = None,
    completed_fields: int | None = None,
    required_fields: int | None = None,
    error: str | None = None,
) -> None:
    state["runtime_status"] = {
        "status": str(status),
        "message_ru": str(message_ru),
        "family": family,
        "stage": stage,
        "stage_index": stage_index,
        "stage_total": stage_total,
        "completed_fields": completed_fields,
        "required_fields": required_fields,
        "error": error,
        "updated_at_utc": _utc_now(),
    }
    _save_state(state)


@dataclass(frozen=True)
class StageDef:
    name: str
    properties: dict
    instructions: str
    max_tokens: int
    effort: str = "medium"
    timeframes: tuple[str, ...] = ()
    fact_timeframes: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()


def _string_properties(names) -> dict:
    return {str(name): {"type": "string"} for name in names}


def _array_named(columns) -> dict:
    return {"type": "array", "items": named_row(columns)}


# ------------------------------- MARKET MAP -------------------------------

MARKET_STAGE_DEFS = (
    # Raw candles are paid only in these three deep scans. The rest of the
    # market-map pipeline works from their durable validated outputs.
    StageDef(
        "MM_D1",
        _string_properties(("D1", "structure", "elliott", "levels", "patterns", "liquidity", "anchors")),
        """Полностью проанализируй D1 raw candles один раз. D1 — итоговый
двуязычный анализ. Отдельно сохрани structure, Elliott candidates, key levels,
patterns, liquidity и exact anchors (timestamp/price), чтобы следующие stages
не покупали D1 raw повторно.""",
        14000, timeframes=("D1",), fact_timeframes=("D1",),
    ),
    StageDef(
        "MM_H4",
        _string_properties(("H4", "structure", "elliott", "levels", "patterns", "liquidity", "anchors")),
        """Полностью проанализируй H4 raw candles внутри validated D1 scan.
H4 — итоговый двуязычный анализ. Отдельно сохрани structure, Elliott candidates,
levels, patterns, liquidity и exact anchors. Отличай вложенную коррекцию от
смены D1 режима.""",
        15000, timeframes=("H4",), fact_timeframes=("H4",), dependencies=("MM_D1",),
    ),
    StageDef(
        "MM_H1",
        _string_properties(("H1", "structure", "elliott", "levels", "patterns", "liquidity", "anchors")),
        """Полностью проанализируй H1 raw candles как рабочий timeframe внутри
validated D1/H4. H1 — итоговый двуязычный анализ. Отдельно сохрани swings,
BOS/CHOCH, Elliott candidates, levels, patterns, liquidity и exact anchors.""",
        16000, timeframes=("H1",), fact_timeframes=("H1",), dependencies=("MM_D1", "MM_H4"),
    ),
    StageDef(
        "MM_CONTEXT",
        {
            "regime_primary_regime": {
                "type": "string",
                "enum": [
                    "trend", "correction", "range", "breakout",
                    "reversal", "transition", "unclear",
                ],
            },
            "regime_direction": {
                "type": "string",
                "enum": [
                    "bullish", "bearish", "neutral", "mixed", "unclear",
                ],
            },
            "regime_current_phase": {"type": "string"},
            "regime_phase_status": {
                "type": "string",
                "enum": [
                    "developing", "mature", "completing", "completed",
                    "transitioning", "failed", "unclear",
                ],
            },
            **_string_properties((
                "regime_maturity", "regime_location", "regime_summary",
                "relationship", "relationship_summary",
                "htf_d1_trend", "htf_d1_wave_context",
                "htf_h4_trend", "htf_h4_wave_context",
                "htf_alignment", "htf_summary",
            )),
        },
        """Синтезируй market regime, D1/H4/H1 relationship и higher-timeframe
context только из validated timeframe scans. Не требуй повторной передачи raw.

КРИТИЧНО:
- regime_primary_regime только:
  trend/correction/range/breakout/reversal/transition/unclear;
- regime_direction только:
  bullish/bearish/neutral/mixed/unclear;
- regime_phase_status только:
  developing/mature/completing/completed/transitioning/failed/unclear;
- enum-поля не содержат объяснений;
- ни одно поле не может содержать placeholder, *_placeholder или временную
  заглушку. Если информации недостаточно, дай содержательную conservative
  оценку из validated upstream данных.""",
        10500, dependencies=("MM_D1", "MM_H4", "MM_H1"),
    ),
    StageDef(
        "MM_STRUCTURE",
        {
            **_string_properties(PRICE_STRUCTURE_COLUMNS),
            "patterns": {"type": "string"},
        },
        """Синтезируй итоговую price_structure и patterns checklist из
validated D1/H4/H1 scans. Используй только координаты, уже извлечённые raw
scan stages; не выдумывай фигуры/уровни.""",
        9500, dependencies=("MM_D1", "MM_H4", "MM_H1", "MM_CONTEXT"),
    ),
    StageDef(
        "MM_ELLIOTT",
        _string_properties(WAVE_COUNT_COLUMNS),
        """Синтезируй единый D1->H4->H1 Elliott count: primary+alternate,
current phase и objective invalidation. Проверь impulse/diagonal/zigzag/flat/
triangle/W-X-Y/alternation. Не подгоняй count под сделку.
КРИТИЧНО: invalidation_level  ТОЛЬКО одна десятичная строка цены, например
"4334.35", либо "" если объективного уровня нет. Никакого текста, EN/RU,
условий или нескольких уровней в invalidation_level не помещай; объяснение
пиши в alternate_count/summary.""",
        11000, dependencies=("MM_D1", "MM_H4", "MM_H1", "MM_STRUCTURE"),
    ),
    StageDef(
        "MM_SCENARIOS",
        _string_properties(SCENARIO_MAP_COLUMNS),
        """Построй primary/alternate scenario, expected path, current/next
opportunity и regime-change trigger только из validated context/structure/Elliott.""",
        8500, dependencies=("MM_CONTEXT", "MM_STRUCTURE", "MM_ELLIOTT"),
    ),
    StageDef(
        "MM_WAVE_REVISION",
        {
            "mode": {"type": "string", "enum": ["initialize", "unchanged", "extend", "recount"]},
            "preserved_anchor_ids": {"type": "array", "items": {"type": "string"}},
            "invalidated_anchor_ids": {"type": "array", "items": {"type": "string"}},
            "reason": {"type": "string"},
        },
        """Сравни current Elliott synthesis с previous_confirmed_wave_anchors.
Каждый старый anchor классифицируй ровно один раз: preserved или invalidated.""",
        7000, dependencies=("MM_ELLIOTT",),
    ),
    StageDef(
        "MM_VIS_WAVES",
        {
            "wave_points": _array_named(WAVE_POINT_COLUMNS),
            "wave_structures": _array_named(WAVE_STRUCTURE_COLUMNS),
            "projected_waves": _array_named(PROJECTED_WAVE_COLUMNS),
        },
        """Построй Elliott visualization только из validated exact anchors
scan stages + Elliott/scenarios. Python затем проверит координаты по raw.""",
        12000, dependencies=("MM_D1", "MM_H4", "MM_H1", "MM_ELLIOTT", "MM_SCENARIOS", "MM_WAVE_REVISION"),
    ),
    StageDef(
        "MM_VIS_LEVELS_EVENTS",
        {
            "levels": _array_named(LEVEL_COLUMNS),
            "zones": _array_named(ZONE_COLUMNS),
            "market_events": _array_named(MARKET_EVENT_COLUMNS),
            "scenario_paths": _array_named(SCENARIO_PATH_COLUMNS),
        },
        """Построй только реально используемые levels/zones/events/scenario
paths из validated scans/structure/scenarios. Filled/inactive FVG не рисуй.""",
        11000, fact_timeframes=("D1", "H4", "H1"),
        dependencies=("MM_D1", "MM_H4", "MM_H1", "MM_STRUCTURE", "MM_SCENARIOS"),
    ),
    StageDef(
        "MM_VIS_GEOMETRY",
        {
            "trendlines": _array_named(TRENDLINE_COLUMNS),
            "channels": _array_named(CHANNEL_COLUMNS),
            "pattern_shapes": _array_named(PATTERN_SHAPE_COLUMNS),
        },
        """Построй только подтверждённую trendline/channel/pattern geometry
из exact anchors/patterns validated scans. Пустой массив лучше выдумки.""",
        10000, dependencies=("MM_D1", "MM_H4", "MM_H1", "MM_STRUCTURE"),
    ),
    StageDef(
        "MM_FINAL_META",
        _string_properties(("chart_comment", "sufficient", "issues")),
        """Дай короткий chart_comment и data quality. sufficient строго
\"true\"/\"false\"; issues одна строка. Не добавляй новый анализ.""",
        4500, dependencies=(
            "MM_D1", "MM_H4", "MM_H1", "MM_CONTEXT", "MM_STRUCTURE", "MM_ELLIOTT",
            "MM_SCENARIOS", "MM_VIS_WAVES", "MM_VIS_LEVELS_EVENTS", "MM_VIS_GEOMETRY",
        ),
    ),
)


# ---------------------------- TRADE DECISION ------------------------------

TD_VIS_DEFS = (
    StageDef(
        "TD_VIS_WAVES",
        {
            "wave_points": _array_named(WAVE_POINT_COLUMNS),
            "wave_structures": _array_named(WAVE_STRUCTURE_COLUMNS),
            "projected_waves": _array_named(PROJECTED_WAVE_COLUMNS),
        },
        """Верни только execution wave objects из validated market map и
TD_CONTEXT/decision. Raw execution candles повторно не передаются.
Все summary/basis должны быть строго bilingual:
EN: ...
RU: ...
Все price wire fields  только decimal strings; optional price может быть "".""",
        16000, effort="low",
        dependencies=("TD_CONTEXT", "TD_DECISION"),
    ),
    StageDef(
        "TD_VIS_LEVELS_EVENTS",
        {
            "levels": _array_named(LEVEL_COLUMNS),
            "zones": _array_named(ZONE_COLUMNS),
            "market_events": _array_named(MARKET_EVENT_COLUMNS),
            "scenario_paths": _array_named(SCENARIO_PATH_COLUMNS),
        },
        """Верни только execution levels/zones/events/path, реально влияющие
на decision. Не создавай декоративные объекты.
Все basis должны быть строго bilingual:
EN: ...
RU: ...
Все price wire fields  только decimal strings.""",
        12000, effort="low",
        dependencies=("TD_CONTEXT", "TD_DECISION", "TD_REASONING"),
    ),
    StageDef(
        "TD_VIS_GEOMETRY",
        {
            "trendlines": _array_named(TRENDLINE_COLUMNS),
            "channels": _array_named(CHANNEL_COLUMNS),
            "pattern_shapes": _array_named(PATTERN_SHAPE_COLUMNS),
        },
        """Верни только execution geometry, уже подтверждённую TD_CONTEXT.
Пустые массивы допустимы.
Все basis должны быть строго bilingual:
EN: ...
RU: ...
Все price wire fields  decimal strings; optional price может быть "".""",
        10000, effort="low",
        dependencies=("TD_CONTEXT", "TD_DECISION"),
    ),
)

TRADE_STAGE_DEFS = (
    StageDef(
        "TD_CONTEXT",
        _string_properties((
            "h1_execution_context",
            "microstructure_and_patterns",
            "multi_timeframe_relationship",
            "entry_candidates",
            "stop_candidates",
            "target_candidates",
            "fvg_context",
        )),
        """Полностью проанализируй свежие H1/M30/M15/M5 один раз.
H1 владеет идеей, M30 даёт промежуточную возможность,
M15 подтверждает вложенную структуру, M5 уточняет trigger.

h1_execution_context, microstructure_and_patterns и
multi_timeframe_relationship ОБЯЗАТЕЛЬНО:
EN: ...
RU: ...

Сохрани entry/stop/target candidates и FVG context.""",
        16000,
        timeframes=("H1", "M30", "M15", "M5"),
        fact_timeframes=("H1",),
    ),

    StageDef(
        "TD_DECISION",
        {
            "action": {
                "type": "string",
                "enum": ["enter_long", "enter_short", "stay_out"],
            },
            "setup_type": {
                "type": "string",
                "enum": [
                    "trend_pullback",
                    "wave3_continuation",
                    "wave5_continuation",
                    "correction_a_leg",
                    "correction_b_leg",
                    "correction_c_leg",
                    "correction_completion",
                    "range_long",
                    "range_short",
                    "range_breakout",
                    "breakout_retest",
                    "false_breakout_reversal",
                    "trend_reversal",
                    "diagonal_reversal",
                    "pattern_continuation",
                    "pattern_reversal",
                    "transition_trade",
                    "other",
                    "no_trade",
                ],
            },
            "trade_horizon": {
                "type": "string",
                "enum": ["intraday", "swing", "multi_day", "unclear"],
            },
            "setup_quality": {
                "type": "string",
                "enum": ["weak", "acceptable", "good", "excellent"],
            },
            "entry_quality": {
                "type": "string",
                "enum": ["poor", "fair", "good", "excellent"],
            },
            "order_type": {
                "type": "string",
                "enum": ["market", "limit", "stop", "none"],
            },
            "confidence": {
                "type": "string",
                "enum": ["low", "medium", "high"],
            },
            "entry_price": {"type": "string"},
            "stop_loss": {"type": "string"},
            "take_profit": {"type": "string"},
            "invalidation_level": {"type": "string"},
        },
        """Прими core action и execution prices только из validated market map
+ TD_CONTEXT.

Если edge недостаточен  stay_out.

СТРОГИЕ ПРАВИЛА:
- stay_out => setup_type=no_trade, order_type=none,
  entry_price="", stop_loss="", take_profit="",
  invalidation_level="";
- enter_long => SL < Entry < TP;
- enter_short => TP < Entry < SL;
- при входе setup_type не может быть no_trade;
- при входе setup_quality не может быть weak;
- при входе entry_quality не может быть poor;
- при входе order_type не может быть none;
- при входе invalidation_level обязателен;
- все price wire fields  только decimal strings или "".

Не добавляй пояснения внутрь enum/price полей.""",
        9000,
        dependencies=("TD_CONTEXT",),
    ),

    StageDef(
        "TD_REASONING",
        {
            **_string_properties((
                "why_now",
                "structural_stop_basis",
                "target_basis",
                "reasoning",
                "invalidation_reason",
            )),
            "fvg_role": {
                "type": "string",
                "enum": [
                    "confirmation",
                    "entry_zone",
                    "target",
                    "invalidation",
                    "conflict",
                    "neutral",
                    "no_relevant_fvg",
                ],
            },
            "fvg_ids": {"type": "string"},
            "fvg_basis": {"type": "string"},
        },
        """Дай доказательное rationale и FVG role уже выбранного decision.

why_now, structural_stop_basis, target_basis, reasoning,
invalidation_reason и fvg_basis ОБЯЗАТЕЛЬНО:
EN: ...
RU: ...

FVG сам по себе не вход.

Если fvg_role=no_relevant_fvg, fvg_ids должен быть "".
Для confirmation/entry_zone/target/invalidation/conflict
укажи точные Python fvg_ids через запятую.
neutral может иметь пустой fvg_ids.

Если TD_DECISION рекомендует вход, fvg_role=conflict недопустим.
Любое обнаруженное противоречие явно укажи.""",
        8500,
        dependencies=("TD_CONTEXT", "TD_DECISION"),
    ),

    *TD_VIS_DEFS,

    StageDef(
        "TD_FINAL_META",
        {
            "chart_comment": {"type": "string"},
            "sufficient": {
                "type": "string",
                "enum": ["true", "false"],
            },
            "issues": {"type": "string"},
        },
        """Короткий chart_comment и execution data quality.

chart_comment ОБЯЗАТЕЛЬНО:
EN: ...
RU: ...

sufficient строго "true" или "false".
issues  одна строка.
Не добавляй новый торговый анализ.""",
        4000,
        dependencies=(
            "TD_CONTEXT",
            "TD_DECISION",
            "TD_REASONING",
            "TD_VIS_WAVES",
            "TD_VIS_LEVELS_EVENTS",
            "TD_VIS_GEOMETRY",
        ),
    ),
)


# ----------------------------- POSITION REVIEW ----------------------------

def _pick_properties(source: dict, names) -> dict:
    props = source.get("properties") or {}
    return {name: copy.deepcopy(props[name]) for name in names}


_PR = POSITION_REVIEW_SCHEMA
_PM = POSITION_MANAGEMENT_WIRE_SCHEMA

POSITION_REVIEW_STAGE_DEFS = (
    # Raw candles are split into two bounded scans. Later stages use only
    # validated review context and never repurchase all six timeframes.
    StageDef(
        "PR_HTF",
        _pick_properties(_PR, (
            "h4_structure_and_patterns", "h1_parent_wave",
            "support_resistance_by_timeframe", "patterns_by_timeframe",
            "fvg_and_liquidity", "thesis_health",
        )),
        """Проанализируй D1/H4/H1 для уже открытой позиции: здоровье исходной
гипотезы, H4/H1 Elliott/structure, уровни, паттерны, FVG/liquidity. Это review,
не новый вход и не команда брокеру.""",
        12000, timeframes=("D1", "H4", "H1"), fact_timeframes=("D1", "H4", "H1"),
    ),
    StageDef(
        "PR_LTF",
        {
            **_pick_properties(_PR, (
                "m15_child_structure", "m5_microstructure", "next_checkpoint",
            )),
            "m15_wave_map": {"type": "string"},
            "ltf_anchors": {"type": "string"},
            "ltf_levels": {"type": "string"},
        },
        """Проанализируй M30/M15/M5 внутри сохранённой H1-гипотезы открытой
позиции. M15 — дочерняя структура, M5 — микро-контекст; укажи следующую
контрольную точку. Отдельно сохрани m15_wave_map, точные ltf_anchors и
ltf_levels (timestamps/prices) для следующих protection stages. Не предлагай
новый вход/доливку/разворот.""",
        9000, timeframes=("M30", "M15", "M5"), dependencies=("PR_HTF",),
    ),
    StageDef(
        "PR_STATUS",
        {
            **_pick_properties(_PR, ("position_status", "confidence", "advisory_action")),
            "summary": {"type": "string"},
        },
        """Синтезируй состояние уже открытой позиции и advisory action только
из validated HTF/LTF review. Это аналитический статус, не команда на MT5.""",
        6500, dependencies=("PR_HTF", "PR_LTF"),
    ),
    StageDef(
        "PR_MANAGEMENT_CORE",
        _pick_properties(_PM, (
            "action", "structure_confirmed", "position_direction",
            "current_m15_wave", "stop_reference_wave", "management_reason",
        )),
        """Определи только тип доказательного protection plan. Запрещены
новый вход, доливка, разворот и автозакрытие. Stop/target могут только
уменьшать риск и затем отдельно проверяются Python.""",
        6500, dependencies=("PR_HTF", "PR_LTF", "PR_STATUS"),
    ),
    StageDef(
        "PR_MANAGEMENT_STOP",
        _pick_properties(_PM, (
            "stop_wave_start_time", "stop_wave_end_time", "stop_wave_status",
            "stop_anchor_time", "stop_anchor_price", "stop_anchor_kind",
        )),
        """Верни только доказательство stop anchor предыдущей завершённой
M15 волны. Если stop не меняется, используй пустые строки/none/unclear согласно
wire contract. Все координаты должны совпадать с закрытыми raw candles.""",
        6500, dependencies=("PR_LTF", "PR_MANAGEMENT_CORE"),
    ),
    StageDef(
        "PR_MANAGEMENT_TARGET",
        _pick_properties(_PM, (
            "fib_method", "fib_timeframe", "fib_ratio",
            "fib_leg_start_time", "fib_leg_start_price", "fib_leg_start_kind",
            "fib_leg_end_time", "fib_leg_end_price", "fib_leg_end_kind",
            "fib_projection_time", "fib_projection_price", "fib_projection_kind",
        )),
        """Верни только Fibonacci evidence для target recalculation. Если
пересчёт цели не требуется — верни none/пустые значения согласно wire contract.
Не выдумывай anchor, которого нет в validated review.""",
        7000, dependencies=("PR_HTF", "PR_LTF", "PR_MANAGEMENT_CORE"),
    ),
    StageDef(
        "PR_VIS_WAVES",
        {
            "wave_points": _array_named(WAVE_POINT_COLUMNS),
            "wave_structures": _array_named(WAVE_STRUCTURE_COLUMNS),
            "projected_waves": _array_named(PROJECTED_WAVE_COLUMNS),
        },
        """Верни только wave visualization, необходимую для сопровождения
открытой позиции. Пустые массивы лучше неподтверждённых координат.""",
        7500, dependencies=("PR_HTF", "PR_LTF", "PR_STATUS"),
    ),
    StageDef(
        "PR_VIS_LEVELS_EVENTS",
        {
            "levels": _array_named(LEVEL_COLUMNS),
            "zones": _array_named(ZONE_COLUMNS),
            "market_events": _array_named(MARKET_EVENT_COLUMNS),
            "scenario_paths": _array_named(SCENARIO_PATH_COLUMNS),
        },
        """Верни только levels/zones/events/path, которые реально нужны для
контроля текущей позиции. Не создавай декоративные объекты.""",
        7500, dependencies=("PR_HTF", "PR_LTF", "PR_STATUS"),
    ),
    StageDef(
        "PR_VIS_GEOMETRY",
        {
            "trendlines": _array_named(TRENDLINE_COLUMNS),
            "channels": _array_named(CHANNEL_COLUMNS),
            "pattern_shapes": _array_named(PATTERN_SHAPE_COLUMNS),
        },
        """Верни только подтверждённую geometry, влияющую на сопровождение
позиции. Пустые массивы допустимы.""",
        6500, dependencies=("PR_HTF", "PR_LTF"),
    ),
    StageDef(
        "PR_FINAL_META",
        _string_properties(("chart_comment", "sufficient", "issues")),
        """Дай короткий chart_comment и data_quality. Не добавляй новый
рыночный тезис; только оцени полноту уже проведённого position review.""",
        4500, dependencies=(
            "PR_HTF", "PR_LTF", "PR_STATUS", "PR_MANAGEMENT_CORE",
            "PR_MANAGEMENT_STOP", "PR_MANAGEMENT_TARGET", "PR_VIS_WAVES",
            "PR_VIS_LEVELS_EVENTS", "PR_VIS_GEOMETRY",
        ),
    ),
)


COMMON_MARKET_SYSTEM = """
Ты выполняешь ОДИН маленький этап профессионального анализа XAUUSD.
Источник истины — только переданные raw MT5 candles и deterministic facts.
Никаких внешних новостей, индикаторов или выдуманных координат.
Этот этап не имеет права отправлять ордера и не является Risk Manager.
Верни только поля текущей JSON schema, без преамбулы и без дополнительных ключей.
Каждое содержательное объяснение пиши в формате `EN: ...\nRU: ...`.
Числовые wire-поля, если schema требует string, возвращай строкой.
Учитывай уже validated upstream results, но raw data имеет приоритет при конфликте.
""".strip()

COMMON_TRADE_SYSTEM = """
Ты выполняешь ОДИН маленький этап execution-анализа XAUUSD на DEMO-системе.
Ты не имеешь доступа к брокеру и не исполняешь сделку. Python отдельно
проверяет Risk Manager и Executor gates. Источник истины — validated market map
и свежие raw MT5 H1/M30/M15/M5. M5 не отменяет H1/H4 без структурного основания.
Верни только поля текущей JSON schema. При отсутствии edge используй stay_out,
а не отказ. Содержательные объяснения — `EN: ...\nRU: ...`.
""".strip()


def _spec_fingerprint() -> str:
    model = "unknown"
    try:
        model = get_model(load_anthropic_config())
    except Exception:
        pass
    serial = {
        "version": PIPELINE_VERSION,
        "model": model,
        "market": [
            [s.name, s.properties, s.max_tokens, s.effort] for s in MARKET_STAGE_DEFS
        ],
        "trade": [
            [s.name, s.properties, s.max_tokens, s.effort] for s in TRADE_STAGE_DEFS
        ],
        "position_review": [
            [s.name, s.properties, s.max_tokens, s.effort]
            for s in POSITION_REVIEW_STAGE_DEFS
        ],
    }
    return hashlib.sha256(_compact(serial).encode("utf-8")).hexdigest()


def _breaker_active(state: dict) -> bool:
    breaker = state.get("breaker") or {}
    if not breaker.get("active"):
        return False
    current = _spec_fingerprint()
    if breaker.get("fingerprint") != current:
        state["breaker"] = _empty_state()["breaker"]
        _save_state(state)
        return False
    return True


def ensure_ai_request_allowed() -> None:
    """Block every new paid Claude call while the global breaker is active."""
    state = _load_state()
    if not _breaker_active(state):
        return
    breaker = state.get("breaker") or {}
    raise ClaudePermanentRequestError(
        "WaveFrame V8.5.3 global AI circuit breaker is active. "
        f"Stage={breaker.get('stage')}; cause={breaker.get('error')}",
        request_id=breaker.get("request_id"),
        status_code=int(breaker.get("status_code") or 400),
    )


def register_permanent_ai_failure(stage: str, error: Exception) -> dict:
    """Open the global breaker for any permanent Claude stage.

    This public hook is used by the legacy small Scout/other wrappers too, so
    a permanent 400/401/402/403/413 can never be bypassed by the next H1 key.
    """
    state = _load_state()
    if not _is_permanent(error):
        return copy.deepcopy(state.get("runtime_status") or {})
    _open_breaker(state, str(stage or "UNKNOWN"), error)
    return copy.deepcopy(state.get("runtime_status") or {})


def _open_breaker(state: dict, stage: str, error: Exception) -> None:
    state["breaker"] = {
        "active": True,
        "fingerprint": _spec_fingerprint(),
        "blocked_at_utc": _utc_now(),
        "stage": stage,
        "failure_class": get_error_failure_class(error),
        "status_code": getattr(error, "status_code", None),
        "request_id": get_error_request_id(error),
        "error": f"{type(error).__name__}: {error}",
    }
    _set_runtime(
        state,
        "PERMANENT_BLOCKED",
        "ИИ-анализ не проводится: Anthropic вернул постоянную ошибку. "
        "Новые платные запросы автоматически остановлены.",
        stage=stage,
        error=str(error),
    )


def _is_permanent(error: Exception) -> bool:
    if isinstance(error, ClaudePermanentRequestError):
        return True
    status = int(getattr(error, "status_code", 0) or 0)
    text = str(error).lower()
    if status in {400, 401, 402, 403, 404, 413}:
        return True
    if status == 429 and get_error_retry_after_seconds(error) is None:
        if any(word in text for word in ("spend", "billing", "monthly", "quota")):
            return True
    return False


def _pipeline_key(
    family: str, payload: dict, pipeline_identity: str | None = None
) -> str:
    timestamp = str(payload.get("timestamp") or payload.get("generated_at_fp") or "")
    identity = str(pipeline_identity or timestamp)
    raw = f"{PIPELINE_VERSION}|{family}|{identity}|{SYMBOL}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _ensure_pipeline(
    state: dict,
    family: str,
    payload: dict,
    specs: tuple[StageDef, ...],
    pipeline_identity: str | None = None,
) -> tuple[str, dict]:
    key = _pipeline_key(family, payload, pipeline_identity)
    pipeline = (state.get("pipelines") or {}).get(key)
    if not isinstance(pipeline, dict):
        pipeline = {
            "pipeline_id": str(uuid.uuid4()),
            "key": key,
            "family": family,
            "symbol": SYMBOL,
            "snapshot_timestamp": payload.get("timestamp"),
            "pipeline_identity": str(pipeline_identity or payload.get("timestamp") or ""),
            "status": "RUNNING",
            "created_at_utc": _utc_now(),
            "updated_at_utc": _utc_now(),
            "stage_order": [spec.name for spec in specs],
            "stages": {},
            "usage": {},
        }
        frozen, payload_sha256, payload_path = _freeze_payload(key, payload)
        pipeline["payload_sha256"] = payload_sha256
        pipeline["payload_path"] = payload_path
        state.setdefault("pipelines", {})[key] = pipeline
        _save_state(state)
    else:
        # Existing identity always continues the original frozen input.
        frozen = _load_pipeline_payload(pipeline, payload)
        if not pipeline.get("payload_sha256"):
            pipeline["payload_sha256"] = hashlib.sha256(
                _compact(frozen).encode("utf-8")
            ).hexdigest()
        if not pipeline.get("payload_path"):
            pipeline["payload_path"] = str(_payload_path(key))
        _save_state(state)
    return key, pipeline


def _ensure_stage(pipeline: dict, spec: StageDef) -> dict:
    stages = pipeline.setdefault("stages", {})
    stage = stages.get(spec.name)
    if not isinstance(stage, dict):
        stage = {
            "name": spec.name,
            "status": "PENDING",
            "required_fields": list(spec.properties),
            "values": {},
            "attempts": [],
            "created_at_utc": _utc_now(),
            "updated_at_utc": _utc_now(),
        }
        stages[spec.name] = stage
    return stage


def _is_decimal_wire_string(value) -> bool:
    """Strict legacy wire decimal: plain decimal string or empty string."""
    if not isinstance(value, str):
        return False

    if value == "":
        return True

    # No whitespace, exponent notation, NaN or Infinity.
    if value != value.strip():
        return False

    text = value
    if text[:1] in {"+", "-"}:
        text = text[1:]

    if not text:
        return False

    if text.count(".") > 1:
        return False

    if "." in text:
        whole, fraction = text.split(".", 1)

        if not fraction or not fraction.isdigit():
            return False

        if whole and not whole.isdigit():
            return False

        return bool(whole or fraction)

    return text.isdigit()



def _is_bilingual_wire_text(value) -> bool:
    if not isinstance(value, str):
        return False

    text = value.strip()

    if not text.startswith("EN:"):
        return False

    marker = "\nRU:"

    if marker not in text:
        return False

    english, russian = text[3:].split(marker, 1)

    english = english.strip()
    russian = russian.strip()

    if not english or not russian:
        return False

    if english.casefold() == russian.casefold():
        return False

    return True


def _is_integer_wire_string(value) -> bool:
    if not isinstance(value, str):
        return False

    if value != value.strip() or not value:
        return False

    try:
        number = int(value)
    except (TypeError, ValueError):
        return False

    return value in {str(number), f"+{number}"}


def _trade_visual_field_semantically_valid(
    spec_name: str,
    field_name: str,
    value,
) -> bool:
    rules = {
        "TD_VIS_WAVES": {
            "wave_points": {
                "required_decimal": {"price"},
                "optional_decimal": set(),
                "integer": {"sequence"},
                "bilingual": set(),
            },
            "wave_structures": {
                "required_decimal": set(),
                "optional_decimal": {
                    "confirmation_level",
                    "invalidation_level",
                },
                "integer": set(),
                "bilingual": {"summary"},
            },
            "projected_waves": {
                "required_decimal": {
                    "anchor_price",
                    "target_price_low",
                    "target_price_high",
                },
                "optional_decimal": {
                    "confirmation_level",
                    "invalidation_level",
                },
                "integer": set(),
                "bilingual": {"basis"},
            },
        },

        "TD_VIS_LEVELS_EVENTS": {
            "levels": {
                "required_decimal": {"price"},
                "optional_decimal": set(),
                "integer": set(),
                "bilingual": {"basis"},
            },
            "zones": {
                "required_decimal": {
                    "price_low",
                    "price_high",
                },
                "optional_decimal": set(),
                "integer": set(),
                "bilingual": set(),
            },
            "market_events": {
                "required_decimal": {"price"},
                "optional_decimal": set(),
                "integer": set(),
                "bilingual": {"basis"},
            },
            "scenario_paths": {
                "required_decimal": {
                    "anchor_price",
                    "target_price_low",
                    "target_price_high",
                },
                "optional_decimal": set(),
                "integer": set(),
                "bilingual": set(),
            },
        },

        "TD_VIS_GEOMETRY": {
            "trendlines": {
                "required_decimal": {
                    "start_price",
                    "end_price",
                },
                "optional_decimal": set(),
                "integer": set(),
                "bilingual": {"basis"},
            },
            "channels": {
                "required_decimal": {
                    "upper_start_price",
                    "upper_end_price",
                    "lower_start_price",
                    "lower_end_price",
                },
                "optional_decimal": {
                    "breakout_price",
                    "reentry_price",
                },
                "integer": set(),
                "bilingual": {"basis"},
            },
            "pattern_shapes": {
                "required_decimal": {
                    "price_low",
                    "price_high",
                },
                "optional_decimal": {
                    "confirmation_level",
                    "invalidation_level",
                    "target_price",
                },
                "integer": set(),
                "bilingual": {"basis"},
            },
        },
    }

    field_rules = rules.get(spec_name, {}).get(field_name)

    if field_rules is None:
        return True

    if not isinstance(value, list):
        return False

    for row in value:
        if not isinstance(row, dict):
            return False

        for column in field_rules["required_decimal"]:
            if column not in row:
                return False

            cell = row[column]

            if (
                not _is_decimal_wire_string(cell)
                or cell == ""
            ):
                return False

        for column in field_rules["optional_decimal"]:
            if column not in row:
                return False

            if not _is_decimal_wire_string(row[column]):
                return False

        for column in field_rules["integer"]:
            if column not in row:
                return False

            if not _is_integer_wire_string(row[column]):
                return False

        for column in field_rules["bilingual"]:
            if column not in row:
                return False

            if not _is_bilingual_wire_text(row[column]):
                return False

    return True


def _prune_cross_invalid_fields(
    stage: dict,
    spec: StageDef,
) -> list[str]:
    values = stage.setdefault("values", {})
    dropped = []

    def drop(name: str):
        if name in values:
            values.pop(name, None)

            if name not in dropped:
                dropped.append(name)

    if spec.name == "TD_DECISION":
        action = values.get("action")

        if action == "stay_out":
            if (
                "setup_type" in values
                and values.get("setup_type") != "no_trade"
            ):
                drop("setup_type")

            if (
                "order_type" in values
                and values.get("order_type") != "none"
            ):
                drop("order_type")

            for name in (
                "entry_price",
                "stop_loss",
                "take_profit",
                "invalidation_level",
            ):
                if name in values and values.get(name) != "":
                    drop(name)

        elif action in {"enter_long", "enter_short"}:
            if (
                "setup_type" in values
                and values.get("setup_type") == "no_trade"
            ):
                drop("setup_type")

            if (
                "setup_quality" in values
                and values.get("setup_quality") == "weak"
            ):
                drop("setup_quality")

            if (
                "entry_quality" in values
                and values.get("entry_quality") == "poor"
            ):
                drop("entry_quality")

            if (
                "order_type" in values
                and values.get("order_type") == "none"
            ):
                drop("order_type")

            for name in (
                "entry_price",
                "stop_loss",
                "take_profit",
                "invalidation_level",
            ):
                if name in values and values.get(name) == "":
                    drop(name)

            price_names = (
                "entry_price",
                "stop_loss",
                "take_profit",
            )

            if all(
                name in values
                and _is_decimal_wire_string(values[name])
                and values[name] != ""
                for name in price_names
            ):
                entry = float(values["entry_price"])
                stop = float(values["stop_loss"])
                take_profit = float(values["take_profit"])

                valid_relation = (
                    stop < entry < take_profit
                    if action == "enter_long"
                    else take_profit < entry < stop
                )

                if not valid_relation:
                    for name in price_names:
                        drop(name)

    elif spec.name == "TD_REASONING":
        role = values.get("fvg_role")

        if "fvg_ids" in values:
            ids = str(values.get("fvg_ids") or "").strip()

            if role == "no_relevant_fvg" and ids:
                drop("fvg_ids")

            elif (
                role in {
                    "confirmation",
                    "entry_zone",
                    "target",
                    "invalidation",
                    "conflict",
                }
                and not ids
            ):
                drop("fvg_ids")

    if dropped:
        missing = [
            name
            for name in spec.properties
            if name not in values
        ]

        stage["status"] = (
            "VALIDATED"
            if not missing
            else ("PARTIAL" if values else "PENDING")
        )

        stage["missing_fields"] = missing
        stage["updated_at_utc"] = _utc_now()

    return dropped


def _field_semantically_valid(spec: StageDef, name: str, value) -> bool:
    decimal_fields = {
        "MM_ELLIOTT": {
            "invalidation_level",
        },
        "TD_DECISION": {
            "entry_price",
            "stop_loss",
            "take_profit",
            "invalidation_level",
        },
    }

    bilingual_fields = {
        "TD_CONTEXT": {
            "h1_execution_context",
            "microstructure_and_patterns",
            "multi_timeframe_relationship",
        },
        "TD_REASONING": {
            "why_now",
            "structural_stop_basis",
            "target_basis",
            "reasoning",
            "invalidation_reason",
            "fvg_basis",
        },
        "TD_FINAL_META": {
            "chart_comment",
        },
    }

    if name in decimal_fields.get(spec.name, set()):
        return _is_decimal_wire_string(value)

    if name in bilingual_fields.get(spec.name, set()):
        return _is_bilingual_wire_text(value)

    if spec.name in {
        "TD_VIS_WAVES",
        "TD_VIS_LEVELS_EVENTS",
        "TD_VIS_GEOMETRY",
    }:
        if not _trade_visual_field_semantically_valid(
            spec.name,
            name,
            value,
        ):
            return False

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized == "placeholder":
            return False

        if normalized == f"{name.lower()}_placeholder":
            return False

    return True


def _prune_invalid_saved_fields(stage: dict, spec: StageDef) -> list[str]:
    """Revalidate durable checkpoint fields after code/schema upgrades."""
    target = stage.setdefault("values", {})
    dropped = []

    for name in list(target):
        schema = spec.properties.get(name)

        if not isinstance(schema, dict):
            target.pop(name, None)
            dropped.append(name)
            continue

        value = target.get(name)

        try:
            validate_closed_json_schema(value, schema, f"$.{name}")
        except Exception:
            target.pop(name, None)
            dropped.append(name)
            continue

        if not _field_semantically_valid(spec, name, value):
            target.pop(name, None)
            dropped.append(name)

    if dropped:
        missing = [
            name for name in spec.properties
            if name not in target
        ]
        stage["missing_fields"] = missing
        stage["status"] = "PARTIAL" if target else "PENDING"
        stage["updated_at_utc"] = _utc_now()

    return dropped


def _merge_valid_fields(stage: dict, spec: StageDef, values: dict) -> list[str]:
    if not isinstance(values, dict):
        return []

    merged = []
    target = stage.setdefault("values", {})

    for name, value in values.items():
        schema = spec.properties.get(name)

        if not isinstance(schema, dict):
            continue

        try:
            validate_closed_json_schema(value, schema, f"$.{name}")
        except Exception:
            continue

        if not _field_semantically_valid(spec, name, value):
            continue

        target[name] = copy.deepcopy(value)
        merged.append(name)

    cross_dropped = _prune_cross_invalid_fields(
        stage,
        spec,
    )

    if cross_dropped:
        merged = [
            name
            for name in merged
            if name not in cross_dropped
        ]

    stage["updated_at_utc"] = _utc_now()

    missing = [
        name for name in spec.properties
        if name not in target
    ]

    stage["status"] = (
        "VALIDATED"
        if not missing
        else ("PARTIAL" if target else "PENDING")
    )
    stage["missing_fields"] = missing

    return merged


def _transitive_dependents(
    specs: tuple[StageDef, ...],
    source_stage: str,
) -> list[str]:
    descendants = set()
    frontier = {str(source_stage)}

    while frontier:
        next_frontier = set()

        for item in specs:
            if item.name == source_stage or item.name in descendants:
                continue

            if any(dep in frontier for dep in item.dependencies):
                descendants.add(item.name)
                next_frontier.add(item.name)

        frontier = next_frontier

    return [
        item.name
        for item in specs
        if item.name in descendants
    ]


def _invalidate_dependency_descendants(
    *,
    pipeline: dict,
    specs: tuple[StageDef, ...],
    source_stage: str,
    reason: str,
) -> list[str]:
    names = _transitive_dependents(specs, source_stage)

    spec_by_name = {
        item.name: item
        for item in specs
    }

    stages = pipeline.setdefault("stages", {})
    invalidated = []

    for name in names:
        stage = stages.get(name)

        if not isinstance(stage, dict):
            continue

        old_values = copy.deepcopy(stage.get("values") or {})
        old_attempts = copy.deepcopy(stage.get("attempts") or [])
        old_status = str(stage.get("status") or "")

        # Nothing has ever been calculated for this descendant.
        if not old_values and not old_attempts and old_status in {"", "PENDING"}:
            continue

        history = stage.setdefault(
            "dependency_invalidation_history",
            [],
        )

        history.append({
            "invalidated_at_utc": _utc_now(),
            "source_stage": str(source_stage),
            "reason": str(reason),
            "previous_status": old_status,
            "previous_values": old_values,
            "previous_attempts": old_attempts,
        })

        # Prevent unbounded durable-state growth.
        if len(history) > 5:
            del history[:-5]

        child_spec = spec_by_name[name]

        stage["status"] = "PENDING"
        stage["values"] = {}
        stage["attempts"] = []
        stage["missing_fields"] = list(child_spec.properties)
        stage["invalidated_by_dependency"] = {
            "source_stage": str(source_stage),
            "reason": str(reason),
            "invalidated_at_utc": _utc_now(),
        }
        stage["updated_at_utc"] = _utc_now()

        invalidated.append(name)

    return invalidated


def _schema_for_missing(spec: StageDef, missing: list[str]) -> dict:
    return {
        "type": "object",
        "properties": {
            name: copy.deepcopy(spec.properties[name])
            for name in missing
        },
        "required": list(missing),
        "additionalProperties": False,
    }


def _dependency_values(pipeline: dict, spec: StageDef) -> dict:
    result = {}
    stages = pipeline.get("stages") or {}
    for name in spec.dependencies:
        item = stages.get(name)
        if isinstance(item, dict) and isinstance(item.get("values"), dict):
            result[name] = copy.deepcopy(item["values"])
    return result


def _stage_context(
    *,
    family: str,
    spec: StageDef,
    payload: dict,
    previous_reference: dict | None,
    pipeline: dict,
    market_map: dict | None = None,
) -> dict:
    context = {
        "pipeline_version": PIPELINE_VERSION,
        "micro_stage": spec.name,
        "frozen_snapshot_timestamp": payload.get("timestamp"),
        "validated_upstream": _dependency_values(pipeline, spec),
    }

    if spec.timeframes:
        raw = _filtered_raw_payload(payload, spec.timeframes)
        context["raw_market"] = build_transport_payload(raw)
    if spec.fact_timeframes:
        context["deterministic_market_facts"] = (
            _filtered_deterministic_market_facts(payload, spec.fact_timeframes)
        )

    if family == "MARKET_MAP":
        if spec.name in {"MM_ELLIOTT", "MM_WAVE_REVISION", "MM_VIS_WAVES"}:
            context["previous_confirmed_wave_anchors"] = (
                build_previous_confirmed_anchor_reference(previous_reference)
            )
    elif family.startswith("TRADE_DECISION") and spec.name == "TD_CONTEXT":
        # The full validated market map is sent exactly once for the execution
        # pipeline. All later decision stages consume the durable TD_CONTEXT
        # output instead of repurchasing the map repeatedly.
        context["validated_market_map"] = copy.deepcopy(market_map or {})
    elif family == "POSITION_REVIEW":
        context["review_trigger"] = payload.get("_review_trigger")
        context["open_position_context"] = copy.deepcopy(
            payload.get("_position_context") or {}
        )
        if spec.name == "PR_HTF":
            # The original trade thesis and previous review are paid once.
            # Later protection stages use the durable PR_HTF/PR_LTF summaries.
            context["original_trade_thesis"] = copy.deepcopy(
                payload.get("_reference_analysis") or {}
            )
            context["previous_position_review"] = copy.deepcopy(
                payload.get("_previous_monitor_result") or {}
            )

    return context


def _record_usage(pipeline: dict, stage_name: str, diagnostics: dict) -> None:
    usage = diagnostics.get("usage")
    if not isinstance(usage, dict):
        return
    model = diagnostics.get("model")
    try:
        cost = estimate_cost(model, usage)
    except Exception:
        cost = None
    records = pipeline.setdefault("usage", {}).setdefault(stage_name, [])
    signature = (
        str(diagnostics.get("request_id") or ""),
        _compact(usage),
    )
    for record in records:
        if (
            str(record.get("request_id") or ""),
            _compact(record.get("usage") or {}),
        ) == signature:
            return
    records.append(
        {
            "model": model,
            "usage": copy.deepcopy(usage),
            "estimated_cost": cost,
            "request_id": diagnostics.get("request_id"),
            "billing_status": diagnostics.get("billing_status"),
            "at_utc": _utc_now(),
        }
    )


def _run_stage(
    *,
    state: dict,
    pipeline: dict,
    family: str,
    spec: StageDef,
    all_specs: tuple[StageDef, ...],
    stage_index: int,
    stage_total: int,
    payload: dict,
    previous_reference: dict | None,
    market_map: dict | None,
) -> dict:
    stage = _ensure_stage(pipeline, spec)

    dropped_saved_fields = _prune_invalid_saved_fields(
        stage,
        spec,
    )

    cross_dropped_saved_fields = _prune_cross_invalid_fields(
        stage,
        spec,
    )

    if cross_dropped_saved_fields:
        dropped_saved_fields = list(dict.fromkeys(
            list(dropped_saved_fields)
            + list(cross_dropped_saved_fields)
        ))

    if dropped_saved_fields:
        invalidated_descendants = _invalidate_dependency_descendants(
            pipeline=pipeline,
            specs=all_specs,
            source_stage=spec.name,
            reason=(
                "Local checkpoint revalidation dropped fields: "
                + ", ".join(dropped_saved_fields)
            ),
        )

        stage["last_checkpoint_revalidation"] = {
            "at_utc": _utc_now(),
            "dropped_fields": list(dropped_saved_fields),
            "invalidated_descendants": list(invalidated_descendants),
        }

        _save_state(state)

    # Crash recovery: an attempt persisted as IN_PROGRESS may have been billed.
    attempts = stage.setdefault("attempts", [])
    if attempts and attempts[-1].get("status") == "IN_PROGRESS":
        attempts[-1]["status"] = "INTERRUPTED_UNKNOWN_AFTER_RESTART"
        attempts[-1]["completed_at_utc"] = _utc_now()
        stage["status"] = "PARTIAL" if stage.get("values") else "WAITING_RETRY"
        _save_state(state)

    while True:
        missing = [
            name for name in spec.properties
            if name not in (stage.get("values") or {})
        ]
        if not missing:
            stage["status"] = "VALIDATED"
            stage["updated_at_utc"] = _utc_now()
            pipeline["updated_at_utc"] = _utc_now()
            _save_state(state)
            return {"ok": True, "values": copy.deepcopy(stage["values"])}

        if _breaker_active(state):
            return {
                "ok": False,
                "permanent": True,
                "error": RuntimeError("Global Claude circuit breaker is active."),
            }

        unknown_count = sum(
            1 for item in attempts
            if item.get("outcome_unknown")
            or item.get("status") == "INTERRUPTED_UNKNOWN_AFTER_RESTART"
        )
        transient_count = sum(
            1 for item in attempts
            if item.get("status") in {
                "FAILED_TRANSIENT", "FAILED_REFUSAL", "FAILED_INCOMPLETE",
                "INTERRUPTED_UNKNOWN_AFTER_RESTART",
            }
        )
        invalid_count = sum(
            1 for item in attempts
            if item.get("status") == "FAILED_INVALID"
        )
        if invalid_count >= MAX_INVALID_ATTEMPTS_PER_STAGE:
            stage["status"] = "TEMPORARILY_BLOCKED"
            _set_runtime(
                state,
                "TEMPORARILY_UNAVAILABLE",
                "ИИ-анализ остановлен: Claude несколько раз подряд вернул "
                "неполный/невалидный текущий микроэтап. Уже валидные поля "
                "сохранены; новый полный контекст не отправляется.",
                family=family, stage=spec.name,
                stage_index=stage_index, stage_total=stage_total,
                completed_fields=len(stage.get("values") or {}),
                required_fields=len(spec.properties),
            )
            return {
                "ok": False,
                "temporary": True,
                "error": RuntimeError("Invalid-response repair limit."),
            }
        if unknown_count >= MAX_OUTCOME_UNKNOWN_ATTEMPTS_PER_STAGE:
            stage["status"] = "TEMPORARILY_BLOCKED"
            _set_runtime(
                state,
                "TEMPORARILY_UNAVAILABLE",
                "ИИ-анализ временно остановлен: слишком много запросов с "
                "неизвестным результатом/возможной тарификацией. Уже полученные "
                "поля сохранены.",
                family=family, stage=spec.name,
                stage_index=stage_index, stage_total=stage_total,
                completed_fields=len(stage.get("values") or {}),
                required_fields=len(spec.properties),
            )
            return {
                "ok": False,
                "temporary": True,
                "error": RuntimeError("Outcome-unknown safety limit."),
            }
        if transient_count >= MAX_TRANSIENT_ATTEMPTS_PER_STAGE:
            stage["status"] = "TEMPORARILY_BLOCKED"
            _set_runtime(
                state,
                "TEMPORARILY_UNAVAILABLE",
                "ИИ-анализ временно недоступен после ограниченного числа "
                "повторов. Полученные поля сохранены; полный контекст заново "
                "не покупается.",
                family=family, stage=spec.name,
                stage_index=stage_index, stage_total=stage_total,
                completed_fields=len(stage.get("values") or {}),
                required_fields=len(spec.properties),
            )
            return {
                "ok": False,
                "temporary": True,
                "error": RuntimeError("Transient retry limit."),
            }

        request_schema = _schema_for_missing(spec, missing)
        context_payload = _stage_context(
            family=family,
            spec=spec,
            payload=payload,
            previous_reference=previous_reference,
            pipeline=pipeline,
            market_map=market_map,
        )
        context_payload["already_validated_fields"] = copy.deepcopy(stage.get("values") or {})
        context_payload["required_now"] = list(missing)

        attempt = {
            "attempt_id": str(uuid.uuid4()),
            "status": "IN_PROGRESS",
            "started_at_utc": _utc_now(),
            "requested_fields": list(missing),
            "request_id": None,
            "failure_class": None,
            "error": None,
            "outcome_unknown": False,
            "partial_fields_saved": [],
        }
        attempts.append(attempt)
        stage["status"] = "IN_PROGRESS"
        stage["updated_at_utc"] = _utc_now()
        pipeline["updated_at_utc"] = _utc_now()
        _set_runtime(
            state,
            "RUNNING" if not stage.get("values") else "RECOVERING",
            (
                f"ИИ-анализ выполняется: {spec.name}. "
                f"Нужно полей: {len(missing)}."
                if not stage.get("values")
                else f"ИИ-анализ восстанавливается: {spec.name}. "
                     f"Сохранено {len(stage.get('values') or {})}/"
                     f"{len(spec.properties)} полей; запрашиваются только "
                     f"недостающие."
            ),
            family=family, stage=spec.name,
            stage_index=stage_index, stage_total=stage_total,
            completed_fields=len(stage.get("values") or {}),
            required_fields=len(spec.properties),
        )

        diagnostics = {}

        def on_progress(values: dict):
            diagnostics.update(values or {})
            request_id = diagnostics.get("request_id")
            if request_id:
                attempt["request_id"] = request_id
            partial = diagnostics.get("partial_fields")
            if isinstance(partial, dict):
                merged = _merge_valid_fields(stage, spec, partial)
                if merged:
                    attempt["partial_fields_saved"] = sorted(
                        set(attempt.get("partial_fields_saved") or []) | set(merged)
                    )
                    pipeline["updated_at_utc"] = _utc_now()
                    _save_state(state)
            _record_usage(pipeline, spec.name, diagnostics)
            _save_state(state)

        previous_failure = None
        if len(attempts) >= 2:
            previous_failure = attempts[-2].get("failure_class")

        try:
            with claude_api_retry_context(
                previous_failure_class=previous_failure,
                retry_round=1,
                attempt_number=len(attempts),
            ):
                result = _request_structured_stage(
                    stage=spec.name,
                    stage_payload=context_payload,
                    system_prompt=(
                        (COMMON_MARKET_SYSTEM if family == "MARKET_MAP" else COMMON_TRADE_SYSTEM)
                        + "\n\nТЕКУЩАЯ ЗАДАЧА:\n"
                        + spec.instructions
                        + "\n\nКРИТИЧНО: верни ТОЛЬКО поля из required_now. "
                          "Поля already_validated_fields не повторяй."
                    ),
                    schema=request_schema,
                    max_tokens=spec.max_tokens,
                    effort_override=spec.effort,
                    on_preflight=on_progress,
                    on_response=on_progress,
                )

            merged = _merge_valid_fields(stage, spec, result)
            attempt["partial_fields_saved"] = sorted(
                set(attempt.get("partial_fields_saved") or []) | set(merged)
            )
            rejected_fields = [
                name for name in missing
                if name not in (stage.get("values") or {})
            ]

            if rejected_fields:
                raise ClaudeInvalidResponseError(
                    (
                        f"V8.5.3 micro-stage {spec.name} returned locally "
                        f"invalid fields: {', '.join(rejected_fields)}"
                    ),
                    invalid_result=result,
                    validation_error=(
                        "Fields failed local schema/semantic validation: "
                        + ", ".join(rejected_fields)
                    ),
                )

            attempt["status"] = "VALIDATED_RESPONSE"
            attempt["completed_at_utc"] = _utc_now()
            attempt["request_id"] = diagnostics.get("request_id") or attempt.get("request_id")
            _record_usage(pipeline, spec.name, diagnostics)
            _save_state(state)
            continue

        except Exception as error:
            # Salvage complete fields both from interrupted stream diagnostics
            # and from a locally-invalid structured result if available.
            error_diag = getattr(error, "diagnostics", None)
            if isinstance(error_diag, dict):
                diagnostics.update(error_diag)
                partial = error_diag.get("partial_fields")
                if isinstance(partial, dict):
                    merged = _merge_valid_fields(stage, spec, partial)
                    attempt["partial_fields_saved"] = sorted(
                        set(attempt.get("partial_fields_saved") or []) | set(merged)
                    )

            invalid = getattr(error, "invalid_result", None)
            if isinstance(invalid, dict):
                merged = _merge_valid_fields(stage, spec, invalid)
                attempt["partial_fields_saved"] = sorted(
                    set(attempt.get("partial_fields_saved") or []) | set(merged)
                )

            attempt["completed_at_utc"] = _utc_now()
            attempt["request_id"] = (
                diagnostics.get("request_id")
                or get_error_request_id(error)
                or attempt.get("request_id")
            )
            attempt["failure_class"] = get_error_failure_class(error)
            attempt["error"] = f"{type(error).__name__}: {error}"
            attempt["outcome_unknown"] = bool(is_outcome_unknown_error(error))
            _record_usage(pipeline, spec.name, diagnostics)

            if _is_permanent(error):
                attempt["status"] = "FAILED_PERMANENT"
                stage["status"] = "PERMANENT_BLOCKED"
                _save_state(state)
                _open_breaker(state, spec.name, error)
                return {"ok": False, "permanent": True, "error": error}

            current_missing = [
                name for name in spec.properties
                if name not in (stage.get("values") or {})
            ]
            if not current_missing:
                attempt["status"] = "RECOVERED_FROM_PARTIAL"
                stage["status"] = "VALIDATED"
                _save_state(state)
                return {"ok": True, "values": copy.deepcopy(stage["values"])}

            attempt["status"] = (
                "FAILED_TRANSIENT"
                if is_retryable_claude_error(error)
                else "FAILED_INVALID"
            )
            stage["status"] = "PARTIAL" if stage.get("values") else "WAITING_RETRY"
            _save_state(state)

            # A locally invalid/partial result is repaired immediately using
            # only still-missing fields.  Transient network/provider failures
            # use bounded backoff and still keep all completed fields.
            if isinstance(error, ClaudeInvalidResponseError):
                continue

            if is_retryable_claude_error(error) or is_outcome_unknown_error(error):
                retry_after = get_error_retry_after_seconds(error)
                delay_index = min(max(0, transient_count), len(TRANSIENT_DELAYS_SECONDS) - 1)
                delay = float(TRANSIENT_DELAYS_SECONDS[delay_index])
                if retry_after is not None:
                    delay = max(delay, float(retry_after))
                _set_runtime(
                    state,
                    "RECOVERING",
                    f"ИИ-анализ: временная ошибка на {spec.name}; "
                    f"повторятся только {len(current_missing)} недостающих полей.",
                    family=family, stage=spec.name,
                    stage_index=stage_index, stage_total=stage_total,
                    completed_fields=len(stage.get("values") or {}),
                    required_fields=len(spec.properties),
                    error=str(error),
                )
                time.sleep(delay)
                continue

            # Unknown non-permanent, non-retryable error: fail closed without
            # spending more money automatically.
            stage["status"] = "TEMPORARILY_BLOCKED"
            _set_runtime(
                state,
                "TEMPORARILY_UNAVAILABLE",
                "ИИ-анализ остановлен на неизвестной ошибке. Уже полученные "
                "поля сохранены; автоматический повтор запрещён.",
                family=family, stage=spec.name,
                stage_index=stage_index, stage_total=stage_total,
                completed_fields=len(stage.get("values") or {}),
                required_fields=len(spec.properties),
                error=str(error),
            )
            return {"ok": False, "temporary": True, "error": error}


def _run_pipeline(
    *,
    family: str,
    specs: tuple[StageDef, ...],
    payload: dict,
    previous_reference: dict | None,
    market_map: dict | None = None,
    pipeline_identity: str | None = None,
) -> tuple[dict, dict, dict, dict]:
    state = _load_state()
    if _breaker_active(state):
        breaker = state.get("breaker") or {}
        _set_runtime(
            state,
            "PERMANENT_BLOCKED",
            "ИИ-анализ не проводится: действует защитная блокировка после "
            "постоянной ошибки Anthropic. Новые платные запросы не отправляются.",
            family=family, stage=breaker.get("stage"),
            error=breaker.get("error"),
        )
        return state, {}, {
            "ok": False,
            "permanent": True,
            "error": RuntimeError(str(breaker.get("error") or "Circuit breaker active.")),
        }, copy.deepcopy(payload)

    key, pipeline = _ensure_pipeline(
        state, family, payload, specs, pipeline_identity=pipeline_identity
    )
    frozen_payload = _load_pipeline_payload(pipeline, payload)
    if pipeline.get("status") == "VALIDATED" and isinstance(
        pipeline.get("final_result"), dict
    ):
        return state, pipeline, {"ok": True, "recovered": True}, frozen_payload
    pipeline["status"] = "RUNNING"
    _save_state(state)

    for index, spec in enumerate(specs, 1):
        result = _run_stage(
            state=state,
            pipeline=pipeline,
            family=family,
            spec=spec,
            all_specs=specs,
            stage_index=index,
            stage_total=len(specs),
            payload=frozen_payload,
            previous_reference=previous_reference,
            market_map=market_map,
        )
        if not result.get("ok"):
            pipeline["status"] = (
                "PERMANENT_BLOCKED" if result.get("permanent")
                else "TEMPORARILY_BLOCKED"
            )
            pipeline["updated_at_utc"] = _utc_now()
            _save_state(state)
            return state, pipeline, result, frozen_payload

    pipeline["status"] = "STAGES_VALIDATED"
    pipeline["updated_at_utc"] = _utc_now()
    _save_state(state)
    return state, pipeline, {"ok": True}, frozen_payload


def _stage_values(pipeline: dict, name: str) -> dict:
    item = (pipeline.get("stages") or {}).get(name) or {}
    values = item.get("values")
    return copy.deepcopy(values) if isinstance(values, dict) else {}


def _aggregate_usage(pipeline: dict) -> dict:
    total = {
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "estimated_cost": 0.0,
        "micro_requests": 0,
    }
    for records in (pipeline.get("usage") or {}).values():
        if not isinstance(records, list):
            continue
        seen = set()
        for record in records:
            if not isinstance(record, dict):
                continue
            # callbacks may report the same usage more than once; dedupe by
            # request_id + serialized usage.
            signature = (
                str(record.get("request_id")),
                _compact(record.get("usage") or {}),
            )
            if signature in seen:
                continue
            seen.add(signature)
            usage = record.get("usage") or {}
            for key in (
                "input_tokens", "output_tokens",
                "cache_creation_input_tokens", "cache_read_input_tokens",
            ):
                try:
                    total[key] += int(usage.get(key) or 0)
                except (TypeError, ValueError):
                    pass
            try:
                estimate = record.get("estimated_cost")
                if isinstance(estimate, dict):
                    estimate = estimate.get("usd")
                if estimate is not None:
                    total["estimated_cost"] += float(estimate)
            except (TypeError, ValueError):
                pass
            total["micro_requests"] += 1
    return total


def run_market_map_pipeline(
    payload: dict,
    *,
    previous_reference: dict | None = None,
    snapshot: dict | None = None,
    cycle_type: str | None = None,
    archive_path=None,
) -> dict:
    """Return the same canonical market-map contract used by V8.5.2."""
    state, pipeline, status, frozen_payload = _run_pipeline(
        family="MARKET_MAP",
        specs=MARKET_STAGE_DEFS,
        payload=payload,
        previous_reference=previous_reference,
    )
    if not status.get("ok"):
        return status

    context = _stage_values(pipeline, "MM_CONTEXT")
    elliott_primary = _stage_values(pipeline, "MM_ELLIOTT")
    final_meta = _stage_values(pipeline, "MM_FINAL_META")

    visualization = {}
    for stage_name in (
        "MM_VIS_WAVES", "MM_VIS_LEVELS_EVENTS", "MM_VIS_GEOMETRY",
    ):
        visualization.update(_stage_values(pipeline, stage_name))
    visualization["chart_comment"] = final_meta.get("chart_comment", "")

    wire = {
        "timestamp": str(frozen_payload.get("timestamp") or ""),
        "instrument": SYMBOL,
        "market_regime": {
            "primary_regime": context.get("regime_primary_regime", ""),
            "direction": context.get("regime_direction", ""),
            "current_phase": context.get("regime_current_phase", ""),
            "phase_status": context.get("regime_phase_status", ""),
            "maturity": context.get("regime_maturity", ""),
            "location": context.get("regime_location", ""),
            "summary": context.get("regime_summary", ""),
        },
        "timeframe_analysis": {
            "D1": _stage_values(pipeline, "MM_D1").get("D1", ""),
            "H4": _stage_values(pipeline, "MM_H4").get("H4", ""),
            "H1": _stage_values(pipeline, "MM_H1").get("H1", ""),
            "relationship": context.get("relationship", ""),
            "summary": context.get("relationship_summary", ""),
        },
        "price_structure": {
            key: value for key, value in _stage_values(pipeline, "MM_STRUCTURE").items()
            if key in PRICE_STRUCTURE_COLUMNS
        },
        "patterns": _stage_values(pipeline, "MM_STRUCTURE").get("patterns", ""),
        "wave_count": elliott_primary,
        "higher_timeframe_context": {
            "d1_trend": context.get("htf_d1_trend", ""),
            "d1_wave_context": context.get("htf_d1_wave_context", ""),
            "h4_trend": context.get("htf_h4_trend", ""),
            "h4_wave_context": context.get("htf_h4_wave_context", ""),
            "alignment": context.get("htf_alignment", ""),
            "summary": context.get("htf_summary", ""),
        },
        "scenario_map": _stage_values(pipeline, "MM_SCENARIOS"),
        "visualization": visualization,
        "data_quality": {
            "sufficient": final_meta.get("sufficient", ""),
            "issues": final_meta.get("issues", ""),
        },
        "wave_revision": _stage_values(pipeline, "MM_WAVE_REVISION"),
    }

    try:
        result = _expand_market_map_wire_result(wire)
        validate_market_map_result(
            result,
            frozen_payload,
            build_previous_confirmed_anchor_reference(previous_reference),
        )
    except Exception as error:
        pipeline["status"] = "FINAL_VALIDATION_FAILED"
        pipeline["final_validation_error"] = f"{type(error).__name__}: {error}"
        pipeline["updated_at_utc"] = _utc_now()
        _set_runtime(
            state,
            "TEMPORARILY_UNAVAILABLE",
            "Все микроэтапы получены, но итоговая market map не прошла "
            "локальную перекрёстную проверку. Торговое решение не создаётся.",
            family="MARKET_MAP", stage="FINAL_VALIDATION",
            error=str(error),
        )
        return {
            "ok": False,
            "temporary": True,
            "error": ClaudeInvalidResponseError(
                f"V8.5.3 final market-map validation failed: {error}",
                invalid_result=wire,
                validation_error=str(error),
            ),
        }

    pipeline["status"] = "VALIDATED"
    pipeline["final_result"] = result
    pipeline["updated_at_utc"] = _utc_now()
    _set_runtime(
        state,
        "OK",
        "Карта рынка ИИ полностью собрана и локально валидирована.",
        family="MARKET_MAP", stage="COMPLETE",
        stage_index=len(MARKET_STAGE_DEFS), stage_total=len(MARKET_STAGE_DEFS),
    )
    return {
        "ok": True,
        "result": result,
        "usage": _aggregate_usage(pipeline),
        "micro_pipeline_id": pipeline.get("pipeline_id"),
    }


def _validate_trade_business_contract(
    *,
    payload: dict,
    market_map: dict,
    trade_decision: dict,
    previous_reference: dict | None,
) -> dict:
    # Local import avoids introducing a module-initialization dependency.
    from claude_staged_client import assemble_staged_analysis

    return assemble_staged_analysis(
        payload=payload,
        market_map=market_map,
        trade_decision=trade_decision,
        previous_reference=previous_reference,
    )


def run_trade_decision_pipeline(
    payload: dict,
    *,
    market_map: dict,
    previous_reference: dict | None = None,
    snapshot: dict | None = None,
    cycle_type: str | None = None,
    archive_path=None,
    decision_family: str = "FULL_DECISION",
    pipeline_identity: str | None = None,
) -> dict:
    """Return the same canonical trade-decision contract used by V8.5.2."""
    # decision_family is retained in state for diagnostics, while schemas stay
    # identical for FULL/H1/M30 entry decisions.
    family = f"TRADE_DECISION:{str(decision_family).upper()}"
    state, pipeline, status, frozen_payload = _run_pipeline(
        family=family,
        specs=TRADE_STAGE_DEFS,
        payload=payload,
        previous_reference=previous_reference,
        market_map=market_map,
        pipeline_identity=pipeline_identity,
    )
    if not status.get("ok"):
        return status

    rec = {}
    rec.update(_stage_values(pipeline, "TD_DECISION"))
    rec.update(_stage_values(pipeline, "TD_REASONING"))

    visualization = {}
    for stage_name in (
        "TD_VIS_WAVES", "TD_VIS_LEVELS_EVENTS", "TD_VIS_GEOMETRY",
    ):
        visualization.update(_stage_values(pipeline, stage_name))
    final_meta = _stage_values(pipeline, "TD_FINAL_META")
    visualization["chart_comment"] = final_meta.get("chart_comment", "")

    context_values = _stage_values(pipeline, "TD_CONTEXT")
    wire = {
        "timestamp": str(frozen_payload.get("timestamp") or ""),
        "instrument": SYMBOL,
        "h1_execution_context": context_values.get("h1_execution_context", ""),
        "microstructure_and_patterns": context_values.get(
            "microstructure_and_patterns", ""
        ),
        "multi_timeframe_relationship": context_values.get(
            "multi_timeframe_relationship", ""
        ),
        "visualization": visualization,
        "recommendation": rec,
        "data_quality": {
            "sufficient": final_meta.get("sufficient", ""),
            "issues": final_meta.get("issues", ""),
        },
    }

    try:
        result = _expand_trade_decision_wire_result(wire)
        if result.get("instrument") != SYMBOL:
            raise ValueError("Unexpected instrument.")
        if set(result) != set(TRADE_DECISION_SCHEMA["required"]):
            raise ValueError("Trade-decision top-level contract mismatch.")

        # IMPORTANT:
        # Micro-stages are not considered a successful trade decision until
        # the exact production FULL contract also passes local validation:
        # trade levels, business enums/cross-field rules, visualization
        # sanitizer and deterministic FVG selection.
        _validate_trade_business_contract(
            payload=frozen_payload,
            market_map=market_map,
            trade_decision=result,
            previous_reference=previous_reference,
        )
    except Exception as error:
        pipeline["status"] = "FINAL_VALIDATION_FAILED"
        pipeline["final_validation_error"] = f"{type(error).__name__}: {error}"
        pipeline["updated_at_utc"] = _utc_now()
        _set_runtime(
            state,
            "TEMPORARILY_UNAVAILABLE",
            "Все микроэтапы решения получены, но итоговый контракт не прошёл "
            "локальную проверку. Risk Manager и Executor не запускаются.",
            family=family, stage="FINAL_VALIDATION", error=str(error),
        )
        return {
            "ok": False,
            "temporary": True,
            "error": ClaudeInvalidResponseError(
                f"V8.5.3 final trade-decision validation failed: {error}",
                invalid_result=wire,
                validation_error=str(error),
            ),
        }

    pipeline["status"] = "VALIDATED"
    pipeline["final_result"] = result
    pipeline["updated_at_utc"] = _utc_now()
    _set_runtime(
        state,
        "OK",
        "ИИ-анализ и торговое решение полностью собраны и валидированы.",
        family=family, stage="COMPLETE",
        stage_index=len(TRADE_STAGE_DEFS), stage_total=len(TRADE_STAGE_DEFS),
    )
    return {
        "ok": True,
        "result": result,
        "usage": _aggregate_usage(pipeline),
        "micro_pipeline_id": pipeline.get("pipeline_id"),
    }


def run_position_review_pipeline(
    payload: dict,
    *,
    review_trigger: str,
    position_context: dict,
    previous_reference: dict | None = None,
    previous_monitor_result: dict | None = None,
    pipeline_identity: str | None = None,
) -> dict:
    """Resilient deep review for an already open managed position.

    The returned object matches the legacy POSITION_REVIEW contract consumed by
    position_protection.py. This function never sends/changes an MT5 order.
    """
    review_payload = copy.deepcopy(payload)
    review_payload["_review_trigger"] = str(review_trigger)
    review_payload["_position_context"] = copy.deepcopy(position_context or {})
    review_payload["_previous_monitor_result"] = copy.deepcopy(
        previous_monitor_result if isinstance(previous_monitor_result, dict) else {}
    )
    reference_analysis = (
        previous_reference.get("analysis")
        if isinstance(previous_reference, dict)
        and isinstance(previous_reference.get("analysis"), dict)
        else {}
    )
    review_payload["_reference_analysis"] = copy.deepcopy(reference_analysis)

    state, pipeline, status, frozen_payload = _run_pipeline(
        family="POSITION_REVIEW",
        specs=POSITION_REVIEW_STAGE_DEFS,
        payload=review_payload,
        previous_reference=previous_reference,
        pipeline_identity=pipeline_identity,
    )
    if not status.get("ok"):
        return status

    htf = _stage_values(pipeline, "PR_HTF")
    ltf = _stage_values(pipeline, "PR_LTF")
    position_status = _stage_values(pipeline, "PR_STATUS")
    management = {}
    for stage_name in (
        "PR_MANAGEMENT_CORE", "PR_MANAGEMENT_STOP", "PR_MANAGEMENT_TARGET"
    ):
        management.update(_stage_values(pipeline, stage_name))

    visualization = {}
    for stage_name in (
        "PR_VIS_WAVES", "PR_VIS_LEVELS_EVENTS", "PR_VIS_GEOMETRY"
    ):
        visualization.update(_stage_values(pipeline, stage_name))
    final_meta = _stage_values(pipeline, "PR_FINAL_META")
    visualization["chart_comment"] = final_meta.get("chart_comment", "")

    wire = {
        "timestamp": str(frozen_payload.get("timestamp") or ""),
        "instrument": SYMBOL,
        "review_trigger": str(review_trigger),
        "position_ticket": str(
            (position_context or {}).get("primary_position_ticket") or ""
        ),
        "position_status": position_status.get("position_status", "unclear"),
        "confidence": position_status.get("confidence", "low"),
        "h4_structure_and_patterns": htf.get("h4_structure_and_patterns", ""),
        "h1_parent_wave": htf.get("h1_parent_wave", ""),
        "m15_child_structure": ltf.get("m15_child_structure", ""),
        "m5_microstructure": ltf.get("m5_microstructure", ""),
        "support_resistance_by_timeframe": htf.get(
            "support_resistance_by_timeframe", ""
        ),
        "patterns_by_timeframe": htf.get("patterns_by_timeframe", ""),
        "fvg_and_liquidity": htf.get("fvg_and_liquidity", ""),
        "thesis_health": htf.get("thesis_health", ""),
        "advisory_action": position_status.get("advisory_action", "no_assessment"),
        "position_management": management,
        "next_checkpoint": ltf.get("next_checkpoint", ""),
        "summary": position_status.get("summary", ""),
        "visualization": visualization,
        "data_quality": {
            "sufficient": final_meta.get("sufficient", "false"),
            "issues": final_meta.get("issues", "none"),
        },
    }

    invalid_result = wire
    try:
        if set(wire) != set(POSITION_REVIEW_SCHEMA["required"]):
            raise ValueError("POSITION_REVIEW top-level contract mismatch.")
        result = copy.deepcopy(wire)
        result["visualization"] = _expand_wire_visualization(
            result["visualization"]
        )
        result["data_quality"] = _expand_wire_data_quality(result["data_quality"])
        result["position_management"] = _expand_position_management_wire(
            result["position_management"]
        )
        invalid_result = result

        if result.get("instrument") != SYMBOL:
            raise ValueError("POSITION_REVIEW returned unexpected instrument.")
        if str(result.get("timestamp")) != str(frozen_payload.get("timestamp")):
            raise ValueError("POSITION_REVIEW changed frozen timestamp.")
        if str(result.get("review_trigger")) != str(review_trigger):
            raise ValueError("POSITION_REVIEW changed review_trigger.")

        expected_ticket = str(
            (position_context or {}).get("primary_position_ticket") or ""
        )
        if expected_ticket and str(result.get("position_ticket")) != expected_ticket:
            raise ValueError("POSITION_REVIEW changed position_ticket.")

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
                text = str(result["position_management"].get("management_reason") or "")
            else:
                text = str(result.get(name) or "")
            if not text.startswith("EN:") or "\nRU:" not in text:
                missing_bilingual.append(name)
        if missing_bilingual:
            raise ValueError(
                "POSITION_REVIEW bilingual contract violated: "
                + ", ".join(missing_bilingual)
            )

        chart_warnings = sanitize_visualization(result, frozen_payload)
        if chart_warnings:
            issues = str(result["data_quality"].get("issues") or "").strip()
            warning_text = "chart: " + "; ".join(chart_warnings)
            result["data_quality"]["issues"] = (
                f"{issues}; {warning_text}"
                if issues and issues != "none"
                else warning_text
            )
    except Exception as error:
        pipeline["status"] = "FINAL_VALIDATION_FAILED"
        pipeline["final_validation_error"] = f"{type(error).__name__}: {error}"
        pipeline["updated_at_utc"] = _utc_now()
        _set_runtime(
            state,
            "TEMPORARILY_UNAVAILABLE",
            "Position review собран, но не прошёл локальную проверку. "
            "SL/TP не изменяются.",
            family="POSITION_REVIEW",
            stage="FINAL_VALIDATION",
            error=str(error),
        )
        return {
            "ok": False,
            "temporary": True,
            "error": ClaudeInvalidResponseError(
                f"V8.5.3 final position-review validation failed: {error}",
                invalid_result=invalid_result,
                validation_error=str(error),
            ),
        }

    pipeline["status"] = "VALIDATED"
    pipeline["final_result"] = result
    pipeline["updated_at_utc"] = _utc_now()
    _set_runtime(
        state,
        "OK",
        "ИИ-анализ открытой позиции полностью собран и валидирован.",
        family="POSITION_REVIEW",
        stage="COMPLETE",
        stage_index=len(POSITION_REVIEW_STAGE_DEFS),
        stage_total=len(POSITION_REVIEW_STAGE_DEFS),
    )
    return {
        "ok": True,
        "result": result,
        "usage": _aggregate_usage(pipeline),
        "micro_pipeline_id": pipeline.get("pipeline_id"),
    }

# ============================================================================
# COST OPTIMIZATION V8.5.3  PHASE 1
# ============================================================================

_COST_V853_PHASE1 = True
_COST_ORIGINAL_RUN_STAGE = _run_stage
_COST_ORIGINAL_STAGE_CONTEXT = _stage_context

_COST_LOCAL_MARKET_STAGES = {
    "MM_VIS_LEVELS_EVENTS",
    "MM_VIS_GEOMETRY",
    "MM_FINAL_META",
}

_COST_LOCAL_TRADE_STAGES = {
    "TD_VIS_WAVES",
    "TD_VIS_LEVELS_EVENTS",
    "TD_VIS_GEOMETRY",
    "TD_FINAL_META",
}

_COST_LOCAL_STAGES = (
    _COST_LOCAL_MARKET_STAGES
    | _COST_LOCAL_TRADE_STAGES
)


_COST_TOKEN_CAPS = {
    "MM_D1": 6500,
    "MM_H4": 6500,
    "MM_H1": 7000,
    "MM_CONTEXT": 4000,
    "MM_STRUCTURE": 3500,
    "MM_ELLIOTT": 4500,
    "MM_SCENARIOS": 3000,
    "MM_WAVE_REVISION": 1500,
    "MM_VIS_WAVES": 4500,

    "TD_CONTEXT": 6500,
    "TD_DECISION": 3000,
    "TD_REASONING": 4000,
}


_COST_LOW_EFFORT = {
    "MM_WAVE_REVISION",
    "MM_VIS_WAVES",
}


_COST_STAGE_SUFFIX = {
    "MM_D1": '''
COST DISCIPLINE:
Keep the bilingual D1 conclusion compact.
The D1 field should normally stay within ~2200 characters total;
each support field within ~700 characters.

Preserve exact levels and anchors.
Do not repeat narrative across fields.

Deep analysis is required.
Verbosity is not.
''',

    "MM_H4": '''
COST DISCIPLINE:
Keep the bilingual H4 conclusion compact.
The H4 field should normally stay within ~2200 characters total;
each support field within ~700 characters.

Do not retell MM_D1.
Preserve only decision-relevant levels and anchors.
''',

    "MM_H1": '''
COST DISCIPLINE:
Keep the bilingual H1 conclusion compact.
The H1 field should normally stay within ~2400 characters total;
each support field within ~800 characters.

Do not retell D1/H4.
Preserve objective swings, levels and anchors.
''',

    "MM_CONTEXT": '''
COST DISCIPLINE:
Synthesize instead of repeating upstream scans.
Narrative fields should normally stay within ~650 characters each.
Enums remain exact.
''',

    "MM_STRUCTURE": '''
COST DISCIPLINE:
Each field should contain only the conclusion plus strongest
objective evidence, normally <= 800 characters.
No repeated narrative.
''',

    "MM_ELLIOTT": '''
COST DISCIPLINE:
Keep primary and alternate counts technically complete but compact.

summary and alternate_count normally <= 1200 characters each.
Other narrative fields normally <= 700 characters.

invalidation_level remains exactly one decimal string or empty.
''',

    "MM_SCENARIOS": '''
COST DISCIPLINE:
Each scenario field normally <= 700 characters.

Describe only information that can change a decision.
Do not repeat the complete market map.
''',

    "MM_WAVE_REVISION": '''
COST DISCIPLINE:
Classify anchors exactly.
Keep reason <= 500 characters.
''',

    "MM_VIS_WAVES": '''
COST DISCIPLINE:
This is the only paid market chart-serialization stage.

Return at most:
- 12 wave_points
- 6 wave_structures
- 2 projected_waves

No decorative objects.
Keep summary/basis short while preserving EN/RU
where the existing contract requires it.
''',

    "TD_CONTEXT": '''
COST DISCIPLINE:
Do not restate the entire market map.

Each required bilingual context field should normally stay
within ~1800 characters total (EN + RU).

entry/stop/target candidates and FVG context must be
compact factual lists.
''',

    "TD_DECISION": '''
COST DISCIPLINE:
Return only enums and prices required by the schema.

No prose inside decision fields.
Do not repeat TD_CONTEXT.
''',

    "TD_REASONING": '''
COST DISCIPLINE:
reasoning normally <= 1800 characters total (EN + RU).

Each other bilingual basis field normally <= 1100 characters total.

State only evidence capable of changing the selected decision.
Do not retell market map or TD_CONTEXT.
''',
}


def _cost_tune_specs(
    specs: tuple[StageDef, ...],
) -> tuple[StageDef, ...]:

    tuned = []

    for spec in specs:
        suffix = _COST_STAGE_SUFFIX.get(
            spec.name,
            "",
        )

        tuned.append(
            StageDef(
                name=spec.name,

                properties=copy.deepcopy(
                    spec.properties
                ),

                instructions=(
                    spec.instructions
                    if not suffix
                    else (
                        spec.instructions
                        + "\n\n"
                        + suffix.strip()
                    )
                ),

                max_tokens=int(
                    _COST_TOKEN_CAPS.get(
                        spec.name,
                        spec.max_tokens,
                    )
                ),

                effort=(
                    "low"
                    if spec.name in _COST_LOW_EFFORT
                    else spec.effort
                ),

                timeframes=tuple(
                    spec.timeframes
                ),

                fact_timeframes=tuple(
                    spec.fact_timeframes
                ),

                dependencies=tuple(
                    spec.dependencies
                ),
            )
        )

    return tuple(tuned)


MARKET_STAGE_DEFS = _cost_tune_specs(
    MARKET_STAGE_DEFS
)

TRADE_STAGE_DEFS = _cost_tune_specs(
    TRADE_STAGE_DEFS
)


# ----------------------------------------------------------------------
# Raw context compaction.
# ----------------------------------------------------------------------

def _cost_trim_closed_bars(
    node,
    limit: int,
):

    if isinstance(node, dict):

        result = {}

        for key, value in node.items():

            if (
                key == "closed_bars"
                and isinstance(value, list)
            ):
                result[key] = copy.deepcopy(
                    value[-int(limit):]
                )

            else:
                result[key] = (
                    _cost_trim_closed_bars(
                        value,
                        limit,
                    )
                )

        return result


    if isinstance(node, list):

        return [
            _cost_trim_closed_bars(
                item,
                limit,
            )
            for item in node
        ]


    return copy.deepcopy(node)


def _cost_compact_market_map_for_trade(
    market_map: dict,
) -> dict:

    result = copy.deepcopy(
        market_map or {}
    )

    visualization = (
        result.get("visualization")
        or {}
    )

    result["visualization"] = {

        "wave_points": list(
            visualization.get(
                "wave_points"
            )
            or []
        )[-12:],

        "wave_structures": list(
            visualization.get(
                "wave_structures"
            )
            or []
        )[-6:],

        "projected_waves": list(
            visualization.get(
                "projected_waves"
            )
            or []
        )[:2],

        "levels": [],
        "zones": [],
        "market_events": [],
        "scenario_paths": [],
        "trendlines": [],
        "channels": [],
        "pattern_shapes": [],
        "chart_comment": "",
    }

    return result


def _stage_context(
    *,
    family: str,
    spec: StageDef,
    payload: dict,
    previous_reference: dict | None,
    pipeline: dict,
    market_map: dict | None = None,
) -> dict:

    context = _COST_ORIGINAL_STAGE_CONTEXT(
        family=family,
        spec=spec,
        payload=payload,
        previous_reference=previous_reference,
        pipeline=pipeline,
        market_map=market_map,
    )


    limit = {

        "MM_D1": 180,

        "MM_H4": 180,

        "MM_H1": 240,

        "TD_CONTEXT": 120,

    }.get(spec.name)


    if (
        limit
        and "raw_market" in context
    ):

        context["raw_market"] = (
            _cost_trim_closed_bars(
                context["raw_market"],
                limit,
            )
        )


    if (
        family.startswith(
            "TRADE_DECISION"
        )
        and spec.name == "TD_CONTEXT"
    ):

        context[
            "validated_market_map"
        ] = (
            _cost_compact_market_map_for_trade(
                market_map or {}
            )
        )


    return context


# ----------------------------------------------------------------------
# Deterministic Python-owned FVG visualization.
# No Claude call.
# ----------------------------------------------------------------------

def _cost_fvg_candidates(
    payload: dict,
) -> list[dict]:

    try:

        from claude_staged_client import (
            _deterministic_fvg_candidates
        )

        values = (
            _deterministic_fvg_candidates(
                payload
            )
        )

    except Exception:

        return []


    return [
        item
        for item in values
        if isinstance(item, dict)
        and item.get("active") is True
    ]


def _cost_fvg_zone(
    candidate: dict,
    scenario: str = "primary",
) -> dict | None:

    low = candidate.get(
        "active_price_low",
        candidate.get(
            "price_low"
        ),
    )

    high = candidate.get(
        "active_price_high",
        candidate.get(
            "price_high"
        ),
    )


    start_time = (
        candidate.get(
            "start_time"
        )
        or candidate.get(
            "formed_at"
        )
        or ""
    )


    timeframe = str(
        candidate.get(
            "timeframe"
        )
        or ""
    )


    if (
        low is None
        or high is None
        or not start_time
        or not timeframe
    ):

        return None


    return {

        "kind": "fvg",

        "scenario": scenario,

        "timeframe": timeframe,

        "start_time": str(
            start_time
        ),

        "end_time": str(
            start_time
        ),

        "price_low": str(
            low
        ),

        "price_high": str(
            high
        ),

        "label": (
            "Python-validated active FVG"
        ),
    }


def _cost_market_fvg_zones(
    payload: dict,
) -> list[dict]:

    zones = []


    for candidate in (
        _cost_fvg_candidates(
            payload
        )
    ):

        if str(
            candidate.get(
                "timeframe"
            )
            or ""
        ) not in {
            "D1",
            "H4",
            "H1",
        }:

            continue


        zone = _cost_fvg_zone(
            candidate
        )


        if zone:

            zones.append(
                zone
            )


        if len(zones) >= 12:

            break


    return zones


def _cost_trade_fvg_zones(
    payload: dict,
    pipeline: dict,
) -> list[dict]:

    reasoning = _stage_values(
        pipeline,
        "TD_REASONING",
    )


    selected = {

        item.strip()

        for item in str(
            reasoning.get(
                "fvg_ids"
            )
            or ""
        ).split(",")

        if item.strip()
    }


    if not selected:

        return []


    zones = []


    for candidate in (
        _cost_fvg_candidates(
            payload
        )
    ):

        identifier = str(
            candidate.get(
                "id"
            )
            or ""
        )


        if identifier not in selected:

            continue


        zone = _cost_fvg_zone(
            candidate
        )


        if zone:

            zones.append(
                zone
            )


    return zones


# ----------------------------------------------------------------------
# Local zero-cost stages.
# ----------------------------------------------------------------------

def _cost_local_stage_values(
    family: str,
    spec: StageDef,
    payload: dict,
    pipeline: dict,
) -> dict:


    if (
        spec.name
        == "MM_VIS_LEVELS_EVENTS"
    ):

        return {

            "levels": [],

            "zones": (
                _cost_market_fvg_zones(
                    payload
                )
            ),

            "market_events": [],

            "scenario_paths": [],
        }


    if (
        spec.name
        == "MM_VIS_GEOMETRY"
    ):

        return {

            "trendlines": [],

            "channels": [],

            "pattern_shapes": [],
        }


    if (
        spec.name
        == "MM_FINAL_META"
    ):

        return {

            "chart_comment": (
                "EN: Read-only chart metadata is "
                "cost-optimized and Python-validated.\n"
                "RU: Метаданные графика оптимизированы "
                "по стоимости и проверяются Python."
            ),

            "sufficient": "true",

            "issues": "none",
        }


    if (
        spec.name
        == "TD_VIS_WAVES"
    ):

        return {

            "wave_points": [],

            "wave_structures": [],

            "projected_waves": [],
        }


    if (
        spec.name
        == "TD_VIS_LEVELS_EVENTS"
    ):

        return {

            "levels": [],

            "zones": (
                _cost_trade_fvg_zones(
                    payload,
                    pipeline,
                )
            ),

            "market_events": [],

            "scenario_paths": [],
        }


    if (
        spec.name
        == "TD_VIS_GEOMETRY"
    ):

        return {

            "trendlines": [],

            "channels": [],

            "pattern_shapes": [],
        }


    if (
        spec.name
        == "TD_FINAL_META"
    ):

        return {

            "chart_comment": (
                "EN: Execution chart metadata is "
                "assembled locally from the validated "
                "decision.\n"
                "RU: Метаданные графика исполнения "
                "собираются локально из проверенного "
                "решения."
            ),

            "sufficient": "true",

            "issues": "none",
        }


    raise KeyError(
        spec.name
    )


# ----------------------------------------------------------------------
# Preserve original resilient _run_stage for all analytical stages.
# Only visualization/meta stages listed above become local.
# ----------------------------------------------------------------------

def _run_stage(
    *,
    state: dict,
    pipeline: dict,
    family: str,
    spec: StageDef,
    all_specs: tuple[StageDef, ...],
    stage_index: int,
    stage_total: int,
    payload: dict,
    previous_reference: dict | None,
    market_map: dict | None,
) -> dict:


    if (
        spec.name
        not in _COST_LOCAL_STAGES
    ):

        return (
            _COST_ORIGINAL_RUN_STAGE(
                state=state,
                pipeline=pipeline,
                family=family,
                spec=spec,
                all_specs=all_specs,
                stage_index=stage_index,
                stage_total=stage_total,
                payload=payload,
                previous_reference=(
                    previous_reference
                ),
                market_map=market_map,
            )
        )


    stage = _ensure_stage(
        pipeline,
        spec,
    )


    stage["values"] = {}


    values = (
        _cost_local_stage_values(
            family,
            spec,
            payload,
            pipeline,
        )
    )


    _merge_valid_fields(
        stage,
        spec,
        values,
    )


    missing = [

        name

        for name in spec.properties

        if name not in (
            stage.get("values")
            or {}
        )
    ]


    if missing:

        stage[
            "status"
        ] = "TEMPORARILY_BLOCKED"

        stage[
            "updated_at_utc"
        ] = _utc_now()

        _save_state(
            state
        )

        return {

            "ok": False,

            "temporary": True,

            "error": RuntimeError(
                "Local cost stage failed "
                "validation: "
                + spec.name
                + " missing "
                + ", ".join(
                    missing
                )
            ),
        }


    attempts = stage.setdefault(
        "attempts",
        [],
    )


    already_recorded = any(

        item.get(
            "status"
        ) == "LOCAL_VALIDATED"

        and item.get(
            "cost_optimization"
        ) == "V8.5.3_PHASE1"

        for item in attempts

        if isinstance(
            item,
            dict,
        )
    )


    if not already_recorded:

        attempts.append(
            {

                "attempt_id": (
                    "LOCAL_COST_"
                    + str(
                        uuid.uuid4()
                    )
                ),

                "status": (
                    "LOCAL_VALIDATED"
                ),

                "started_at_utc": (
                    _utc_now()
                ),

                "completed_at_utc": (
                    _utc_now()
                ),

                "requested_fields": list(
                    spec.properties
                ),

                "request_id": None,

                "failure_class": None,

                "error": None,

                "outcome_unknown": False,

                "partial_fields_saved": sorted(
                    stage["values"]
                ),

                "cost_optimization": (
                    "V8.5.3_PHASE1"
                ),
            }
        )


    stage[
        "status"
    ] = "VALIDATED"


    stage[
        "local_cost_optimized"
    ] = True


    stage[
        "updated_at_utc"
    ] = _utc_now()


    pipeline[
        "updated_at_utc"
    ] = _utc_now()


    _save_state(
        state
    )


    return {

        "ok": True,

        "values": copy.deepcopy(
            stage["values"]
        ),

        "local": True,
    }

# ============================================================================
# COST OPTIMIZATION V8.5.3  PHASE 2A COST LEDGER
# ============================================================================

_COST_PHASE2A_ORIGINAL_RECORD_USAGE = _record_usage


def _record_usage(
    pipeline: dict,
    stage_name: str,
    diagnostics: dict,
) -> None:

    before_records = list(
        (
            pipeline.get("usage")
            or {}
        ).get(
            stage_name,
            [],
        )
    )

    _COST_PHASE2A_ORIGINAL_RECORD_USAGE(
        pipeline,
        stage_name,
        diagnostics,
    )

    after_records = (
        (
            pipeline.get("usage")
            or {}
        ).get(
            stage_name,
            [],
        )
    )


    if (
        len(after_records)
        <= len(before_records)
    ):
        return


    try:

        from ai_cost_guard import (
            record_pipeline_usage
        )

        record_pipeline_usage(
            family=str(
                pipeline.get(
                    "family"
                )
                or ""
            ),
            stage=str(
                stage_name
            ),
            record=copy.deepcopy(
                after_records[-1]
            ),
        )

    except Exception as error:

        # Cost telemetry must never corrupt the analytical result.
        print(
            "[COST LEDGER WARNING] "
            f"{type(error).__name__}: {error}"
        )

# ============================================================================
# COST OPTIMIZATION V8.5.3  PHASE 2B POSITION REVIEW
# ============================================================================

_COST_LOCAL_POSITION_STAGES = {
    "PR_VIS_WAVES",
    "PR_VIS_LEVELS_EVENTS",
    "PR_VIS_GEOMETRY",
    "PR_FINAL_META",
}

_COST_LOCAL_STAGES = (
    set(_COST_LOCAL_STAGES)
    | _COST_LOCAL_POSITION_STAGES
)


_COST_PHASE2B_PREVIOUS_LOCAL_VALUES = (
    _cost_local_stage_values
)


def _cost_local_stage_values(
    family: str,
    spec: StageDef,
    payload: dict,
    pipeline: dict,
) -> dict:

    if spec.name == "PR_VIS_WAVES":

        return {
            "wave_points": [],
            "wave_structures": [],
            "projected_waves": [],
        }


    if spec.name == "PR_VIS_LEVELS_EVENTS":

        return {
            "levels": [],
            "zones": [],
            "market_events": [],
            "scenario_paths": [],
        }


    if spec.name == "PR_VIS_GEOMETRY":

        return {
            "trendlines": [],
            "channels": [],
            "pattern_shapes": [],
        }


    if spec.name == "PR_FINAL_META":

        return {
            "chart_comment": (
                "EN: Position chart metadata is assembled "
                "locally from the validated protection review.\n"
                "RU: Метаданные графика позиции собраны "
                "локально из подтверждённого анализа защиты."
            ),
            "sufficient": "true",
            "issues": "none",
        }


    return _COST_PHASE2B_PREVIOUS_LOCAL_VALUES(
        family,
        spec,
        payload,
        pipeline,
    )


_COST_POSITION_TOKEN_CAPS = {
    "PR_HTF": 3500,
    "PR_LTF": 3500,
    "PR_STATUS": 1400,
    "PR_MANAGEMENT_CORE": 1600,
    "PR_MANAGEMENT_STOP": 1200,
    "PR_MANAGEMENT_TARGET": 1400,
}


_COST_POSITION_LOW_EFFORT = {
    "PR_STATUS",
    "PR_MANAGEMENT_CORE",
    "PR_MANAGEMENT_STOP",
    "PR_MANAGEMENT_TARGET",
}


_COST_POSITION_SUFFIX = {
    "PR_HTF": """
COST DISCIPLINE:
Analyze deeply but answer compactly.
Each narrative field should normally stay below ~900 characters.
Do not repeat the stored trade thesis in every field.
Preserve exact structural evidence.
""",

    "PR_LTF": """
COST DISCIPLINE:
Focus only on M30/M15/M5 facts relevant to the already-open position.
Each narrative field should normally stay below ~900 characters.
Keep exact timestamps/prices for protection evidence.
""",

    "PR_STATUS": """
COST DISCIPLINE:
Synthesize only the validated HTF/LTF result.
Keep summary below ~1000 characters total.
No repeated market-map narrative.
""",

    "PR_MANAGEMENT_CORE": """
COST DISCIPLINE:
Return only the protection-plan classification and concise evidence.
management_reason should normally stay below ~900 characters.
""",

    "PR_MANAGEMENT_STOP": """
COST DISCIPLINE:
Return exact stop-anchor evidence only.
No narrative expansion.
""",

    "PR_MANAGEMENT_TARGET": """
COST DISCIPLINE:
Return exact Fibonacci target evidence only.
No narrative expansion.
""",
}


def _cost_tune_position_specs(
    specs: tuple[StageDef, ...],
) -> tuple[StageDef, ...]:

    tuned = []

    for spec in specs:

        suffix = _COST_POSITION_SUFFIX.get(
            spec.name,
            "",
        )

        tuned.append(
            StageDef(
                name=spec.name,

                properties=copy.deepcopy(
                    spec.properties
                ),

                instructions=(
                    spec.instructions
                    if not suffix
                    else (
                        spec.instructions
                        + "\n\n"
                        + suffix.strip()
                    )
                ),

                max_tokens=int(
                    _COST_POSITION_TOKEN_CAPS.get(
                        spec.name,
                        spec.max_tokens,
                    )
                ),

                effort=(
                    "low"
                    if spec.name
                    in _COST_POSITION_LOW_EFFORT
                    else spec.effort
                ),

                timeframes=tuple(
                    spec.timeframes
                ),

                fact_timeframes=tuple(
                    spec.fact_timeframes
                ),

                dependencies=tuple(
                    spec.dependencies
                ),
            )
        )

    return tuple(tuned)


POSITION_REVIEW_STAGE_DEFS = (
    _cost_tune_position_specs(
        POSITION_REVIEW_STAGE_DEFS
    )
)


# ----------------------------------------------------------------------
# Compact raw bars specifically for the paid position-review stages.
# ----------------------------------------------------------------------

_COST_PHASE2B_PREVIOUS_STAGE_CONTEXT = (
    _stage_context
)


def _stage_context(
    *,
    family: str,
    spec: StageDef,
    payload: dict,
    previous_reference: dict | None,
    pipeline: dict,
    market_map: dict | None = None,
) -> dict:

    context = (
        _COST_PHASE2B_PREVIOUS_STAGE_CONTEXT(
            family=family,
            spec=spec,
            payload=payload,
            previous_reference=previous_reference,
            pipeline=pipeline,
            market_map=market_map,
        )
    )


    limit = {
        "PR_HTF": 80,
        "PR_LTF": 64,
    }.get(
        spec.name
    )


    if (
        limit
        and "raw_market" in context
    ):

        context[
            "raw_market"
        ] = _cost_trim_closed_bars(
            context[
                "raw_market"
            ],
            limit,
        )


    # Reference visualization is not needed for paid position reasoning.
    if isinstance(
        context.get(
            "_reference_analysis"
        ),
        dict,
    ):

        context[
            "_reference_analysis"
        ] = (
            _cost_compact_market_map_for_trade(
                context[
                    "_reference_analysis"
                ]
            )
        )


    if isinstance(
        context.get(
            "_previous_monitor_result"
        ),
        dict,
    ):

        previous_monitor = copy.deepcopy(
            context[
                "_previous_monitor_result"
            ]
        )

        previous_monitor[
            "visualization"
        ] = {}

        context[
            "_previous_monitor_result"
        ] = previous_monitor


    return context

# ============================================================================
# V8.5.3 FULL RECOVERY HOTFIX  BILINGUAL FORMAT COMPAT
# ============================================================================

_COST_PREVIOUS_BILINGUAL_WIRE_VALIDATOR = (
    _is_bilingual_wire_text
)


def _is_bilingual_wire_text(
    value,
) -> bool:
    """
    Accept the two wire forms already understood by
    claude_staged_client._bilingual_parts():

        EN: text\nRU: text
        EN: text RU: text

    Also tolerate a literal escaped \\nRU: marker.

    Content still must contain two non-empty, different parts.
    """

    if not isinstance(
        value,
        str,
    ):
        return False


    text = value.strip()

    if not text.startswith(
        "EN:"
    ):
        return False


    marker = None

    for candidate in (
        "\nRU:",
        " RU:",
        "\\nRU:",
    ):

        if candidate in text:

            marker = candidate
            break


    if marker is None:
        return False


    english, russian = (
        text[3:].split(
            marker,
            1,
        )
    )


    english = english.strip()
    russian = russian.strip()


    if (
        not english
        or not russian
    ):
        return False


    if (
        english.casefold()
        == russian.casefold()
    ):
        return False


    return True

# ============================================================================
# V8.5.3 LOCAL VISUALIZATION ENRICHMENT
# ============================================================================

_COST_VIS_ORIGINAL_RUN_MARKET_MAP_PIPELINE = (
    run_market_map_pipeline
)


def run_market_map_pipeline(
    payload: dict,
    *,
    previous_reference: dict | None = None,
    snapshot: dict | None = None,
    cycle_type: str | None = None,
    archive_path=None,
) -> dict:
    """
    Original analytical MARKET_MAP +
    deterministic $0 chart enrichment.

    No additional Claude request is made here.
    """

    result = (
        _COST_VIS_ORIGINAL_RUN_MARKET_MAP_PIPELINE(
            payload,
            previous_reference=previous_reference,
            snapshot=snapshot,
            cycle_type=cycle_type,
            archive_path=archive_path,
        )
    )


    if not (
        isinstance(result, dict)
        and result.get("ok")
        and isinstance(
            result.get("result"),
            dict,
        )
    ):

        return result


    from local_visual_builder import (
        enrich_market_visualization
    )


    enriched = copy.deepcopy(
        result
    )


    report = (
        enrich_market_visualization(
            enriched["result"],
            payload,
        )
    )


    enriched[
        "local_visual_enrichment"
    ] = report


    return enriched

# ============================================================================
# V8.5.3 FVG CONTRACT + WINDOWS STATE WRITE HARDENING
# ============================================================================

import threading as _v853_threading


# ----------------------------------------------------------------------
# A. Robust local state write.
#
# A brief Windows file lock must never be interpreted as a failed Claude
# analytical response and trigger another paid request.
# ----------------------------------------------------------------------

_V853_STATE_WRITE_LOCK = (
    _v853_threading.RLock()
)


def _atomic_write(
    path: Path,
    value: dict,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp = path.with_name(
        path.name
        + "."
        + str(os.getpid())
        + "."
        + str(uuid.uuid4())
        + ".tmp"
    )

    with _V853_STATE_WRITE_LOCK:

        try:

            with open(
                tmp,
                "w",
                encoding="utf-8",
            ) as fh:

                json.dump(
                    value,
                    fh,
                    ensure_ascii=False,
                    indent=2,
                )

                fh.flush()

                os.fsync(
                    fh.fileno()
                )


            delays = (
                0.0,
                0.05,
                0.10,
                0.20,
                0.40,
                0.80,
            )

            last_error = None


            for delay in delays:

                if delay:
                    time.sleep(
                        delay
                    )

                try:

                    os.replace(
                        tmp,
                        path,
                    )

                    return

                except OSError as error:

                    winerror = getattr(
                        error,
                        "winerror",
                        None,
                    )

                    retryable = (
                        isinstance(
                            error,
                            PermissionError,
                        )
                        or winerror
                        in {
                            5,
                            32,
                            33,
                        }
                    )

                    if not retryable:
                        raise

                    last_error = error


            if last_error is not None:
                raise last_error

            raise RuntimeError(
                "Atomic state write failed."
            )

        finally:

            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass


# ----------------------------------------------------------------------
# B. Authoritative active FVG IDs.
# ----------------------------------------------------------------------

def _v853_active_fvg_ids(
    payload: dict,
) -> set[str]:

    facts = (
        payload.get(
            "deterministic_market_facts"
        )
        or {}
    )

    imbalances = (
        facts.get(
            "imbalances"
        )
        or {}
    )

    result = set()


    if not isinstance(
        imbalances,
        dict,
    ):
        return result


    for items in (
        imbalances.values()
    ):

        if not isinstance(
            items,
            list,
        ):
            continue

        for item in items:

            if not isinstance(
                item,
                dict,
            ):
                continue

            if (
                item.get("active")
                is not True
            ):
                continue

            identifier = str(
                item.get("id")
                or ""
            ).strip()

            if identifier:
                result.add(
                    identifier
                )


    return result


def _v853_selected_fvg_ids(
    value,
) -> set[str]:

    return {
        item.strip()

        for item in str(
            value or ""
        ).split(",")

        if item.strip()
    }


# ----------------------------------------------------------------------
# C. Give TD_REASONING the exact allowed Python IDs without resending
# raw candle history.
# ----------------------------------------------------------------------

_V853_PREVIOUS_STAGE_CONTEXT = (
    _stage_context
)


def _stage_context(
    *,
    family: str,
    spec: StageDef,
    payload: dict,
    previous_reference: dict | None,
    pipeline: dict,
    market_map: dict | None = None,
) -> dict:

    context = (
        _V853_PREVIOUS_STAGE_CONTEXT(
            family=family,
            spec=spec,
            payload=payload,
            previous_reference=previous_reference,
            pipeline=pipeline,
            market_map=market_map,
        )
    )


    if (
        str(family).startswith(
            "TRADE_DECISION"
        )
        and spec.name
        == "TD_REASONING"
    ):

        context[
            "allowed_active_fvg_ids"
        ] = sorted(
            _v853_active_fvg_ids(
                payload
            )
        )

        context[
            "fvg_id_contract"
        ] = (
            "recommendation.fvg_ids may contain ONLY exact values "
            "from allowed_active_fvg_ids. Never construct an ID "
            "from timeframe or price boundaries."
        )


    return context


# ----------------------------------------------------------------------
# D. Reconcile TD_REASONING against deterministic Python IDs BEFORE
# final business validation.
#
# stay_out:
#   invalid FVG metadata cannot promote an order, therefore discard it
#   locally at $0 and retain the conservative no-trade decision.
#
# enter_long / enter_short:
#   FVG may be material to the setup. Fail closed and request only the
#   invalid FVG fields, never the entire market map.
# ----------------------------------------------------------------------

def _v853_reconcile_reasoning_values(
    *,
    pipeline: dict,
    payload: dict,
) -> dict:

    stage = (
        (
            pipeline.get(
                "stages"
            )
            or {}
        ).get(
            "TD_REASONING"
        )
        or {}
    )

    values = (
        stage.get(
            "values"
        )
        or {}
    )


    selected = (
        _v853_selected_fvg_ids(
            values.get(
                "fvg_ids"
            )
        )
    )

    allowed = (
        _v853_active_fvg_ids(
            payload
        )
    )

    unknown = (
        selected
        - allowed
    )


    if not unknown:

        return {
            "changed": False,
            "mode": "valid",
            "unknown": [],
        }


    action = str(
        _stage_values(
            pipeline,
            "TD_DECISION",
        ).get(
            "action"
        )
        or ""
    )


    if action == "stay_out":

        values[
            "fvg_role"
        ] = "neutral"

        values[
            "fvg_ids"
        ] = ""

        values[
            "fvg_basis"
        ] = (
            "EN: Non-authoritative FVG identifiers were discarded "
            "by deterministic Python validation; FVG is not used "
            "to justify an entry.\n"
            "RU: Неавторитетные идентификаторы FVG отброшены "
            "детерминированной проверкой Python; FVG не используется "
            "как основание для входа."
        )

        mode = (
            "stay_out_local_canonicalization"
        )

    else:

        values.pop(
            "fvg_ids",
            None,
        )

        values.pop(
            "fvg_basis",
            None,
        )

        if not allowed:

            values.pop(
                "fvg_role",
                None,
            )

        mode = (
            "entry_requires_exact_fvg_retry"
        )


    stage[
        "values"
    ] = values


    return {
        "changed": True,
        "mode": mode,
        "unknown": sorted(
            unknown
        ),
        "allowed": sorted(
            allowed
        ),
    }


_V853_PREVIOUS_RUN_STAGE = (
    _run_stage
)


def _run_stage(
    *,
    state: dict,
    pipeline: dict,
    family: str,
    spec: StageDef,
    all_specs: tuple[StageDef, ...],
    stage_index: int,
    stage_total: int,
    payload: dict,
    previous_reference: dict | None,
    market_map: dict | None,
) -> dict:

    if (
        spec.name
        == "TD_REASONING"
    ):

        pre = (
            _v853_reconcile_reasoning_values(
                pipeline=pipeline,
                payload=payload,
            )
        )

        if pre.get(
            "changed"
        ):

            stage = (
                _ensure_stage(
                    pipeline,
                    spec,
                )
            )

            stage[
                "status"
            ] = (
                "PARTIAL"
                if stage.get(
                    "values"
                )
                else "PENDING"
            )

            stage[
                "missing_fields"
            ] = [
                name
                for name
                in spec.properties
                if name not in (
                    stage.get(
                        "values"
                    )
                    or {}
                )
            ]

            _invalidate_dependency_descendants(
                pipeline=pipeline,
                specs=all_specs,
                source_stage=spec.name,
                reason=(
                    "Deterministic FVG contract correction: "
                    + pre.get(
                        "mode",
                        "",
                    )
                ),
            )

            _save_state(
                state
            )


            # stay_out was corrected locally and all required fields remain.
            if (
                pre.get(
                    "mode"
                )
                == "stay_out_local_canonicalization"
            ):

                stage[
                    "status"
                ] = "VALIDATED"

                _save_state(
                    state
                )


    result = (
        _V853_PREVIOUS_RUN_STAGE(
            state=state,
            pipeline=pipeline,
            family=family,
            spec=spec,
            all_specs=all_specs,
            stage_index=stage_index,
            stage_total=stage_total,
            payload=payload,
            previous_reference=previous_reference,
            market_map=market_map,
        )
    )


    if not (
        spec.name
        == "TD_REASONING"
        and isinstance(
            result,
            dict,
        )
        and result.get(
            "ok"
        )
    ):

        return result


    post = (
        _v853_reconcile_reasoning_values(
            pipeline=pipeline,
            payload=payload,
        )
    )


    if not post.get(
        "changed"
    ):

        return result


    stage = (
        _ensure_stage(
            pipeline,
            spec,
        )
    )


    _invalidate_dependency_descendants(
        pipeline=pipeline,
        specs=all_specs,
        source_stage=spec.name,
        reason=(
            "Post-response deterministic FVG contract correction: "
            + post.get(
                "mode",
                "",
            )
        ),
    )


    if (
        post.get(
            "mode"
        )
        == "stay_out_local_canonicalization"
    ):

        stage[
            "status"
        ] = "VALIDATED"

        stage[
            "missing_fields"
        ] = []

        _save_state(
            state
        )

        return {
            **result,
            "values": copy.deepcopy(
                stage.get(
                    "values"
                )
                or {}
            ),
            "local_fvg_canonicalization": True,
        }


    # A trading entry may not silently use a fabricated FVG identifier.
    # Convert the just-finished request into a local-invalid attempt and let
    # the resilient engine request only the missing FVG fields.
    attempts = stage.setdefault(
        "attempts",
        [],
    )


    if attempts:

        last = attempts[-1]

        if (
            last.get(
                "status"
            )
            in {
                "VALIDATED_RESPONSE",
                "RECOVERED_FROM_PARTIAL",
            }
        ):

            last[
                "status"
            ] = "FAILED_INVALID"

            last[
                "failure_class"
            ] = (
                "INVALID_FVG_ID"
            )

            last[
                "error"
            ] = (
                "TD_REASONING returned FVG IDs outside "
                "allowed_active_fvg_ids."
            )


    stage[
        "status"
    ] = "PARTIAL"

    stage[
        "missing_fields"
    ] = [
        name
        for name
        in spec.properties
        if name not in (
            stage.get(
                "values"
            )
            or {}
        )
    ]


    _save_state(
        state
    )


    return _run_stage(
        state=state,
        pipeline=pipeline,
        family=family,
        spec=spec,
        all_specs=all_specs,
        stage_index=stage_index,
        stage_total=stage_total,
        payload=payload,
        previous_reference=previous_reference,
        market_map=market_map,
    )
