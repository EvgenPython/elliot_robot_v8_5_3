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
        _string_properties((
            "regime_primary_regime", "regime_direction", "regime_current_phase",
            "regime_phase_status", "regime_maturity", "regime_location", "regime_summary",
            "relationship", "relationship_summary", "htf_d1_trend", "htf_d1_wave_context",
            "htf_h4_trend", "htf_h4_wave_context", "htf_alignment", "htf_summary",
        )),
        """Синтезируй market regime, D1/H4/H1 relationship и higher-timeframe
context только из validated timeframe scans. Не требуй повторной передачи raw.""",
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
triangle/W-X-Y/alternation. Не подгоняй count под сделку.""",
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
TD_CONTEXT/decision. Raw execution candles повторно не передаются.""",
        8000, dependencies=("TD_CONTEXT", "TD_DECISION"),
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
на decision. Не создавай декоративные объекты.""",
        8000, dependencies=("TD_CONTEXT", "TD_DECISION", "TD_REASONING"),
    ),
    StageDef(
        "TD_VIS_GEOMETRY",
        {
            "trendlines": _array_named(TRENDLINE_COLUMNS),
            "channels": _array_named(CHANNEL_COLUMNS),
            "pattern_shapes": _array_named(PATTERN_SHAPE_COLUMNS),
        },
        """Верни только execution geometry, уже подтверждённую TD_CONTEXT.
Пустые массивы допустимы.""",
        6500, dependencies=("TD_CONTEXT", "TD_DECISION"),
    ),
)

TRADE_STAGE_DEFS = (
    # Fresh H1/M30/M15/M5 raw is paid once here. Later stages use the durable
    # execution scan plus a compact market-map summary.
    StageDef(
        "TD_CONTEXT",
        _string_properties((
            "h1_execution_context", "microstructure_and_patterns",
            "multi_timeframe_relationship", "entry_candidates", "stop_candidates",
            "target_candidates", "fvg_context",
        )),
        """Полностью проанализируй свежие H1/M30/M15/M5 один раз. H1 владеет
идеей, M30 даёт промежуточную возможность, M15 подтверждает вложенную структуру,
M5 уточняет trigger. Сохрани entry/stop/target candidates и FVG context.""",
        16000, timeframes=("H1", "M30", "M15", "M5"), fact_timeframes=("H1",),
    ),
    StageDef(
        "TD_DECISION",
        _string_properties((
            "action", "setup_type", "trade_horizon", "setup_quality", "entry_quality",
            "order_type", "confidence", "entry_price", "stop_loss", "take_profit",
            "invalidation_level",
        )),
        """Прими core action и execution prices только из validated market map
+ TD_CONTEXT. Если edge недостаточен — stay_out. SL структурный, TP реалистичный;
price wire fields — decimal strings/пустая строка по старому контракту.""",
        9000, dependencies=("TD_CONTEXT",),
    ),
    StageDef(
        "TD_REASONING",
        _string_properties((
            "why_now", "structural_stop_basis", "target_basis", "reasoning",
            "invalidation_reason", "fvg_role", "fvg_ids", "fvg_basis",
        )),
        """Дай доказательное rationale и FVG role уже выбранного decision.
FVG сам по себе не вход. Любое обнаруженное противоречие явно укажи.""",
        8500, dependencies=("TD_CONTEXT", "TD_DECISION"),
    ),
    *TD_VIS_DEFS,
    StageDef(
        "TD_FINAL_META",
        _string_properties(("chart_comment", "sufficient", "issues")),
        """Короткий chart_comment и execution data quality. sufficient строго
\"true\"/\"false\"; issues одна строка.""",
        4000, dependencies=("TD_CONTEXT", "TD_DECISION", "TD_REASONING", "TD_VIS_WAVES", "TD_VIS_LEVELS_EVENTS", "TD_VIS_GEOMETRY"),
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
        target[name] = copy.deepcopy(value)
        merged.append(name)
    stage["updated_at_utc"] = _utc_now()
    missing = [name for name in spec.properties if name not in target]
    stage["status"] = "VALIDATED" if not missing else ("PARTIAL" if target else "PENDING")
    stage["missing_fields"] = missing
    return merged


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
    stage_index: int,
    stage_total: int,
    payload: dict,
    previous_reference: dict | None,
    market_map: dict | None,
) -> dict:
    stage = _ensure_stage(pipeline, spec)

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
