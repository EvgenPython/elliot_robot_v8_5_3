from observability import observe, emit, artifact
import hashlib
import json
import os
from pathlib import Path
from instruments import active_instrument

SYMBOL = active_instrument()

import anthropic

from claude_client import (
    ClaudePermanentRequestError,
    ClaudeRequestOutcomeUnknownError,
    ClaudeTransientRequestError,
    _anthropic_error_request_id,
    _anthropic_retry_after_seconds,
    _translate_api_status_error,
    load_anthropic_config,
)
from claude_stream_recovery import consume_structured_stream
from single_instance import SingleInstanceError, SingleInstanceLock


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug"
DEBUG_SCOUT_RESPONSE_PATH = DEBUG_DIR / "claude_scout_response.json"
DEBUG_SCOUT_RAW_PATH = DEBUG_DIR / "claude_scout_raw_response.json"
DEBUG_SCOUT_ATTEMPTS_DIR = DEBUG_DIR / "claude_scout_attempts"
_COMMON_LOCK_ROOT = Path(os.environ.get("PROGRAMDATA", str(BASE_DIR)))
_LOCK_SYMBOL = "".join(
    character if character.isalnum() else "_" for character in SYMBOL
)
CLAUDE_API_LOCK_PATH = (
    _COMMON_LOCK_ROOT / "WaveFrame" / f"claude_api_{_LOCK_SYMBOL}.lock"
)


# ============================================================
# SCOUT POLICY
# ============================================================

DEFAULT_SCOUT_MODEL = "claude-haiku-4-5-20251001"
SCOUT_MAX_TOKENS = 6000
SCOUT_EFFORT = "low"
SCOUT_TIMEOUT_SECONDS = 600


def get_scout_model(config: dict) -> str:
    """Scout has its own inexpensive model and never changes FULL model."""
    return str(
        config.get("scout_model") or DEFAULT_SCOUT_MODEL
    ).strip()


def get_scout_effort(config: dict, model: str) -> str | None:
    """Return an effort level only for models that support that parameter."""
    configured = config.get("scout_effort")
    if configured not in (None, ""):
        return str(configured).strip()

    # Haiku 4.5 supports Structured Outputs, but not output_config.effort.
    if "haiku-4-5" in str(model).lower():
        return None

    return SCOUT_EFFORT


def _scout_output_config(effort: str | None) -> dict:
    result = {
        "format": {
            "type": "json_schema",
            "schema": SCOUT_SCHEMA,
        },
    }
    if effort:
        result["effort"] = effort
    return result

SCOUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "instrument": {"type": "string", "enum": [SYMBOL]},
        "timestamp": {"type": "string"},
        "material_change": {"type": "boolean"},
        "possible_setup": {"type": "boolean"},
        "full_analysis_required": {"type": "boolean"},
        "confidence": {
            "type": "string",
            "enum": ["low", "medium", "high"],
        },
        "trigger_kind": {
            "type": "string",
            "enum": [
                "unchanged",
                "same_move",
                "expected_continuation",
                "approaching_level",
                "expected_level_break",
                "level_break",
                "pullback",
                "retest",
                "consolidation",
                "reversal",
                "rejection",
                "structure_change",
                "new_wave",
                "m15_subwave_change",
                "pattern_change",
                "support_resistance_event",
                "target_approach",
                "position_risk_increase",
                "momentum_change",
                "character_change",
                "scenario_invalidation",
                "entry_projection_invalidated",
                "reference_conflict",
                "possible_setup",
                "other",
            ],
        },
        "observed_changes": {
            "type": "array",
            "items": {"type": "string"},
        },
        "reason": {"type": "string"},
    },
    "required": [
        "instrument",
        "timestamp",
        "material_change",
        "possible_setup",
        "full_analysis_required",
        "confidence",
        "trigger_kind",
        "observed_changes",
        "reason",
    ],
}

SCOUT_SYSTEM_PROMPT = """
Ты — SCOUT-компонент торговой системы XAUUSD.

Это НЕ полный торговый анализ.
Ты НЕ имеешь права рекомендовать BUY/SELL, Entry, Stop Loss или Take Profit.
Твоя единственная задача — решить, нужен ли сейчас глубокий независимый FULL
анализ рынка.

Если payload.task=POSITION_SCOUT, позиция уже открыта. В этом режиме поле
full_analysis_required означает «нужен глубокий POSITION_REVIEW», а не новый
поиск входа. Проверь, возникла ли на закрытой M15 новая дочерняя волна внутри
текущей H1-волны, изменилась ли подтверждённая фигура/канал/уровень/FVG,
появился ли риск инвалидации исходной гипотезы или цена вошла в терминальную
целевую область. Даже в этом режиме запрещены BUY/SELL, новый Entry, изменение
SL/TP или команда закрытия позиции.

На входе:
1) последний успешный FULL-анализ как reference;
2) компактный свежий raw MT5 tape после/вокруг него;
3) текущие незакрытые D1/H4/H1/M30/M15/M5 бары и Bid/Ask/spread.
4) deterministic_reference_facts — уже проверенные Python факты закрытых
   свечей. Их числа неизменяемы: не утверждай обратное тому, что показывает
   сравнение close с confirmation/invalidation level.

ДЕТЕРМИНИРОВАННЫЕ ФАКТЫ ИМЕЮТ ПРИОРИТЕТ:
- reference_level_statuses отдельно сообщает relation/cross_event/touch для
  каждого уровня. Не объединяй уровни через `/` и не называй всю зону
  непройденной, если её нижняя граница уже закрыта выше;
- h1_tick_volume содержит готовое отношение к медиане. Запрещено придумывать
  собственное "типичное" значение или сравнивать незакрытый бар с закрытым;
- imbalances содержит геометрически найденные FVG. Это контекст, а не готовый
  торговый сигнал. Активны только untouched/touched_before_midpoint. После
  достижения исходной середины 50% зона снимается с активной карты; её реакция
  остаётся лишь историческим фактом и не должна изображаться как действующий
  имбаланс.

Относись к предыдущему FULL только как к reference, а не как к истине.
Сравни его с новыми raw данными.

FULL ОБЯЗАТЕЛЕН, если после предыдущего FULL появилось новое смысловое
событие, которое требует перестроить карту или может дать торгуемый setup:
- начался откат/коррекция против описанного импульса;
- сформировался retest пробитого уровня;
- появилась база/консолидация после импульса;
- начался разворот или ложный пробой;
- заметно изменился характер движения: импульс стал коррекцией, сжатием или импульсом
  в противоположную сторону;
- цена пошла против сценария reference или объективно его инвалидировала;
- возникло конкретное противоречие между reference и свежими raw данными;
- появился новый потенциально торгуемый setup, для которого FULL сможет определить Entry/SL/TP.

Для POSITION_SCOUT глубокий POSITION_REVIEW обязателен при trigger_kind:
m15_subwave_change, pattern_change, support_resistance_event,
target_approach, position_risk_increase, scenario_invalidation,
structure_change, character_change или reference_conflict. Простое продолжение
уже размеченной M15-подволны без нового pivot/event не требует глубокого вызова.

FULL НЕ НУЖЕН и должен быть full_analysis_required=false, если идёт то же самое уже описанное
движение, в том числе:
- импульс продолжается в прежнем направлении;
- обновился максимум/минимум внутри той же волны;
- пробит ещё один уровень по уже описанному ожидаемому пути;
- цена лишь приблизилась к уровню без реакции, retest, базы или нового setup;
- волатильность остаётся высокой, но характер и направление движения не изменились;
- предыдущий FULL уже рекомендовал stay_out из-за растянутого движения, а цена просто продолжила
  движение без новой структуры для входа.

Для такого продолжения выбирай trigger_kind=expected_continuation, same_move или expected_level_break.
Само по себе low/medium confidence не является причиной FULL. Неопределённость должна быть описана
как конкретное reference_conflict, а не как общее сомнение. Никакого cooldown или лимита FULL по времени/количеству нет.
Важен смысл события: настоящий новый setup нельзя пропускать, но продолжение уже описанного шокового
движения нельзя выдавать за новое событие.
Отмена только младшего conditional entry projection НЕ равна отмене всей
рыночной карты и сама по себе НЕ требует FULL. Для неё используй
trigger_kind=entry_projection_invalidated, possible_setup=false и
full_analysis_required=false, если нет отдельного нового структурного события.
Чётко различай: (a) отмену точки входа, (b) отмену сценария и (c) конфликт
всей reference-карты.
Поля observed_changes и reason должны быть компактными и двуязычными внутри
одной строки в формате EN: <English>\nRU: <Русский>. Не используй в русской
части слова stay_out, reference, structure_state, relationship, raw tape,
swing high/low или буквальные машинные enum: переводи их естественно. Это один вывод Scout,
а не два независимых решения. Ответ должен строго соответствовать JSON schema.
""".strip()
SCOUT_SYSTEM_PROMPT = SCOUT_SYSTEM_PROMPT.replace("XAUUSD", SYMBOL)


def _compact_json(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    )


SCOUT_BAR_COLUMNS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "tick_volume",
    "spread_points",
    "real_volume",
]


def _bar_table(bars) -> dict:
    if not isinstance(bars, list):
        bars = []
    columns = list(SCOUT_BAR_COLUMNS)
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        for name in bar:
            if name not in columns:
                columns.append(str(name))
    return {
        "columns": columns,
        "rows": [
            [bar.get(name) for name in columns]
            for bar in bars
            if isinstance(bar, dict)
        ],
    }


def build_scout_transport_payload(payload: dict) -> dict:
    """Columnar wire copy; the archived/web Scout payload is unchanged."""
    result = dict(payload)
    current_market = payload.get("current_market")
    if not isinstance(current_market, dict):
        return result

    transport_market = dict(current_market)
    timeframes = current_market.get("raw_timeframes")
    transport_timeframes = {}
    if isinstance(timeframes, dict):
        for timeframe_name, source in timeframes.items():
            if not isinstance(source, dict):
                continue
            encoded = dict(source)
            encoded["closed_bars"] = _bar_table(source.get("closed_bars"))
            transport_timeframes[str(timeframe_name)] = encoded

    transport_market["raw_timeframes"] = transport_timeframes
    result["current_market"] = transport_market
    result["raw_bar_transport_format"] = {
        "kind": "columnar_rows_v1",
        "meaning": (
            "Each row follows columns exactly; no raw bars or values removed."
        ),
    }
    return result


def _extract_text(response) -> str:
    parts = []

    for block in response.content:
        if getattr(block, "type", None) == "text":
            text = getattr(block, "text", "")
            if text:
                parts.append(text)

    return "".join(parts).strip()


def _usage_dict(response) -> dict:
    usage = getattr(response, "usage", None)

    return {
        "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
        "cache_creation_input_tokens": int(
            getattr(usage, "cache_creation_input_tokens", 0) or 0
        ),
        "cache_read_input_tokens": int(
            getattr(usage, "cache_read_input_tokens", 0) or 0
        ),
    }


def _save_raw(response, request_id=None):
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    raw = {
        "id": getattr(response, "id", None),
        "request_id": request_id,
        "model": getattr(response, "model", None),
        "stop_reason": getattr(response, "stop_reason", None),
        "usage": _usage_dict(response),
    }

    with open(DEBUG_SCOUT_RAW_PATH, "w", encoding="utf-8") as file:
        json.dump(raw, file, ensure_ascii=False, indent=2)

    response_identifier = raw.get("id") or raw.get("request_id")
    if response_identifier:
        safe_identifier = "".join(
            character
            for character in str(response_identifier)
            if character.isalnum() or character in {"-", "_"}
        )
        if safe_identifier:
            DEBUG_SCOUT_ATTEMPTS_DIR.mkdir(parents=True, exist_ok=True)
            attempt_path = DEBUG_SCOUT_ATTEMPTS_DIR / f"{safe_identifier}.json"
            with open(attempt_path, "w", encoding="utf-8") as file:
                json.dump(raw, file, ensure_ascii=False, indent=2)


def _normalize_result(result: dict, payload: dict) -> dict:
    # FULL решается типом события, а не самим фактом нового экстремума,
    # пробоя очередного уровня или low/medium confidence. Ошибки transport/JSON
    # по-прежнему обрабатываются отдельной fail-open логикой.
    trigger_kind = str(result.get("trigger_kind") or "other")
    escalation_triggers = {
        "level_break",
        "pullback",
        "retest",
        "consolidation",
        "reversal",
        "rejection",
        "structure_change",
        "new_wave",
        "m15_subwave_change",
        "pattern_change",
        "support_resistance_event",
        "target_approach",
        "position_risk_increase",
        "momentum_change",
        "character_change",
        "scenario_invalidation",
        "reference_conflict",
        "possible_setup",
    }
    no_full_triggers = {
        "unchanged",
        "same_move",
        "expected_continuation",
        "approaching_level",
        "expected_level_break",
        "entry_projection_invalidated",
    }

    if result.get("possible_setup"):
        result["full_analysis_required"] = True
    elif trigger_kind in escalation_triggers:
        result["full_analysis_required"] = True
    elif trigger_kind in no_full_triggers:
        result["full_analysis_required"] = False
    else:
        # Неизвестный смысловой trigger нельзя молча пропускать.
        result["full_analysis_required"] = True

    result["instrument"] = SYMBOL
    result["timestamp"] = str(payload.get("timestamp"))

    entry_fact = (
        payload.get("deterministic_reference_facts", {})
        .get("entry_projection", {})
    )
    if isinstance(entry_fact, dict) and entry_fact.get("status") == "invalidated":
        event = entry_fact.get("terminal_event") or {}
        bar = event.get("bar") or entry_fact.get("last_checked_bar") or {}
        level = event.get("level")
        close = bar.get("close")
        timeframe = (entry_fact.get("projection") or {}).get("timeframe") or "M5/M15"
        fact_text = (
            f"EN: Deterministic {timeframe} closed-bar check invalidated only the "
            f"conditional entry projection: close={close}, invalidation={level}. "
            "The market map remains the reference unless a separate structural event exists.\n"
            f"RU: Детерминированная проверка закрытой свечи {timeframe} отменила "
            f"только условный план входа: close={close}, инвалидация={level}. "
            "Рыночная карта остаётся опорной, пока нет отдельного структурного события."
        )
        changes = result.get("observed_changes")
        if not isinstance(changes, list):
            changes = []
        if fact_text not in changes:
            changes.insert(0, fact_text)
        result["observed_changes"] = changes
        if not result.get("full_analysis_required"):
            result["material_change"] = False
            result["possible_setup"] = False
            result["trigger_kind"] = "entry_projection_invalidated"
            result["reason"] = fact_text

    return result


@observe("api", artifacts=True)
def _analyze_scout_unlocked(
    payload: dict,
    on_preflight=None,
    on_response=None,
) -> dict:
    config = load_anthropic_config()
    model = get_scout_model(config)
    effort = get_scout_effort(config, model)
    output_config = _scout_output_config(effort)
    client = anthropic.Anthropic(
        api_key=str(config["api_key"]).strip(),
        timeout=SCOUT_TIMEOUT_SECONDS,
        max_retries=0,
    )

    transport_payload = build_scout_transport_payload(payload)
    transport_payload_bytes = len(
        _compact_json(transport_payload).encode("utf-8")
    )
    transport_payload_sha256 = hashlib.sha256(
        _compact_json(transport_payload).encode("utf-8")
    ).hexdigest()

    task = str(payload.get("task") or "SCOUT_ONLY").upper()
    user_content = (
        f"{task} MARKET UPDATE. Determine only whether the corresponding "
        "deep analysis is required.\n\n"
        "<scout_payload>\n"
        f"{_compact_json(transport_payload)}\n"
        "</scout_payload>"
    )

    print()
    print("=" * 80)
    print("ANTHROPIC API — SCOUT")
    print("=" * 80)
    print(f"Model:         {model}")
    print(f"Max tokens:    {SCOUT_MAX_TOKENS}")
    print(f"Effort:        {effort or 'not sent (model unsupported)'}")
    print("Prompt cache:  OFF")
    print("Authority:     escalation only; NO trade/position command")
    print(f"Timeout:       {SCOUT_TIMEOUT_SECONDS} sec")
    print("SDK retries:   OFF (business retries are journaled by main.py)")
    print(f"Wire payload:  {transport_payload_bytes / 1024:.1f} KB")

    try:
        token_result = client.messages.count_tokens(
            model=model,
            system=SCOUT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
            output_config=output_config,
        )
    except anthropic.AuthenticationError as error:
        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API в SCOUT token count.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error
    except anthropic.PermissionDeniedError as error:
        raise ClaudePermanentRequestError(
            "Anthropic API запретил SCOUT token count.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error
    except (anthropic.APITimeoutError, anthropic.APIConnectionError) as error:
        raise ClaudeTransientRequestError(
            "SCOUT token count временно недоступен; Messages ещё не отправлен.",
            request_id=_anthropic_error_request_id(error),
        ) from error
    except anthropic.APIStatusError as error:
        raise _translate_api_status_error(error, "SCOUT token counting") from error

    diagnostics = {
        "model": model,
        "input_tokens": int(token_result.input_tokens),
        "transport_payload_bytes": int(transport_payload_bytes),
        "payload_sha256": transport_payload_sha256,
        "request_id": None,
        "response_id": None,
        "stop_reason": None,
        "response_received": False,
        "usage": None,
    }
    if callable(on_preflight):
        on_preflight(dict(diagnostics))

    print(f"Input tokens:  {int(token_result.input_tokens):,}")
    transport_path = artifact("scout_transport", {"model": model, "system": SCOUT_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_content}], "output_config": output_config,
        "max_tokens": SCOUT_MAX_TOKENS})
    emit("api", "transport_prepared", data={"stage": task, "model": model, "transport_artifact": transport_path})
    print("[INFO] Отправляем дешёвый Scout-запрос Claude...")

    request_id = None
    recovered_result = None

    def record_stream_progress(values: dict):
        diagnostics.update(values)
        if callable(on_response):
            try:
                on_response(dict(diagnostics))
            except Exception as callback_error:
                print(
                    "[API JOURNAL WARNING] SCOUT stream telemetry failed: "
                    f"{type(callback_error).__name__}: {callback_error}"
                )

    try:
        with client.messages.stream(
            model=model,
            max_tokens=SCOUT_MAX_TOKENS,
            system=SCOUT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
            output_config=output_config,
        ) as stream:
            stream_result = consume_structured_stream(
                stream,
                stage="SCOUT",
                schema=SCOUT_SCHEMA,
                payload_sha256=transport_payload_sha256,
                on_progress=record_stream_progress,
            )
            diagnostics.update(stream_result.get("diagnostics") or {})
            request_id = diagnostics.get("request_id")
            recovered_result = stream_result.get("recovered_result")
            response = stream_result.get("response")
            if recovered_result is None and stream_result.get("error") is not None:
                raise ClaudeRequestOutcomeUnknownError(
                    "SCOUT SSE stream lost and complete JSON was not "
                    "recoverable. Outcome unknown.",
                    request_id=request_id,
                    diagnostics=diagnostics,
                ) from stream_result["error"]

    except (
        anthropic.APITimeoutError,
        anthropic.APIConnectionError,
    ) as error:
        raise ClaudeRequestOutcomeUnknownError(
            "SCOUT Anthropic request interrupted after send; outcome unknown. "
            "Controlled retry is allowed by project policy.",
            request_id=_anthropic_error_request_id(error),
        ) from error

    except anthropic.AuthenticationError as error:
        raise ClaudePermanentRequestError(
            "Ошибка авторизации Anthropic API в SCOUT.",
            request_id=_anthropic_error_request_id(error),
            status_code=401,
        ) from error

    except anthropic.PermissionDeniedError as error:
        raise ClaudePermanentRequestError(
            "Anthropic API запретил SCOUT-запрос.",
            request_id=_anthropic_error_request_id(error),
            status_code=403,
        ) from error

    except anthropic.RateLimitError as error:
        raise ClaudeTransientRequestError(
            "SCOUT получил Anthropic rate limit; controlled retry разрешён.",
            request_id=_anthropic_error_request_id(error),
            status_code=429,
            retry_after_seconds=_anthropic_retry_after_seconds(error),
        ) from error

    except anthropic.APIStatusError as error:
        raise _translate_api_status_error(error, "SCOUT streaming") from error

    except ClaudeRequestOutcomeUnknownError:
        raise

    except Exception as error:
        raise ClaudeRequestOutcomeUnknownError(
            "Непредвиденная ошибка внутри SCOUT SSE stream; controlled "
            "retry разрешён.",
            request_id=_anthropic_error_request_id(error),
        ) from error

    if recovered_result is None:
        _save_raw(response, request_id=request_id)
        stop_reason = getattr(response, "stop_reason", None)
        usage = _usage_dict(response)
        text = _extract_text(response)
    else:
        stop_reason = "recovered_complete_json_without_message_stop"
        usage = {}
        text = _compact_json(recovered_result)
        diagnostics.update(
            {
                "delivery_recovered": True,
                "billing_status": "UNKNOWN_MAY_BE_BILLED",
            }
        )

    diagnostics.update(
        {
            "request_id": (
                str(request_id) if request_id not in (None, "") else None
            ),
            "response_id": getattr(response, "id", None),
            "stop_reason": stop_reason,
            "response_received": True,
            "usage": dict(usage) if usage else None,
        }
    )
    if callable(on_response):
        try:
            on_response(dict(diagnostics))
        except Exception as callback_error:
            print(
                "[API JOURNAL WARNING] SCOUT response telemetry failed: "
                f"{type(callback_error).__name__}: {callback_error}"
            )

    print(f"Request ID:    {request_id}")
    print(f"Stop reason:   {stop_reason}")
    print(f"Input:         {int(usage.get('input_tokens', 0)):,}")
    print(f"Output:        {int(usage.get('output_tokens', 0)):,}")

    # Известный завершённый Scout, который не смог дать JSON,
    # безопаснее эскалировать в FULL, а не повторять Scout.
    if stop_reason == "max_tokens":
        return {
            "instrument": SYMBOL,
            "timestamp": str(payload.get("timestamp")),
            "material_change": True,
            "possible_setup": True,
            "full_analysis_required": True,
            "confidence": "low",
            "trigger_kind": "uncertainty",
            "observed_changes": [],
            "reason": (
                "Scout достиг max_tokens. По fail-open policy требуется FULL."
            ),
            "scout_transport_fallback": "MAX_TOKENS_ESCALATE",
            "usage": usage,
        }

    if stop_reason not in (
        "end_turn",
        "recovered_complete_json_without_message_stop",
        None,
    ):
        return {
            "instrument": SYMBOL,
            "timestamp": str(payload.get("timestamp")),
            "material_change": True,
            "possible_setup": True,
            "full_analysis_required": True,
            "confidence": "low",
            "trigger_kind": "uncertainty",
            "observed_changes": [],
            "reason": (
                f"Scout stop_reason={stop_reason}. По fail-open policy требуется FULL."
            ),
            "scout_transport_fallback": "STOP_REASON_ESCALATE",
            "usage": usage,
        }

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        result = {
            "instrument": SYMBOL,
            "timestamp": str(payload.get("timestamp")),
            "material_change": True,
            "possible_setup": True,
            "full_analysis_required": True,
            "confidence": "low",
            "trigger_kind": "uncertainty",
            "observed_changes": [],
            "reason": (
                "Scout вернул невалидный JSON. По fail-open policy требуется FULL."
            ),
            "scout_transport_fallback": "INVALID_JSON_ESCALATE",
        }

    result = _normalize_result(result, payload)
    result["usage"] = usage
    result["request_id"] = request_id
    result["delivery_recovered"] = recovered_result is not None

    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    with open(DEBUG_SCOUT_RESPONSE_PATH, "w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=2)

    print()
    print("SCOUT DECISION")
    print("-" * 80)
    print(f"Material change: {result.get('material_change')}")
    print(f"Possible setup:  {result.get('possible_setup')}")
    print(f"FULL required:   {result.get('full_analysis_required')}")
    print(f"Confidence:      {result.get('confidence')}")
    print(f"Trigger:         {result.get('trigger_kind')}")
    reason = str(result.get("reason") or "")
    if os.getenv("ROBOT_CONSOLE_DETAIL", "compact").strip().lower() in {
        "full", "detailed", "debug", "1", "true", "yes"
    }:
        print(f"Reason:          {reason}")
    else:
        russian = reason.split("RU:", 1)[-1].strip() if "RU:" in reason else reason
        russian = " ".join(russian.split())
        if len(russian) > 420:
            russian = russian[:417].rstrip() + "..."
        print(f"Причина кратко: {russian}")
        print("[DETAIL] Полное объяснение сохранено в analysis_archive и debug.")
    print("=" * 80)

    return result


def analyze_scout(
    payload: dict,
    on_preflight=None,
    on_response=None,
) -> dict:
    """Serialize Scout and obey/open the V8.5.3 global AI breaker."""

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
            "Другой процесс уже выполняет Claude API stage; Scout не "
            "отправлен и будет повторён позже."
        ) from error
    try:
        try:
            return _analyze_scout_unlocked(
                payload,
                on_preflight=on_preflight,
                on_response=on_response,
            )
        except ClaudePermanentRequestError as error:
            register_permanent_ai_failure(
                str(payload.get("task") or "SCOUT"), error
            )
            raise
    finally:
        lock.release()
