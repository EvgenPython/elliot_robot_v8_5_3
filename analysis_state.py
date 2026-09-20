import copy
import json
from datetime import datetime
from pathlib import Path

from risk_manager import now_fp

from trade_state import (
    extract_latest_closed_h1_time,
)


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument, symbol_state_path

SYMBOL = active_instrument()

STATE_VERSION = 1

MAX_HISTORY_ITEMS = 500


# ============================================================
# ПУТИ
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

STATE_DIR = (
    BASE_DIR
    / "state"
)

ANALYSIS_STATE_PATH = symbol_state_path("analysis_state.json")


# ============================================================
# GATE DECISIONS
# ============================================================

GATE_NEW_H1 = "NEW_H1"

GATE_ALREADY_ANALYZED = "ALREADY_ANALYZED"

GATE_NO_H1_DATA = "NO_H1_DATA"

GATE_H1_TIME_REGRESSION = "H1_TIME_REGRESSION"


# ============================================================
# ПУСТОЕ СОСТОЯНИЕ
# ============================================================

def create_empty_analysis_state() -> dict:
    """
    Создаёт пустое состояние H1 Analysis Gate.
    """

    return {

        "version": (
            STATE_VERSION
        ),

        "updated_at_fp": (
            now_fp().isoformat()
        ),

        # ----------------------------------------------------
        # ПОСЛЕДНЯЯ УСПЕШНО ОБРАБОТАННАЯ H1
        # ----------------------------------------------------

        "last_analyzed_h1": None,

        # ----------------------------------------------------
        # КОГДА БЫЛ ВЫПОЛНЕН АНАЛИЗ
        # ----------------------------------------------------

        "last_analysis_time_fp": None,

        # ----------------------------------------------------
        # КАКОЕ РЕШЕНИЕ БЫЛО ПОЛУЧЕНО
        # ----------------------------------------------------

        "last_risk_decision": None,

        "last_claude_action": None,

        "last_order_type": None,

        "last_confidence": None,

        "last_setup_type": None,

        "last_market_regime": None,

        "last_current_phase": None,

        "last_phase_status": None,

        "last_setup_quality": None,

        "last_entry_quality": None,

        "last_trade_state_action": None,

        # ----------------------------------------------------
        # СТАТУС ОБРАБОТКИ H1 / ЗАЩИТА ОТ ДВОЙНОЙ ОПЛАТЫ API
        # ----------------------------------------------------

        "last_processing_status": None,

        "last_error_type": None,

        "last_error_message": None,

        # ----------------------------------------------------
        # ЕСЛИ БЫЛ СОЗДАН TRADE PLAN
        # ----------------------------------------------------

        "last_plan_id": None,

        # ----------------------------------------------------
        # ИНСТРУМЕНТ
        # ----------------------------------------------------

        "symbol": (
            SYMBOL
        ),

        # ----------------------------------------------------
        # ИСТОРИЯ
        # ----------------------------------------------------

        "history": [],
    }


# ============================================================
# NORMALIZE STATE
# ============================================================

def normalize_analysis_state(
    state: dict,
) -> tuple[dict, bool]:
    """
    Проверяет структуру analysis_state.json
    и добавляет отсутствующие поля.
    """

    if not isinstance(
        state,
        dict,
    ):

        raise RuntimeError(
            "analysis_state.json имеет "
            "некорректную структуру."
        )

    changed = False

    defaults = (
        create_empty_analysis_state()
    )

    for key, default_value in defaults.items():

        if key not in state:

            state[
                key
            ] = copy.deepcopy(
                default_value
            )

            changed = True

    if (
        int(
            state.get(
                "version",
                0,
            )
        )
        != STATE_VERSION
    ):

        state[
            "version"
        ] = STATE_VERSION

        changed = True

    if not isinstance(
        state.get(
            "history"
        ),
        list,
    ):

        state[
            "history"
        ] = []

        changed = True

    return (
        state,
        changed,
    )


# ============================================================
# LOAD
# ============================================================

def load_analysis_state() -> dict:
    """
    Загружает H1 Analysis State.
    """

    if not ANALYSIS_STATE_PATH.exists():

        return (
            create_empty_analysis_state()
        )

    try:

        with open(
            ANALYSIS_STATE_PATH,
            "r",
            encoding="utf-8",
        ) as file:

            state = json.load(
                file
            )

    except (
        json.JSONDecodeError,
        OSError,
    ) as error:

        raise RuntimeError(
            "Не удалось прочитать Analysis State:\n"
            f"{ANALYSIS_STATE_PATH}"
        ) from error

    state, changed = (
        normalize_analysis_state(
            state
        )
    )

    if changed:

        save_analysis_state(
            state
        )

    return state


# ============================================================
# SAVE
# ============================================================

def save_analysis_state(
    state: dict,
):
    """
    Сохраняет H1 Analysis State.
    """

    STATE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    state[
        "version"
    ] = STATE_VERSION

    state[
        "updated_at_fp"
    ] = (
        now_fp().isoformat()
    )

    with open(
        ANALYSIS_STATE_PATH,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            state,
            file,
            ensure_ascii=False,
            indent=2,
        )


# ============================================================
# DATETIME
# ============================================================

def parse_iso_datetime(
    value,
) -> datetime | None:
    """
    Безопасно преобразует ISO datetime.
    """

    if value is None:
        return None

    if isinstance(
        value,
        datetime,
    ):

        return value

    try:

        return datetime.fromisoformat(
            str(
                value
            )
        )

    except (
        TypeError,
        ValueError,
    ):

        return None


# ============================================================
# CURRENT H1
# ============================================================

def get_current_closed_h1(
    snapshot: dict,
) -> str | None:
    """
    Получает время последней закрытой H1
    из Market Snapshot.
    """

    return (
        extract_latest_closed_h1_time(
            snapshot
        )
    )


# ============================================================
# ANALYSIS GATE
# ============================================================

def check_analysis_gate(
    snapshot: dict,
    symbol: str = SYMBOL,
) -> dict:
    """
    Проверяет, нужно ли запускать новый Claude analysis cycle
    для текущей закрытой H1.

    Правило STEP 10:

        одна закрытая H1
            =
        один завершённый analysis cycle

    Cycle может быть SCOUT_NO_FULL или FULL.

    Возможные решения:

        NEW_H1
        ALREADY_ANALYZED
        NO_H1_DATA
        H1_TIME_REGRESSION
    """

    state = load_analysis_state()

    current_h1 = (
        get_current_closed_h1(
            snapshot
        )
    )

    last_analyzed_h1 = (
        state.get(
            "last_analyzed_h1"
        )
    )

    current_time = (
        now_fp().isoformat()
    )

    # ========================================================
    # НЕТ H1
    # ========================================================

    if not current_h1:

        return {

            "should_analyze": False,

            "decision": (
                GATE_NO_H1_DATA
            ),

            "reason": (
                "Не удалось определить "
                "последнюю закрытую H1-свечу."
            ),

            "checked_at_fp": (
                current_time
            ),

            "symbol": (
                symbol
            ),

            "current_h1": None,

            "last_analyzed_h1": (
                last_analyzed_h1
            ),

            "last_analysis_time_fp": (
                state.get(
                    "last_analysis_time_fp"
                )
            ),

            "last_risk_decision": (
                state.get(
                    "last_risk_decision"
                )
            ),

            "last_claude_action": (
                state.get(
                    "last_claude_action"
                )
            ),

            "last_plan_id": (
                state.get(
                    "last_plan_id"
                )
            ),

            "state_path": str(
                ANALYSIS_STATE_PATH
            ),
        }

    # ========================================================
    # ПЕРВЫЙ ЗАПУСК
    # ========================================================

    if not last_analyzed_h1:

        return {

            "should_analyze": True,

            "decision": (
                GATE_NEW_H1
            ),

            "reason": (
                "Analysis State ещё не содержит "
                "обработанных H1-свечей."
            ),

            "checked_at_fp": (
                current_time
            ),

            "symbol": (
                symbol
            ),

            "current_h1": (
                current_h1
            ),

            "last_analyzed_h1": None,

            "last_analysis_time_fp": None,

            "last_risk_decision": None,

            "last_claude_action": None,

            "last_plan_id": None,

            "state_path": str(
                ANALYSIS_STATE_PATH
            ),
        }

    # ========================================================
    # ТА ЖЕ H1
    # ========================================================

    if (
        str(
            current_h1
        )
        ==
        str(
            last_analyzed_h1
        )
    ):

        processing_status = str(
            state.get(
                "last_processing_status"
            )
            or
            ""
        )

        if processing_status in {
            "API_OUTCOME_UNKNOWN_NO_RETRY",
            "API_RETRIES_EXHAUSTED",
        }:

            if processing_status == "API_RETRIES_EXHAUSTED":
                same_h1_reason = (
                    "Для этой H1 все настроенные controlled attempts "
                    "Anthropic уже исчерпаны без валидного ответа. "
                    "Невалидный результат не передаётся в торговлю."
                )
            else:
                same_h1_reason = (
                    "Эта H1 была остановлена старой no-retry версией после "
                    "неопределённого результата Anthropic."
                )

        else:

            same_h1_reason = (
                "Эта закрытая H1-свеча "
                "уже была успешно обработана analysis cycle."
            )

        return {

            "should_analyze": False,

            "decision": (
                GATE_ALREADY_ANALYZED
            ),

            "reason": (
                same_h1_reason
            ),

            "checked_at_fp": (
                current_time
            ),

            "symbol": (
                symbol
            ),

            "current_h1": (
                current_h1
            ),

            "last_analyzed_h1": (
                last_analyzed_h1
            ),

            "last_analysis_time_fp": (
                state.get(
                    "last_analysis_time_fp"
                )
            ),

            "last_risk_decision": (
                state.get(
                    "last_risk_decision"
                )
            ),

            "last_claude_action": (
                state.get(
                    "last_claude_action"
                )
            ),

            "last_plan_id": (
                state.get(
                    "last_plan_id"
                )
            ),

            "state_path": str(
                ANALYSIS_STATE_PATH
            ),
        }

    # ========================================================
    # ПРОВЕРКА ВРЕМЕНИ
    # ========================================================

    current_h1_dt = (
        parse_iso_datetime(
            current_h1
        )
    )

    last_h1_dt = (
        parse_iso_datetime(
            last_analyzed_h1
        )
    )

    if (
        current_h1_dt is not None
        and
        last_h1_dt is not None
        and
        current_h1_dt
        <
        last_h1_dt
    ):

        return {

            "should_analyze": False,

            "decision": (
                GATE_H1_TIME_REGRESSION
            ),

            "reason": (
                "Время последней закрытой H1 "
                "оказалось старше уже "
                "обработанной H1. "
                "Анализ заблокирован, "
                "так как это может означать "
                "проблему истории или времени."
            ),

            "checked_at_fp": (
                current_time
            ),

            "symbol": (
                symbol
            ),

            "current_h1": (
                current_h1
            ),

            "last_analyzed_h1": (
                last_analyzed_h1
            ),

            "last_analysis_time_fp": (
                state.get(
                    "last_analysis_time_fp"
                )
            ),

            "last_risk_decision": (
                state.get(
                    "last_risk_decision"
                )
            ),

            "last_claude_action": (
                state.get(
                    "last_claude_action"
                )
            ),

            "last_plan_id": (
                state.get(
                    "last_plan_id"
                )
            ),

            "state_path": str(
                ANALYSIS_STATE_PATH
            ),
        }

    # ========================================================
    # НОВАЯ H1
    # ========================================================

    return {

        "should_analyze": True,

        "decision": (
            GATE_NEW_H1
        ),

        "reason": (
            "Появилась новая закрытая H1-свеча."
        ),

        "checked_at_fp": (
            current_time
        ),

        "symbol": (
            symbol
        ),

        "current_h1": (
            current_h1
        ),

        "last_analyzed_h1": (
            last_analyzed_h1
        ),

        "last_analysis_time_fp": (
            state.get(
                "last_analysis_time_fp"
            )
        ),

        "last_risk_decision": (
            state.get(
                "last_risk_decision"
            )
        ),

        "last_claude_action": (
            state.get(
                "last_claude_action"
            )
        ),

        "last_plan_id": (
            state.get(
                "last_plan_id"
            )
        ),

        "state_path": str(
            ANALYSIS_STATE_PATH
        ),
    }


# ============================================================
# REGISTER COMPLETED ANALYSIS
# ============================================================

def register_completed_analysis(
    snapshot: dict,
    analysis: dict,
    risk_report: dict,
    state_result: dict,
    symbol: str = SYMBOL,
    cycle_type: str = "FULL",
    scout_result: dict | None = None,
) -> dict:
    """
    Помечает текущую закрытую H1
    как успешно обработанную.

    Эту функцию нужно вызывать ТОЛЬКО ПОСЛЕ:

        Claude успешно ответил
            ↓
        Structured Output разобран
            ↓
        Risk Manager завершён
            ↓
        Trade State успешно обновлён

    Если API Claude упал,
    Risk Manager упал
    или Trade State упал:

        эту функцию НЕ вызываем.

    Тогда следующая попытка сможет
    повторно обработать эту же H1.
    """

    current_h1 = (
        get_current_closed_h1(
            snapshot
        )
    )

    if not current_h1:

        raise RuntimeError(
            "Нельзя зарегистрировать анализ: "
            "не удалось определить "
            "закрытую H1-свечу."
        )

    state = load_analysis_state()

    recommendation = (
        analysis.get(
            "recommendation",
            {}
        )
    )

    risk_decision = str(
        risk_report.get(
            "decision",
            ""
        )
    )

    if risk_decision not in (
        "APPROVED",
        "NO_TRADE",
        "REJECTED",
    ):

        raise RuntimeError(
            "Нельзя зарегистрировать анализ: "
            "Risk Manager вернул "
            f"неожиданное решение "
            f"{risk_decision}."
        )

    active_plan = (
        state_result.get(
            "active_plan"
        )
    )

    plan_id = None

    if isinstance(
        active_plan,
        dict,
    ):

        plan_id = (
            active_plan.get(
                "plan_id"
            )
        )

    analysis_time = (
        analysis.get(
            "timestamp"
        )
        or
        now_fp().isoformat()
    )

    # ========================================================
    # HISTORY RECORD
    # ========================================================

    history_record = {

        "h1_closed_bar_time": (
            current_h1
        ),

        "analysis_time_fp": (
            analysis_time
        ),

        "registered_at_fp": (
            now_fp().isoformat()
        ),

        "processing_status": (
            "COMPLETED"
        ),

        "symbol": (
            symbol
        ),

        "analysis_cycle_type": str(cycle_type),

        "scout_result": copy.deepcopy(scout_result) if isinstance(scout_result, dict) else None,

        "risk_decision": (
            risk_decision
        ),

        "risk_approved": bool(
            risk_report.get(
                "approved",
                False,
            )
        ),

        "claude_action": (
            recommendation.get(
                "action"
            )
        ),

        "order_type": (
            recommendation.get(
                "order_type"
            )
        ),

        "confidence": (
            recommendation.get(
                "confidence"
            )
        ),

        "setup_type": (
            recommendation.get(
                "setup_type"
            )
        ),

        "market_regime": (
            analysis.get(
                "market_regime",
                {},
            ).get(
                "primary_regime"
            )
        ),

        "market_direction": (
            analysis.get(
                "market_regime",
                {},
            ).get(
                "direction"
            )
        ),

        "current_phase": (
            analysis.get(
                "market_regime",
                {},
            ).get(
                "current_phase"
            )
            or
            analysis.get(
                "market_regime",
                {},
            ).get(
                "phase"
            )
        ),

        "phase_status": (
            analysis.get(
                "market_regime",
                {},
            ).get(
                "phase_status"
            )
        ),

        "setup_quality": (
            recommendation.get(
                "setup_quality"
            )
        ),

        "entry_quality": (
            recommendation.get(
                "entry_quality"
            )
        ),

        "timeframe_relationship": (
            analysis.get(
                "timeframe_analysis",
                {},
            ).get(
                "relationship"
            )
        ),

        "trade_state_action": (
            state_result.get(
                "state_action"
            )
        ),

        "plan_id": (
            plan_id
        ),
    }

    # ========================================================
    # ЗАЩИТА ОТ ДУБЛИКАТА
    # ========================================================

    existing_record = None

    for record in state.get(
        "history",
        [],
    ):

        if (
            str(
                record.get(
                    "h1_closed_bar_time"
                )
            )
            ==
            str(
                current_h1
            )
        ):

            existing_record = (
                record
            )

            break

    if existing_record is not None:

        raise RuntimeError(
            "Попытка повторно зарегистрировать "
            "уже обработанную H1: "
            f"{current_h1}"
        )

    # ========================================================
    # UPDATE CURRENT STATE
    # ========================================================

    state[
        "last_analyzed_h1"
    ] = current_h1

    state[
        "last_analysis_time_fp"
    ] = analysis_time

    state[
        "last_risk_decision"
    ] = risk_decision

    state[
        "last_claude_action"
    ] = recommendation.get(
        "action"
    )

    state[
        "last_order_type"
    ] = recommendation.get(
        "order_type"
    )

    state[
        "last_confidence"
    ] = recommendation.get(
        "confidence"
    )

    state[
        "last_setup_type"
    ] = recommendation.get(
        "setup_type"
    )

    state[
        "last_market_regime"
    ] = analysis.get(
        "market_regime",
        {},
    ).get(
        "primary_regime"
    )

    state[
        "last_current_phase"
    ] = (
        analysis.get(
            "market_regime",
            {},
        ).get(
            "current_phase"
        )
        or
        analysis.get(
            "market_regime",
            {},
        ).get(
            "phase"
        )
    )

    state[
        "last_phase_status"
    ] = analysis.get(
        "market_regime",
        {},
    ).get(
        "phase_status"
    )

    state[
        "last_setup_quality"
    ] = recommendation.get(
        "setup_quality"
    )

    state[
        "last_entry_quality"
    ] = recommendation.get(
        "entry_quality"
    )

    state[
        "last_trade_state_action"
    ] = state_result.get(
        "state_action"
    )

    state[
        "last_processing_status"
    ] = "COMPLETED"

    state[
        "last_cycle_type"
    ] = str(cycle_type)

    state[
        "last_scout_result"
    ] = copy.deepcopy(scout_result) if isinstance(scout_result, dict) else None

    state[
        "last_error_type"
    ] = None

    state[
        "last_error_message"
    ] = None

    state[
        "last_plan_id"
    ] = plan_id

    state[
        "symbol"
    ] = symbol

    # ========================================================
    # HISTORY
    # ========================================================

    history = state.setdefault(
        "history",
        [],
    )

    history.append(
        history_record
    )

    if (
        len(
            history
        )
        > MAX_HISTORY_ITEMS
    ):

        state[
            "history"
        ] = history[
            -MAX_HISTORY_ITEMS:
        ]

    save_analysis_state(
        state
    )

    return {

        "registered": True,

        "analysis_cycle_type": str(cycle_type),

        "h1_closed_bar_time": (
            current_h1
        ),

        "analysis_time_fp": (
            analysis_time
        ),

        "risk_decision": (
            risk_decision
        ),

        "claude_action": (
            recommendation.get(
                "action"
            )
        ),

        "market_regime": (
            analysis.get(
                "market_regime",
                {},
            ).get(
                "primary_regime"
            )
        ),

        "current_phase": (
            analysis.get(
                "market_regime",
                {},
            ).get(
                "current_phase"
            )
            or
            analysis.get(
                "market_regime",
                {},
            ).get(
                "phase"
            )
        ),

        "phase_status": (
            analysis.get(
                "market_regime",
                {},
            ).get(
                "phase_status"
            )
        ),

        "setup_type": (
            recommendation.get(
                "setup_type"
            )
        ),

        "setup_quality": (
            recommendation.get(
                "setup_quality"
            )
        ),

        "entry_quality": (
            recommendation.get(
                "entry_quality"
            )
        ),

        "plan_id": (
            plan_id
        ),

        "state_path": str(
            ANALYSIS_STATE_PATH
        ),
    }


# ============================================================
# REGISTER COMPLETED SCOUT CYCLE
# ============================================================

def register_completed_scout_cycle(
    snapshot: dict,
    scout_result: dict,
    symbol: str = SYMBOL,
) -> dict:
    """
    Помечает H1 как полностью обработанную, когда Scout уверенно решил,
    что глубокий FULL для этой H1 не нужен.

    ВАЖНО:
    - торговое решение НЕ создаётся;
    - Risk Manager НЕ запускается;
    - Trade State НЕ меняется;
    - та же H1 повторно Claude не отправляется.
    """

    current_h1 = get_current_closed_h1(snapshot)

    if not current_h1:
        raise RuntimeError(
            "Нельзя зарегистрировать Scout cycle: не удалось определить H1."
        )

    if bool(scout_result.get("full_analysis_required")):
        raise RuntimeError(
            "Нельзя зарегистрировать SCOUT_NO_FULL: Scout требует FULL."
        )

    state = load_analysis_state()

    for record in state.get("history", []):
        if str(record.get("h1_closed_bar_time")) == str(current_h1):
            raise RuntimeError(
                "Попытка повторно зарегистрировать уже обработанную H1: "
                f"{current_h1}"
            )

    registered_at = now_fp().isoformat()

    history_record = {
        "h1_closed_bar_time": current_h1,
        "analysis_time_fp": registered_at,
        "registered_at_fp": registered_at,
        "processing_status": "SCOUT_NO_FULL_COMPLETED",
        "analysis_cycle_type": "SCOUT_NO_FULL",
        "symbol": symbol,
        "risk_decision": "SCOUT_NO_FULL",
        "risk_approved": False,
        "claude_action": None,
        "order_type": None,
        "confidence": scout_result.get("confidence"),
        "setup_type": None,
        "market_regime": None,
        "market_direction": None,
        "current_phase": None,
        "phase_status": None,
        "setup_quality": None,
        "entry_quality": None,
        "timeframe_relationship": None,
        "trade_state_action": "NO_TRADE_SCOUT_NO_FULL",
        "plan_id": None,
        "scout_result": copy.deepcopy(scout_result),
    }

    state["last_analyzed_h1"] = current_h1
    state["last_analysis_time_fp"] = registered_at
    state["last_risk_decision"] = "SCOUT_NO_FULL"
    state["last_claude_action"] = None
    state["last_order_type"] = None
    state["last_confidence"] = scout_result.get("confidence")
    state["last_setup_type"] = None
    state["last_market_regime"] = None
    state["last_current_phase"] = None
    state["last_phase_status"] = None
    state["last_setup_quality"] = None
    state["last_entry_quality"] = None
    state["last_trade_state_action"] = "NO_TRADE_SCOUT_NO_FULL"
    state["last_processing_status"] = "SCOUT_NO_FULL_COMPLETED"
    state["last_cycle_type"] = "SCOUT_NO_FULL"
    state["last_scout_result"] = copy.deepcopy(scout_result)
    state["last_error_type"] = None
    state["last_error_message"] = None
    state["last_plan_id"] = None
    state["symbol"] = symbol

    history = state.setdefault("history", [])
    history.append(history_record)

    if len(history) > MAX_HISTORY_ITEMS:
        state["history"] = history[-MAX_HISTORY_ITEMS:]

    save_analysis_state(state)

    return {
        "registered": True,
        "analysis_cycle_type": "SCOUT_NO_FULL",
        "h1_closed_bar_time": current_h1,
        "analysis_time_fp": registered_at,
        "risk_decision": "SCOUT_NO_FULL",
        "claude_action": None,
        "market_regime": None,
        "current_phase": None,
        "phase_status": None,
        "setup_type": None,
        "setup_quality": None,
        "entry_quality": None,
        "plan_id": None,
        "scout_full_required": False,
        "scout_confidence": scout_result.get("confidence"),
        "scout_reason": scout_result.get("reason"),
        "state_path": str(ANALYSIS_STATE_PATH),
    }


# ============================================================
# REGISTER EXHAUSTED ANTHROPIC ATTEMPTS
# ============================================================

def register_uncertain_api_attempt(
    snapshot: dict,
    error_message: str,
    symbol: str = SYMBOL,
    api_stage: str = "FULL",
) -> dict:
    """
    Помечает текущую H1 только ПОСЛЕ исчерпания controlled attempts.

    Почему это нужно:

        запрос уже мог попасть на сервер Anthropic
            ↓
        генерация могла начаться / завершиться
            ↓
        клиент не получил финальный ответ
            ↓
        controlled retries уже были выполнены по policy проекта

    После исчерпания лимита fail-closed политика:

        не используем отсутствующий/невалидный ответ
        ордер не создаём
        следующую новую H1 можно анализировать штатно
    """

    current_h1 = (
        get_current_closed_h1(
            snapshot
        )
    )

    if not current_h1:

        raise RuntimeError(
            "Нельзя зарегистрировать неопределённый API-запрос: "
            "не удалось определить закрытую H1-свечу."
        )

    state = load_analysis_state()

    registered_at = (
        now_fp().isoformat()
    )

    # Не создаём дубль history, если запись уже существует.
    for record in state.get(
        "history",
        [],
    ):

        if (
            str(
                record.get(
                    "h1_closed_bar_time"
                )
            )
            ==
            str(
                current_h1
            )
        ):

            return {
                "registered": False,
                "already_registered": True,
                "h1_closed_bar_time": current_h1,
                "processing_status": record.get(
                    "processing_status"
                ),
                "registered_at_fp": record.get(
                    "registered_at_fp"
                ),
                "error_message": record.get(
                    "error_message"
                ),
                "state_path": str(
                    ANALYSIS_STATE_PATH
                ),
            }

    history_record = {
        "h1_closed_bar_time": current_h1,
        "analysis_time_fp": None,
        "registered_at_fp": registered_at,
        "processing_status": (
            "API_RETRIES_EXHAUSTED"
        ),
        "symbol": symbol,
        "api_stage": str(api_stage),
        "analysis_cycle_type": f"{api_stage}_RETRIES_EXHAUSTED",
        "risk_decision": (
            "API_RETRIES_EXHAUSTED"
        ),
        "risk_approved": False,
        "claude_action": None,
        "order_type": None,
        "confidence": None,
        "setup_type": None,
        "market_regime": None,
        "market_direction": None,
        "current_phase": None,
        "phase_status": None,
        "setup_quality": None,
        "entry_quality": None,
        "timeframe_relationship": None,
        "trade_state_action": (
            "NO_TRADE_API_RETRIES_EXHAUSTED"
        ),
        "plan_id": None,
        "error_type": (
            "ClaudeAPIRetriesExhausted"
        ),
        "error_message": str(
            error_message
        ),
    }

    state[
        "last_analyzed_h1"
    ] = current_h1

    state[
        "last_analysis_time_fp"
    ] = registered_at

    state[
        "last_risk_decision"
    ] = "API_RETRIES_EXHAUSTED"

    state[
        "last_claude_action"
    ] = None

    state[
        "last_order_type"
    ] = None

    state[
        "last_confidence"
    ] = None

    state[
        "last_setup_type"
    ] = None

    state[
        "last_market_regime"
    ] = None

    state[
        "last_current_phase"
    ] = None

    state[
        "last_phase_status"
    ] = None

    state[
        "last_setup_quality"
    ] = None

    state[
        "last_entry_quality"
    ] = None

    state[
        "last_trade_state_action"
    ] = "NO_TRADE_API_RETRIES_EXHAUSTED"

    state[
        "last_plan_id"
    ] = None

    state[
        "last_processing_status"
    ] = "API_RETRIES_EXHAUSTED"

    state[
        "last_cycle_type"
    ] = f"{api_stage}_RETRIES_EXHAUSTED"

    state[
        "last_error_type"
    ] = "ClaudeAPIRetriesExhausted"

    state[
        "last_error_message"
    ] = str(
        error_message
    )

    state[
        "symbol"
    ] = symbol

    history = state.setdefault(
        "history",
        [],
    )

    history.append(
        history_record
    )

    if (
        len(
            history
        )
        > MAX_HISTORY_ITEMS
    ):

        state[
            "history"
        ] = history[
            -MAX_HISTORY_ITEMS:
        ]

    save_analysis_state(
        state
    )

    return {
        "registered": True,
        "api_stage": str(api_stage),
        "already_registered": False,
        "h1_closed_bar_time": current_h1,
        "processing_status": (
            "API_RETRIES_EXHAUSTED"
        ),
        "registered_at_fp": registered_at,
        "error_message": str(
            error_message
        ),
        "state_path": str(
            ANALYSIS_STATE_PATH
        ),
    }


def print_uncertain_api_registration(
    result: dict,
):
    """
    Печатает fail-closed регистрацию после исчерпания attempt budget.
    """

    print()
    print("=" * 80)
    print(
        "ANTHROPIC CONTROLLED RETRIES EXHAUSTED"
    )
    print("=" * 80)

    print(
        f"H1 closed bar:   "
        f"{result.get('h1_closed_bar_time')}"
    )

    print(
        f"Status:          "
        f"{result.get('processing_status')}"
    )

    print(
        f"Registered:      "
        f"{result.get('registered')}"
    )

    print(
        f"Registered at:   "
        f"{result.get('registered_at_fp')}"
    )

    print()

    print(
        "[FAIL CLOSED] Ордер не создаётся."
    )

    print(
        "[ATTEMPTS EXHAUSTED] Настроенный лимит повторов для этой H1 "
        "исчерпан; бесконечные платные запросы не выполняются."
    )

    print(
        "[NEXT] Следующая новая закрытая H1 "
        "может анализироваться штатно."
    )

    print()

    print(
        f"State file:      "
        f"{result.get('state_path')}"
    )

    print("=" * 80)


# ============================================================
# MANUAL RESET
# ============================================================

def reset_analysis_state():
    """
    Полностью сбрасывает H1 Analysis State.

    НИКОГДА не должен использоваться
    автоматически торговым роботом.

    Это только ручная функция
    для разработки / тестирования.
    """

    state = (
        create_empty_analysis_state()
    )

    save_analysis_state(
        state
    )


# ============================================================
# PRINT GATE
# ============================================================

def print_analysis_gate(
    gate: dict,
):
    """
    Выводит состояние H1 Analysis Gate.
    """

    print()
    print("=" * 80)
    print(
        "H1 ANALYSIS GATE"
    )
    print("=" * 80)

    print(
        f"Decision:        "
        f"{gate['decision']}"
    )

    print(
        f"Should analyze:  "
        f"{gate['should_analyze']}"
    )

    print()

    print(
        f"Current H1:      "
        f"{gate['current_h1']}"
    )

    print(
        f"Last analyzed:   "
        f"{gate['last_analyzed_h1']}"
    )

    print(
        f"Last analysis:   "
        f"{gate['last_analysis_time_fp']}"
    )

    print()

    print(
        f"Last decision:   "
        f"{gate['last_risk_decision']}"
    )

    print(
        f"Last action:     "
        f"{gate['last_claude_action']}"
    )

    print(
        f"Last Plan ID:    "
        f"{gate['last_plan_id']}"
    )

    print()

    print(
        f"Reason:          "
        f"{gate['reason']}"
    )

    print()

    print(
        f"State file:      "
        f"{gate['state_path']}"
    )

    print("=" * 80)


# ============================================================
# PRINT REGISTER RESULT
# ============================================================

def print_analysis_registration(
    result: dict,
):
    """
    Выводит результат регистрации
    завершённого анализа.
    """

    print()
    print("=" * 80)
    print(
        "H1 ANALYSIS STATE UPDATED"
    )
    print("=" * 80)

    print(
        f"Registered:      "
        f"{result['registered']}"
    )

    if result.get("analysis_cycle_type"):
        print(
            f"Cycle type:      "
            f"{result.get('analysis_cycle_type')}"
        )

    print(
        f"H1 closed bar:   "
        f"{result['h1_closed_bar_time']}"
    )

    print(
        f"Analysis time:   "
        f"{result['analysis_time_fp']}"
    )

    print(
        f"Risk decision:   "
        f"{result['risk_decision']}"
    )

    print(
        f"Claude action:   "
        f"{result['claude_action']}"
    )

    print(
        f"Market regime:   "
        f"{result.get('market_regime')}"
    )

    print(
        f"Current phase:   "
        f"{result.get('current_phase')}"
    )

    print(
        f"Phase status:    "
        f"{result.get('phase_status')}"
    )

    print(
        f"Setup type:      "
        f"{result.get('setup_type')}"
    )

    print(
        f"Setup quality:   "
        f"{result.get('setup_quality')}"
    )

    print(
        f"Entry quality:   "
        f"{result.get('entry_quality')}"
    )

    print(
        f"Plan ID:         "
        f"{result['plan_id']}"
    )

    print()

    print(
        f"State file:      "
        f"{result['state_path']}"
    )

    print("=" * 80)
