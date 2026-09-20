import copy
import hashlib
import json
import os
import uuid
from pathlib import Path

import MetaTrader5 as mt5
import pandas as pd

from risk_manager import now_fp


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument, symbol_state_path

SYMBOL = active_instrument()

STATE_VERSION = 2

MAX_HISTORY_ITEMS = 200

MAX_ACTION_HISTORY_ITEMS = 200

MAX_PROTECTION_HISTORY_ITEMS = 100


# ============================================================
# ПУТИ
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

STATE_DIR = (
    BASE_DIR
    / "state"
)

TRADE_STATE_PATH = symbol_state_path("trade_state.json")


# ============================================================
# СТАТУСЫ ТОРГОВОГО ПЛАНА
# ============================================================

PLAN_STATUS_APPROVED = "approved"

PLAN_STATUS_PENDING = "pending"

PLAN_STATUS_FILLED = "filled"

PLAN_STATUS_SUPERSEDED = "superseded"

PLAN_STATUS_CANCEL_REQUESTED = "cancel_requested"

PLAN_STATUS_CANCELLED = "cancelled"

PLAN_STATUS_INVALIDATION_TRIGGERED = (
    "invalidation_triggered"
)

PLAN_STATUS_EXPIRED = "expired"

PLAN_STATUS_CLOSED = "closed"

PLAN_STATUS_EXECUTION_FAILED = "execution_failed"


# ============================================================
# СТАТУСЫ ИСПОЛНЕНИЯ
# ============================================================

EXECUTION_NOT_SENT = "not_sent"

EXECUTION_PENDING_SENT = "pending_sent"

EXECUTION_POSITION_OPEN = "position_open"

EXECUTION_CANCEL_REQUESTED = "cancel_requested"

EXECUTION_CANCELLED = "cancelled"

EXECUTION_EXPIRED = "expired"

EXECUTION_CLOSED = "closed"

EXECUTION_SEND_INTENT = "send_intent"

EXECUTION_SEND_FAILED = "send_failed"


# ============================================================
# ACTION TYPES
# ============================================================

ACTION_CANCEL_PENDING = "cancel_pending"

ACTION_MANAGE_POSITION = "manage_position"

ACTION_REANALYZE = "reanalyse"


# ============================================================
# ACTION STATUS
# ============================================================

ACTION_STATUS_PENDING = "pending"

ACTION_STATUS_COMPLETED = "completed"

ACTION_STATUS_FAILED = "failed"


# ============================================================
# ПУСТОЕ СОСТОЯНИЕ
# ============================================================

def create_empty_state() -> dict:
    """
    Создаёт пустой Trade State.
    """

    return {

        "version": (
            STATE_VERSION
        ),

        "updated_at_fp": (
            now_fp().isoformat()
        ),

        # ----------------------------------------------------
        # ТЕКУЩИЙ ENTRY / PENDING PLAN
        # ----------------------------------------------------

        "active_plan": None,

        # ----------------------------------------------------
        # УЖЕ ОТКРЫТЫЕ ПОЗИЦИИ
        # ----------------------------------------------------

        "managed_positions": [],

        # ----------------------------------------------------
        # ДЕЙСТВИЯ, КОТОРЫЕ ЕЩЁ НУЖНО ВЫПОЛНИТЬ
        # ----------------------------------------------------

        "pending_actions": [],

        # ----------------------------------------------------
        # ИСТОРИЯ ACTIONS
        # ----------------------------------------------------

        "action_history": [],

        # ----------------------------------------------------
        # ПОСЛЕДНЕЕ РЕШЕНИЕ
        # ----------------------------------------------------

        "last_decision": None,

        # ----------------------------------------------------
        # ИСТОРИЯ ПЛАНОВ
        # ----------------------------------------------------

        "history": [],
    }


# ============================================================
# NORMALIZATION / MIGRATION
# ============================================================

def normalize_trade_state(
    state: dict,
) -> tuple[dict, bool]:
    """
    Проверяет структуру Trade State
    и выполняет миграцию старых состояний.
    """

    changed = False

    if not isinstance(
        state,
        dict,
    ):

        raise RuntimeError(
            "trade_state.json имеет "
            "некорректную структуру."
        )

    defaults = {

        "version": (
            STATE_VERSION
        ),

        "updated_at_fp": None,

        "active_plan": None,

        "managed_positions": [],

        "pending_actions": [],

        "action_history": [],

        "last_decision": None,

        "history": [],
    }

    for key, default_value in defaults.items():

        if key not in state:

            state[
                key
            ] = copy.deepcopy(
                default_value
            )

            changed = True

    # ========================================================
    # VERSION
    # ========================================================

    if (
        int(
            state.get(
                "version",
                1,
            )
            or 1
        )
        != STATE_VERSION
    ):

        state[
            "version"
        ] = STATE_VERSION

        changed = True

    # ========================================================
    # LIST FIELDS
    # ========================================================

    list_fields = (
        "managed_positions",
        "pending_actions",
        "action_history",
        "history",
    )

    for field in list_fields:

        if not isinstance(
            state.get(
                field
            ),
            list,
        ):

            state[
                field
            ] = []

            changed = True

    # ========================================================
    # MIGRATE POSITION FROM OLD ACTIVE PLAN
    # ========================================================

    active_plan = state.get(
        "active_plan"
    )

    if isinstance(
        active_plan,
        dict,
    ):

        execution_status = (
            active_plan.get(
                "execution_status"
            )
        )

        position_ticket = (
            active_plan.get(
                "position_ticket"
            )
        )

        if (
            execution_status
            == EXECUTION_POSITION_OPEN
            or
            position_ticket is not None
        ):

            plan_id = (
                active_plan.get(
                    "plan_id"
                )
            )

            already_exists = any(
                item.get(
                    "plan_id"
                )
                == plan_id
                for item
                in state[
                    "managed_positions"
                ]
                if isinstance(
                    item,
                    dict,
                )
            )

            if not already_exists:

                managed = copy.deepcopy(
                    active_plan
                )

                managed[
                    "status"
                ] = PLAN_STATUS_FILLED

                managed[
                    "execution_status"
                ] = EXECUTION_POSITION_OPEN

                managed[
                    "managed_since_fp"
                ] = (
                    managed.get(
                        "managed_since_fp"
                    )
                    or
                    now_fp().isoformat()
                )

                state[
                    "managed_positions"
                ].append(
                    managed
                )

            state[
                "active_plan"
            ] = None

            changed = True

    return (
        state,
        changed,
    )


# ============================================================
# LOAD
# ============================================================

def load_trade_state() -> dict:
    """
    Загружает Trade State.
    """

    if not TRADE_STATE_PATH.exists():

        return create_empty_state()

    try:

        with open(
            TRADE_STATE_PATH,
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
            "Не удалось прочитать Trade State:\n"
            f"{TRADE_STATE_PATH}"
        ) from error

    state, changed = (
        normalize_trade_state(
            state
        )
    )

    if changed:

        save_trade_state(
            state
        )

    return state


# ============================================================
# SAVE
# ============================================================

def save_trade_state(
    state: dict,
):
    """
    Сохраняет Trade State атомарно.

    Сначала JSON полностью записывается
    во временный файл в той же папке,
    затем одним os.replace() подменяет
    основной state-файл.

    Это особенно важно перед реальным
    order_send(): SEND_INTENT должен быть
    надёжно записан на диск ДО обращения
    к MT5.
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

    temp_path = (
        TRADE_STATE_PATH.with_suffix(
            ".json.tmp"
        )
    )

    try:

        with open(
            temp_path,
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                state,
                file,
                ensure_ascii=False,
                indent=2,
            )

            file.flush()

            os.fsync(
                file.fileno()
            )

        os.replace(
            temp_path,
            TRADE_STATE_PATH,
        )

    finally:

        if temp_path.exists():

            try:
                temp_path.unlink()

            except OSError:
                pass


# ============================================================
# NORMALIZE VALUES
# ============================================================

def normalize_price(
    value,
):
    """
    Нормализует цену для сигнатуры.
    """

    if value is None:
        return None

    return round(
        float(
            value
        ),
        5,
    )


def normalize_volume(
    value,
):
    """
    Нормализует lot.
    """

    if value is None:
        return None

    return round(
        float(
            value
        ),
        8,
    )


# ============================================================
# ID
# ============================================================

def generate_plan_id() -> str:
    """
    Генерирует ID Trade Plan.
    """

    return (
        uuid.uuid4()
        .hex[:16]
    )


def generate_action_id() -> str:
    """
    Генерирует ID служебного action.
    """

    return (
        uuid.uuid4()
        .hex[:16]
    )


# ============================================================
# PLAN SIGNATURE
# ============================================================

def build_plan_signature(
    symbol: str,
    action: str,
    order_type: str,
    entry_price,
    stop_loss,
    take_profit,
    invalidation_level,
    volume,
) -> str:
    """
    Создаёт сигнатуру торгового плана.
    """

    signature_data = {

        "symbol": str(
            symbol
        ),

        "action": str(
            action
        ),

        "order_type": str(
            order_type
        ),

        "entry_price": (
            normalize_price(
                entry_price
            )
        ),

        "stop_loss": (
            normalize_price(
                stop_loss
            )
        ),

        "take_profit": (
            normalize_price(
                take_profit
            )
        ),

        "invalidation_level": (
            normalize_price(
                invalidation_level
            )
        ),

        "volume": (
            normalize_volume(
                volume
            )
        ),
    }

    raw = json.dumps(
        signature_data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        raw.encode(
            "utf-8"
        )
    ).hexdigest()


# ============================================================
# H1 SOURCE BAR
# ============================================================

def extract_latest_closed_h1_time(
    snapshot: dict | None,
):
    """
    Возвращает время последней
    закрытой H1-свечи.
    """

    if snapshot is None:
        return None

    try:

        timeframe = (
            snapshot[
                "timeframes"
            ][
                "H1"
            ]
        )

        closed_bars = (
            timeframe[
                "closed_bars"
            ]
        )

    except (
        KeyError,
        TypeError,
    ):

        return None

    if closed_bars is None:
        return None

    # ========================================================
    # DATAFRAME
    # ========================================================

    if isinstance(
        closed_bars,
        pd.DataFrame,
    ):

        if closed_bars.empty:
            return None

        if (
            "time_fp"
            not in closed_bars.columns
        ):

            return None

        value = (
            closed_bars.iloc[
                -1
            ][
                "time_fp"
            ]
        )

        if hasattr(
            value,
            "isoformat",
        ):

            return (
                value.isoformat()
            )

        return str(
            value
        )

    # ========================================================
    # LIST
    # ========================================================

    if isinstance(
        closed_bars,
        list,
    ):

        if not closed_bars:
            return None

        last_bar = (
            closed_bars[
                -1
            ]
        )

        if not isinstance(
            last_bar,
            dict,
        ):

            return None

        value = (
            last_bar.get(
                "time_fp"
            )
            or
            last_bar.get(
                "time"
            )
        )

        if value is None:
            return None

        if hasattr(
            value,
            "isoformat",
        ):

            return (
                value.isoformat()
            )

        return str(
            value
        )

    return None


# ============================================================
# PLAN HISTORY
# ============================================================

def archive_plan(
    state: dict,
    plan: dict,
    final_status: str,
    reason: str,
):
    """
    Добавляет snapshot плана в history.
    """

    archived = copy.deepcopy(
        plan
    )

    archived[
        "status"
    ] = final_status

    archived[
        "archived_at_fp"
    ] = (
        now_fp().isoformat()
    )

    archived[
        "archive_reason"
    ] = str(
        reason
    )

    history = state.setdefault(
        "history",
        [],
    )

    history.append(
        archived
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


# ============================================================
# ACTION HISTORY
# ============================================================

def archive_action(
    state: dict,
    action: dict,
):
    """
    Переносит завершённый action
    в action_history.
    """

    history = state.setdefault(
        "action_history",
        [],
    )

    history.append(
        copy.deepcopy(
            action
        )
    )

    if (
        len(
            history
        )
        > MAX_ACTION_HISTORY_ITEMS
    ):

        state[
            "action_history"
        ] = history[
            -MAX_ACTION_HISTORY_ITEMS:
        ]


# ============================================================
# CANCEL ACTION SEARCH
# ============================================================

def find_pending_cancel_action(
    state: dict,
    ticket: int,
) -> dict | None:
    """
    Ищет существующий cancel_pending action.
    """

    for action in state.get(
        "pending_actions",
        [],
    ):

        if (
            action.get(
                "type"
            )
            == ACTION_CANCEL_PENDING
            and
            action.get(
                "status"
            )
            == ACTION_STATUS_PENDING
            and
            int(
                action.get(
                    "ticket",
                    0,
                )
                or 0
            )
            == int(
                ticket
            )
        ):

            return action

    return None


# ============================================================
# QUEUE CANCEL PENDING
# ============================================================

def queue_cancel_pending_action(
    state: dict,
    plan: dict,
    reason: str,
) -> tuple[dict, bool]:
    """
    Создаёт action на отмену pending-order.
    """

    ticket = (
        plan.get(
            "pending_ticket"
        )
    )

    if ticket is None:

        raise ValueError(
            "Нельзя создать cancel_pending: "
            "у плана отсутствует pending_ticket."
        )

    existing = (
        find_pending_cancel_action(
            state=state,
            ticket=int(
                ticket
            ),
        )
    )

    if existing is not None:

        return (
            existing,
            False,
        )

    action = {

        "action_id": (
            generate_action_id()
        ),

        "type": (
            ACTION_CANCEL_PENDING
        ),

        "status": (
            ACTION_STATUS_PENDING
        ),

        "created_at_fp": (
            now_fp().isoformat()
        ),

        "plan_id": (
            plan.get(
                "plan_id"
            )
        ),

        "symbol": (
            plan.get(
                "symbol"
            )
        ),

        "ticket": int(
            ticket
        ),

        "reason": str(
            reason
        ),

        "plan_snapshot": (
            copy.deepcopy(
                plan
            )
        ),
    }

    state.setdefault(
        "pending_actions",
        [],
    ).append(
        action
    )

    return (
        action,
        True,
    )


# ============================================================
# QUEUE POSITION MANAGEMENT
# ============================================================

def queue_position_management_action(
    state: dict,
    position: dict,
    reason: str,
) -> tuple[dict, bool]:
    """
    Заготовка под будущие технические
    actions управления позицией.

    В текущей стратегии позиция
    не переоценивается Claude.
    """

    position_ticket = (
        position.get(
            "position_ticket"
        )
    )

    if position_ticket is None:

        raise ValueError(
            "У managed position отсутствует "
            "position_ticket."
        )

    for action in state.get(
        "pending_actions",
        [],
    ):

        if (
            action.get(
                "type"
            )
            == ACTION_MANAGE_POSITION
            and
            action.get(
                "status"
            )
            == ACTION_STATUS_PENDING
            and
            int(
                action.get(
                    "ticket",
                    0,
                )
                or 0
            )
            == int(
                position_ticket
            )
        ):

            return (
                action,
                False,
            )

    action = {

        "action_id": (
            generate_action_id()
        ),

        "type": (
            ACTION_MANAGE_POSITION
        ),

        "status": (
            ACTION_STATUS_PENDING
        ),

        "created_at_fp": (
            now_fp().isoformat()
        ),

        "plan_id": (
            position.get(
                "plan_id"
            )
        ),

        "symbol": (
            position.get(
                "symbol"
            )
        ),

        "ticket": int(
            position_ticket
        ),

        "reason": str(
            reason
        ),

        "position_snapshot": (
            copy.deepcopy(
                position
            )
        ),
    }

    state.setdefault(
        "pending_actions",
        [],
    ).append(
        action
    )

    return (
        action,
        True,
    )


# ============================================================
# GET PENDING ACTIONS
# ============================================================

def get_pending_actions(
    action_type: str | None = None,
) -> list[dict]:
    """
    Возвращает незавершённые actions.
    """

    state = load_trade_state()

    actions = []

    for action in state.get(
        "pending_actions",
        [],
    ):

        if (
            action.get(
                "status"
            )
            != ACTION_STATUS_PENDING
        ):

            continue

        if (
            action_type is not None
            and
            action.get(
                "type"
            )
            != action_type
        ):

            continue

        actions.append(
            copy.deepcopy(
                action
            )
        )

    return actions


# ============================================================
# COMPLETE ACTION
# ============================================================

def complete_pending_action(
    action_id: str,
    success: bool,
    result_note: str,
    mt5_result=None,
):
    """
    Завершает pending action.
    """

    state = load_trade_state()

    actions = state.get(
        "pending_actions",
        [],
    )

    found = None

    remaining = []

    for action in actions:

        if (
            action.get(
                "action_id"
            )
            == action_id
            and
            found is None
        ):

            found = (
                copy.deepcopy(
                    action
                )
            )

        else:

            remaining.append(
                action
            )

    if found is None:

        raise RuntimeError(
            "Pending action не найден: "
            f"{action_id}"
        )

    found[
        "status"
    ] = (
        ACTION_STATUS_COMPLETED
        if success
        else ACTION_STATUS_FAILED
    )

    found[
        "completed_at_fp"
    ] = (
        now_fp().isoformat()
    )

    found[
        "result_note"
    ] = str(
        result_note
    )

    if mt5_result is not None:

        found[
            "mt5_result"
        ] = mt5_result

    state[
        "pending_actions"
    ] = remaining

    archive_action(
        state,
        found,
    )

    save_trade_state(
        state
    )


# ============================================================
# BUILD APPROVED PLAN
# ============================================================

def build_approved_plan(
    analysis: dict,
    risk_report: dict,
    snapshot: dict | None = None,
) -> dict:
    """
    Формирует persistent Trade Plan.
    """

    recommendation = (
        analysis[
            "recommendation"
        ]
    )

    wave_count = (
        analysis[
            "wave_count"
        ]
    )

    market_regime = (
        analysis.get(
            "market_regime",
            {},
        )
    )

    scenario_map = (
        analysis.get(
            "scenario_map",
            {},
        )
    )

    trade = (
        risk_report[
            "trade"
        ]
    )

    current_time = (
        now_fp().isoformat()
    )

    symbol = str(
        trade.get(
            "symbol",
            SYMBOL,
        )
    )

    action = str(
        recommendation[
            "action"
        ]
    )

    order_type = str(
        recommendation[
            "order_type"
        ]
    )

    entry_price = float(
        recommendation[
            "entry_price"
        ]
    )

    stop_loss = float(
        recommendation[
            "stop_loss"
        ]
    )

    take_profit = float(
        recommendation[
            "take_profit"
        ]
    )

    # Основная инвалидация теперь относится ко всей торговой идее,
    # а не только к Elliott count. Для обратной совместимости
    # старое поле wave_invalidation_level продолжает использоваться
    # Executor-ом, но получает structural setup invalidation.
    invalidation_level = (
        recommendation.get(
            "invalidation_level"
        )
    )

    if invalidation_level is None:
        invalidation_level = (
            wave_count.get(
                "invalidation_level"
            )
        )

    if (
        invalidation_level
        is not None
    ):

        invalidation_level = float(
            invalidation_level
        )

    volume = float(
        trade[
            "volume"
        ]
    )

    signature = (
        build_plan_signature(
            symbol=symbol,
            action=action,
            order_type=order_type,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            invalidation_level=(
                invalidation_level
            ),
            volume=volume,
        )
    )

    return {

        "plan_id": (
            generate_plan_id()
        ),

        "signature": (
            signature
        ),

        "status": (
            PLAN_STATUS_APPROVED
        ),

        "execution_status": (
            EXECUTION_NOT_SENT
        ),

        "created_at_fp": (
            current_time
        ),

        "updated_at_fp": (
            current_time
        ),

        "source_analysis_time": (
            analysis.get(
                "timestamp"
            )
        ),

        "source_h1_closed_bar_time": (
            extract_latest_closed_h1_time(
                snapshot
            )
        ),

        "symbol": (
            symbol
        ),

        "action": (
            action
        ),

        "order_type": (
            order_type
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

        "trade_horizon": (
            recommendation.get(
                "trade_horizon"
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

        "market_regime": (
            market_regime.get(
                "primary_regime"
            )
        ),

        "market_direction": (
            market_regime.get(
                "direction"
            )
        ),

        "current_phase": (
            market_regime.get(
                "current_phase"
            )
            or
            market_regime.get(
                "phase"
            )
        ),

        # Legacy alias retained for compatibility with old state readers.
        "market_phase": (
            market_regime.get(
                "current_phase"
            )
            or
            market_regime.get(
                "phase"
            )
        ),

        "phase_status": (
            market_regime.get(
                "phase_status"
            )
        ),

        "entry_price": (
            entry_price
        ),

        "stop_loss": (
            stop_loss
        ),

        "take_profit": (
            take_profit
        ),

        # Legacy name used by current Executor. Semantically this is now
        # the structural invalidation of the complete trading setup.
        "wave_invalidation_level": (
            invalidation_level
        ),

        "setup_invalidation_level": (
            invalidation_level
        ),

        "why_now": (
            recommendation.get(
                "why_now"
            )
        ),

        "structural_stop_basis": (
            recommendation.get(
                "structural_stop_basis"
            )
        ),

        "target_basis": (
            recommendation.get(
                "target_basis"
            )
        ),

        "scenario_primary": (
            scenario_map.get(
                "primary_scenario"
            )
        ),

        "scenario_next_opportunity": (
            scenario_map.get(
                "next_opportunity"
            )
        ),

        "volume": (
            volume
        ),

        "risk_reward": (
            trade.get(
                "risk_reward"
            )
        ),

        "risk_budget": (
            trade.get(
                "risk_budget"
            )
        ),

        "expected_loss": (
            trade.get(
                "expected_loss"
            )
        ),

        "expected_profit": (
            trade.get(
                "expected_profit"
            )
        ),

        "required_margin": (
            trade.get(
                "required_margin"
            )
        ),

        "wave_structure": (
            wave_count.get(
                "structure_type"
            )
        ),

        "wave_direction": (
            wave_count.get(
                "direction"
            )
        ),

        "wave_label": (
            wave_count.get(
                "current_label"
            )
        ),

        "wave_summary": (
            wave_count.get(
                "summary"
            )
        ),

        "reasoning": (
            recommendation.get(
                "reasoning"
            )
        ),

        "invalidation_reason": (
            recommendation.get(
                "invalidation_reason"
            )
        ),

        "confirmation_count": 1,

        "last_confirmed_at_fp": (
            current_time
        ),

        "pending_ticket": None,

        "position_ticket": None,

        "source_pending_ticket": None,

        "requires_order_cancel": False,

        "requires_reanalysis": False,

        "invalidation_trigger_price": None,

        "invalidation_triggered_at_fp": None,

        # ----------------------------------------------------
        # EXPIRATION
        # ----------------------------------------------------

        "expired_at_fp": None,

        "expiration_reason": None,
    }


# ============================================================
# LAST DECISION
# ============================================================

def set_last_decision(
    state: dict,
    decision: str,
    analysis: dict,
    risk_report: dict,
    note: str,
):
    """
    Сохраняет последнее решение системы.
    """

    recommendation = (
        analysis.get(
            "recommendation",
            {},
        )
    )

    state[
        "last_decision"
    ] = {

        "recorded_at_fp": (
            now_fp().isoformat()
        ),

        "analysis_time": (
            analysis.get(
                "timestamp"
            )
        ),

        "decision": (
            decision
        ),

        "action": (
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

        "risk_approved": bool(
            risk_report.get(
                "approved",
                False,
            )
        ),

        "note": str(
            note
        ),
    }


# ============================================================
# GET MANAGED POSITIONS
# ============================================================

def get_managed_positions() -> list[dict]:
    """
    Возвращает managed positions.
    """

    state = load_trade_state()

    return copy.deepcopy(
        state.get(
            "managed_positions",
            [],
        )
    )


# ============================================================
# FIND MANAGED POSITION
# ============================================================

def find_managed_position(
    plan_id: str | None = None,
    position_ticket: int | None = None,
) -> dict | None:
    """
    Ищет managed position.
    """

    state = load_trade_state()

    for position in state.get(
        "managed_positions",
        [],
    ):

        if (
            plan_id is not None
            and
            position.get(
                "plan_id"
            )
            == plan_id
        ):

            return (
                copy.deepcopy(
                    position
                )
            )

        if (
            position_ticket is not None
            and
            int(
                position.get(
                    "position_ticket",
                    0,
                )
                or 0
            )
            == int(
                position_ticket
            )
        ):

            return (
                copy.deepcopy(
                    position
                )
            )

    return None


def update_managed_position_protection(
    *,
    position_ticket: int,
    actual_stop_loss: float,
    actual_take_profit: float,
    expected_previous_stop_loss: float | None = None,
    expected_previous_take_profit: float | None = None,
    audit: dict | None = None,
) -> dict:
    """Persist broker-confirmed SL/TP for one managed position.

    The function is deliberately compare-and-set: if Trade State changed after
    the review was built, it refuses to overwrite the newer protection.
    """
    state = load_trade_state()
    matches = [
        item
        for item in state.get("managed_positions", [])
        if isinstance(item, dict)
        and int(item.get("position_ticket", 0) or 0) == int(position_ticket)
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "Для обновления protection нужна ровно одна managed position "
            f"с ticket #{position_ticket}; найдено {len(matches)}."
        )

    position = matches[0]
    previous_sl = float(position.get("stop_loss", 0.0) or 0.0)
    previous_tp = float(position.get("take_profit", 0.0) or 0.0)
    tolerance = 1e-9
    if (
        expected_previous_stop_loss is not None
        and abs(previous_sl - float(expected_previous_stop_loss)) > tolerance
    ):
        raise RuntimeError(
            "Trade State SL изменился после построения protection plan."
        )
    if (
        expected_previous_take_profit is not None
        and abs(previous_tp - float(expected_previous_take_profit)) > tolerance
    ):
        raise RuntimeError(
            "Trade State TP изменился после построения protection plan."
        )

    current_time = now_fp().isoformat()
    position["stop_loss"] = float(actual_stop_loss)
    position["take_profit"] = float(actual_take_profit)
    position["protection_updated_at_fp"] = current_time
    history = position.setdefault("protection_history", [])
    if not isinstance(history, list):
        history = []
        position["protection_history"] = history
    history.append(
        {
            "updated_at_fp": current_time,
            "previous_stop_loss": previous_sl,
            "new_stop_loss": float(actual_stop_loss),
            "previous_take_profit": previous_tp,
            "new_take_profit": float(actual_take_profit),
            "audit": copy.deepcopy(audit) if isinstance(audit, dict) else {},
        }
    )
    if len(history) > MAX_PROTECTION_HISTORY_ITEMS:
        position["protection_history"] = history[-MAX_PROTECTION_HISTORY_ITEMS:]
    save_trade_state(state)
    return {
        "updated": True,
        "position_ticket": int(position_ticket),
        "previous_stop_loss": previous_sl,
        "new_stop_loss": float(actual_stop_loss),
        "previous_take_profit": previous_tp,
        "new_take_profit": float(actual_take_profit),
        "updated_at_fp": current_time,
    }


# ============================================================
# GET ACTIVE PLAN
# ============================================================

def get_active_plan() -> dict | None:
    """
    Возвращает текущий entry/pending plan.
    """

    state = load_trade_state()

    plan = state.get(
        "active_plan"
    )

    if plan is None:
        return None

    return copy.deepcopy(
        plan
    )


# ============================================================
# EXPIRE ACTIVE PLAN
# ============================================================

def expire_active_plan(
    reason: str,
    expected_plan_id: str | None = None,
) -> dict:
    """
    Переводит НЕОТПРАВЛЕННЫЙ active_plan
    в EXPIRED.

    Используется, например, когда:

        market plan старше допустимого TTL.

    КРИТИЧЕСКИ:

    Функция НЕ имеет права истекать:

        - pending order;
        - открытую позицию;
        - plan с неизвестным execution state.

    Такие состояния требуют отдельного
    жизненного цикла.
    """

    state = load_trade_state()

    plan = state.get(
        "active_plan"
    )

    if plan is None:

        return {

            "expired": False,

            "reason": (
                "Активного Trade Plan нет."
            ),

            "plan_id": None,

            "state_path": str(
                TRADE_STATE_PATH
            ),
        }

    plan_id = (
        plan.get(
            "plan_id"
        )
    )

    if (
        expected_plan_id is not None
        and
        str(
            plan_id
        )
        !=
        str(
            expected_plan_id
        )
    ):

        raise RuntimeError(
            "Нельзя истечь Trade Plan: "
            "active_plan уже изменился. "
            f"Expected={expected_plan_id}, "
            f"actual={plan_id}."
        )

    execution_status = (
        plan.get(
            "execution_status"
        )
    )

    pending_ticket = (
        plan.get(
            "pending_ticket"
        )
    )

    position_ticket = (
        plan.get(
            "position_ticket"
        )
    )

    # ========================================================
    # ТОЛЬКО NOT SENT
    # ========================================================

    if (
        execution_status
        != EXECUTION_NOT_SENT
    ):

        raise RuntimeError(
            "Нельзя автоматически пометить "
            "Trade Plan как expired: "
            "execution_status="
            f"{execution_status}."
        )

    if pending_ticket is not None:

        raise RuntimeError(
            "Нельзя пометить Trade Plan "
            "как expired: существует "
            f"pending ticket #{pending_ticket}."
        )

    if position_ticket is not None:

        raise RuntimeError(
            "Нельзя пометить Trade Plan "
            "как expired: существует "
            f"position ticket #{position_ticket}."
        )

    expired_at = (
        now_fp().isoformat()
    )

    expired_plan = (
        copy.deepcopy(
            plan
        )
    )

    expired_plan[
        "status"
    ] = PLAN_STATUS_EXPIRED

    expired_plan[
        "execution_status"
    ] = EXECUTION_EXPIRED

    expired_plan[
        "expired_at_fp"
    ] = expired_at

    expired_plan[
        "expiration_reason"
    ] = str(
        reason
    )

    expired_plan[
        "updated_at_fp"
    ] = expired_at

    archive_plan(
        state=state,
        plan=expired_plan,
        final_status=PLAN_STATUS_EXPIRED,
        reason=reason,
    )

    # ========================================================
    # ACTIVE PLAN БОЛЬШЕ НЕ СУЩЕСТВУЕТ
    # ========================================================

    state[
        "active_plan"
    ] = None

    # ========================================================
    # LAST DECISION
    # ========================================================

    state[
        "last_decision"
    ] = {

        "recorded_at_fp": (
            expired_at
        ),

        "analysis_time": (
            plan.get(
                "source_analysis_time"
            )
        ),

        "decision": (
            "PLAN_EXPIRED"
        ),

        "action": (
            plan.get(
                "action"
            )
        ),

        "order_type": (
            plan.get(
                "order_type"
            )
        ),

        "confidence": (
            plan.get(
                "confidence"
            )
        ),

        "risk_approved": True,

        "note": str(
            reason
        ),
    }

    save_trade_state(
        state
    )

    return {

        "expired": True,

        "reason": str(
            reason
        ),

        "plan_id": (
            plan_id
        ),

        "expired_at_fp": (
            expired_at
        ),

        "archived_plan": (
            expired_plan
        ),

        "state_path": str(
            TRADE_STATE_PATH
        ),
    }


# ============================================================
# RETIRE OLD ACTIVE PLAN
# ============================================================

def retire_active_plan(
    state: dict,
    plan: dict,
    reason: str,
) -> dict:
    """
    Корректно снимает старый active_plan.
    """

    result = {

        "cancel_required": False,

        "cancel_action": None,

        "position_preserved": False,
    }

    execution_status = (
        plan.get(
            "execution_status"
        )
    )

    pending_ticket = (
        plan.get(
            "pending_ticket"
        )
    )

    position_ticket = (
        plan.get(
            "position_ticket"
        )
    )

    # ========================================================
    # POSITION OPEN
    # ========================================================

    if (
        position_ticket is not None
        or
        execution_status
        == EXECUTION_POSITION_OPEN
    ):

        managed = copy.deepcopy(
            plan
        )

        managed[
            "status"
        ] = PLAN_STATUS_FILLED

        managed[
            "execution_status"
        ] = EXECUTION_POSITION_OPEN

        managed[
            "managed_since_fp"
        ] = (
            managed.get(
                "managed_since_fp"
            )
            or
            now_fp().isoformat()
        )

        already_exists = any(
            existing.get(
                "plan_id"
            )
            == managed.get(
                "plan_id"
            )
            for existing
            in state.get(
                "managed_positions",
                [],
            )
        )

        if not already_exists:

            state.setdefault(
                "managed_positions",
                [],
            ).append(
                managed
            )

        result[
            "position_preserved"
        ] = True

        return result

    # ========================================================
    # PENDING ORDER
    # ========================================================

    if pending_ticket is not None:

        cancel_action, _ = (
            queue_cancel_pending_action(
                state=state,
                plan=plan,
                reason=reason,
            )
        )

        plan_to_archive = (
            copy.deepcopy(
                plan
            )
        )

        plan_to_archive[
            "status"
        ] = PLAN_STATUS_CANCEL_REQUESTED

        plan_to_archive[
            "execution_status"
        ] = EXECUTION_CANCEL_REQUESTED

        plan_to_archive[
            "requires_order_cancel"
        ] = True

        archive_plan(
            state=state,
            plan=plan_to_archive,
            final_status=(
                PLAN_STATUS_CANCEL_REQUESTED
            ),
            reason=reason,
        )

        result[
            "cancel_required"
        ] = True

        result[
            "cancel_action"
        ] = (
            copy.deepcopy(
                cancel_action
            )
        )

        return result

    # ========================================================
    # NOT SENT
    # ========================================================

    archive_plan(
        state=state,
        plan=plan,
        final_status=(
            PLAN_STATUS_SUPERSEDED
        ),
        reason=reason,
    )

    return result


# ============================================================
# REGISTER TRADE DECISION
# ============================================================

def register_trade_decision(
    analysis: dict,
    risk_report: dict,
    snapshot: dict | None = None,
) -> dict:
    """
    Главная функция Trade State.
    """

    state = load_trade_state()

    decision = str(
        risk_report.get(
            "decision",
            "",
        )
    )

    approved = bool(
        risk_report.get(
            "approved",
            False,
        )
    )

    active_plan = (
        state.get(
            "active_plan"
        )
    )

    newly_created_actions = []

    cancel_previous_order = False

    # ========================================================
    # APPROVED
    # ========================================================

    if (
        decision
        == "APPROVED"
        and
        approved
    ):

        candidate = (
            build_approved_plan(
                analysis=analysis,
                risk_report=risk_report,
                snapshot=snapshot,
            )
        )

        # ====================================================
        # SAME PLAN
        # ====================================================

        if (
            active_plan is not None
            and
            active_plan.get(
                "signature"
            )
            ==
            candidate.get(
                "signature"
            )
        ):

            current_time = (
                now_fp().isoformat()
            )

            active_plan[
                "updated_at_fp"
            ] = current_time

            active_plan[
                "last_confirmed_at_fp"
            ] = current_time

            active_plan[
                "confirmation_count"
            ] = (
                int(
                    active_plan.get(
                        "confirmation_count",
                        1,
                    )
                )
                + 1
            )

            active_plan[
                "source_analysis_time"
            ] = (
                candidate.get(
                    "source_analysis_time"
                )
            )

            active_plan[
                "source_h1_closed_bar_time"
            ] = (
                candidate.get(
                    "source_h1_closed_bar_time"
                )
            )

            active_plan[
                "confidence"
            ] = (
                candidate.get(
                    "confidence"
                )
            )

            active_plan[
                "wave_structure"
            ] = (
                candidate.get(
                    "wave_structure"
                )
            )

            active_plan[
                "wave_direction"
            ] = (
                candidate.get(
                    "wave_direction"
                )
            )

            active_plan[
                "wave_label"
            ] = (
                candidate.get(
                    "wave_label"
                )
            )

            active_plan[
                "wave_summary"
            ] = (
                candidate.get(
                    "wave_summary"
                )
            )

            active_plan[
                "reasoning"
            ] = (
                candidate.get(
                    "reasoning"
                )
            )

            active_plan[
                "invalidation_reason"
            ] = (
                candidate.get(
                    "invalidation_reason"
                )
            )

            active_plan[
                "requires_reanalysis"
            ] = False

            state[
                "active_plan"
            ] = active_plan

            set_last_decision(
                state=state,
                decision="CONFIRMED",
                analysis=analysis,
                risk_report=risk_report,
                note=(
                    "Новый анализ подтвердил "
                    "существующий Trade Plan."
                ),
            )

            save_trade_state(
                state
            )

            return (
                build_state_result(
                    state=state,
                    state_action=(
                        "CONFIRMED_EXISTING_PLAN"
                    ),
                    plan_changed=False,
                    cancel_previous_order=False,
                    newly_created_actions=[],
                )
            )

        # ====================================================
        # NEW PLAN
        # ====================================================

        replaced_plan_id = None

        if active_plan is not None:

            replaced_plan_id = (
                active_plan.get(
                    "plan_id"
                )
            )

            retired = (
                retire_active_plan(
                    state=state,
                    plan=active_plan,
                    reason=(
                        "Новый одобренный анализ "
                        "изменил Trade Plan."
                    ),
                )
            )

            cancel_previous_order = bool(
                retired[
                    "cancel_required"
                ]
            )

            if (
                retired[
                    "cancel_action"
                ]
                is not None
            ):

                newly_created_actions.append(
                    retired[
                        "cancel_action"
                    ]
                )

        candidate[
            "replaces_plan_id"
        ] = (
            replaced_plan_id
        )

        state[
            "active_plan"
        ] = candidate

        set_last_decision(
            state=state,
            decision="APPROVED",
            analysis=analysis,
            risk_report=risk_report,
            note=(
                "Создан новый одобренный "
                "Trade Plan."
            ),
        )

        save_trade_state(
            state
        )

        return (
            build_state_result(
                state=state,
                state_action=(
                    "CREATED_NEW_PLAN"
                ),
                plan_changed=True,
                cancel_previous_order=(
                    cancel_previous_order
                ),
                newly_created_actions=(
                    newly_created_actions
                ),
            )
        )

    # ========================================================
    # NO TRADE
    # ========================================================

    if (
        decision
        == "NO_TRADE"
    ):

        plan_changed = (
            active_plan
            is not None
        )

        if active_plan is not None:

            retired = (
                retire_active_plan(
                    state=state,
                    plan=active_plan,
                    reason=(
                        "Новый анализ Claude "
                        "вернул NO_TRADE."
                    ),
                )
            )

            cancel_previous_order = bool(
                retired[
                    "cancel_required"
                ]
            )

            if (
                retired[
                    "cancel_action"
                ]
                is not None
            ):

                newly_created_actions.append(
                    retired[
                        "cancel_action"
                    ]
                )

            state[
                "active_plan"
            ] = None

        set_last_decision(
            state=state,
            decision="NO_TRADE",
            analysis=analysis,
            risk_report=risk_report,
            note=(
                "Новая торговая идея отсутствует. "
                "Открытые позиции автоматически "
                "не закрываются."
            ),
        )

        save_trade_state(
            state
        )

        return (
            build_state_result(
                state=state,
                state_action="NO_TRADE",
                plan_changed=plan_changed,
                cancel_previous_order=(
                    cancel_previous_order
                ),
                newly_created_actions=(
                    newly_created_actions
                ),
            )
        )

    # ========================================================
    # REJECTED
    # ========================================================

    if (
        decision
        == "REJECTED"
    ):

        plan_changed = (
            active_plan
            is not None
        )

        if active_plan is not None:

            retired = (
                retire_active_plan(
                    state=state,
                    plan=active_plan,
                    reason=(
                        "Новая торговая идея "
                        "не прошла Risk Manager."
                    ),
                )
            )

            cancel_previous_order = bool(
                retired[
                    "cancel_required"
                ]
            )

            if (
                retired[
                    "cancel_action"
                ]
                is not None
            ):

                newly_created_actions.append(
                    retired[
                        "cancel_action"
                    ]
                )

            state[
                "active_plan"
            ] = None

        set_last_decision(
            state=state,
            decision="REJECTED",
            analysis=analysis,
            risk_report=risk_report,
            note=(
                "Новая торговая идея отклонена. "
                "Открытые позиции автоматически "
                "не закрываются."
            ),
        )

        save_trade_state(
            state
        )

        return (
            build_state_result(
                state=state,
                state_action="REJECTED",
                plan_changed=plan_changed,
                cancel_previous_order=(
                    cancel_previous_order
                ),
                newly_created_actions=(
                    newly_created_actions
                ),
            )
        )

    raise ValueError(
        "Trade State получил неизвестное "
        "решение Risk Manager: "
        f"{decision}"
    )


# ============================================================
# BUILD STATE RESULT
# ============================================================

def build_state_result(
    state: dict,
    state_action: str,
    plan_changed: bool,
    cancel_previous_order: bool,
    newly_created_actions: list,
) -> dict:
    """
    Единый ответ Trade State.
    """

    return {

        "state_action": (
            state_action
        ),

        "plan_changed": (
            plan_changed
        ),

        "cancel_previous_order": (
            cancel_previous_order
        ),

        "active_plan": (
            copy.deepcopy(
                state.get(
                    "active_plan"
                )
            )
        ),

        "managed_positions": (
            copy.deepcopy(
                state.get(
                    "managed_positions",
                    [],
                )
            )
        ),

        "managed_positions_count": len(
            state.get(
                "managed_positions",
                [],
            )
        ),

        "new_actions": (
            copy.deepcopy(
                newly_created_actions
            )
        ),

        "pending_actions": (
            copy.deepcopy(
                state.get(
                    "pending_actions",
                    [],
                )
            )
        ),

        "pending_actions_count": len(
            state.get(
                "pending_actions",
                [],
            )
        ),

        "state_path": str(
            TRADE_STATE_PATH
        ),
    }


# ============================================================
# ATTACH PENDING TICKET
# ============================================================

def attach_pending_ticket(
    plan_id: str,
    ticket: int,
    order_snapshot: dict | None = None,
):
    """
    Привязывает реальный MT5 pending ticket.

    Допускает два нормальных источника:

        - обычный переход not_sent -> pending_sent;
        - восстановление durable SEND_INTENT -> pending_sent.

    Повторная запись того же ticket идемпотентна.
    Другой ticket для того же plan_id запрещён.
    """

    state = load_trade_state()

    plan = state.get(
        "active_plan"
    )

    if plan is None:
        raise RuntimeError(
            "Невозможно записать pending ticket: "
            "active_plan отсутствует."
        )

    if str(
        plan.get(
            "plan_id",
            "",
        )
    ) != str(
        plan_id
    ):
        raise RuntimeError(
            "Plan ID не совпадает с active_plan."
        )

    current_ticket = plan.get(
        "pending_ticket"
    )

    if current_ticket is not None:
        if int(
            current_ticket
        ) != int(
            ticket
        ):
            raise RuntimeError(
                "Trade Plan уже содержит другой "
                "pending ticket: "
                f"{current_ticket}"
            )

        if (
            plan.get(
                "execution_status"
            )
            in (
                EXECUTION_PENDING_SENT,
                EXECUTION_CANCEL_REQUESTED,
            )
        ):
            return {
                "attached": False,
                "already_attached": True,
                "plan_id": str(
                    plan_id
                ),
                "ticket": int(
                    ticket
                ),
                "state_path": str(
                    TRADE_STATE_PATH
                ),
            }

    execution_status = plan.get(
        "execution_status"
    )

    if execution_status not in (
        EXECUTION_NOT_SENT,
        EXECUTION_SEND_INTENT,
    ):
        raise RuntimeError(
            "Невозможно привязать pending ticket: "
            "execution_status="
            f"{execution_status}."
        )

    current_time = now_fp().isoformat()

    plan[
        "pending_ticket"
    ] = int(
        ticket
    )

    plan[
        "status"
    ] = PLAN_STATUS_PENDING

    plan[
        "execution_status"
    ] = EXECUTION_PENDING_SENT

    plan[
        "pending_sent_at_fp"
    ] = (
        plan.get(
            "pending_sent_at_fp"
        )
        or current_time
    )

    plan[
        "requires_order_cancel"
    ] = False

    if order_snapshot is not None:
        plan[
            "pending_order_snapshot"
        ] = copy.deepcopy(
            order_snapshot
        )

    plan[
        "updated_at_fp"
    ] = current_time

    state[
        "active_plan"
    ] = plan

    save_trade_state(
        state
    )

    return {
        "attached": True,
        "already_attached": False,
        "plan_id": str(
            plan_id
        ),
        "ticket": int(
            ticket
        ),
        "attached_at_fp": current_time,
        "state_path": str(
            TRADE_STATE_PATH
        ),
    }


# ============================================================
# MARKET SEND INTENT
# ============================================================

def mark_active_plan_send_intent(
    plan_id: str,
    request: dict,
) -> dict:
    """
    ДО реального mt5.order_send() фиксирует
    необратимый SEND_INTENT в Trade State.

    После этой записи автоматический повторный
    order_send() для того же плана запрещён,
    пока reconciliation не докажет результат
    первой попытки.

    Это защита от сценария:

        state сохранён
        ↓
        order_send()
        ↓
        MT5 принял ордер
        ↓
        Python упал до attach_position_ticket()
        ↓
        restart
        ↓
        НЕЛЬЗЯ отправлять второй ордер
    """

    state = load_trade_state()

    plan = state.get(
        "active_plan"
    )

    if plan is None:

        raise RuntimeError(
            "Невозможно создать SEND_INTENT: "
            "active_plan отсутствует."
        )

    actual_plan_id = str(
        plan.get(
            "plan_id",
            "",
        )
    )

    if (
        actual_plan_id
        != str(
            plan_id
        )
    ):

        raise RuntimeError(
            "Невозможно создать SEND_INTENT: "
            "Plan ID уже изменился. "
            f"Expected={plan_id}, "
            f"actual={actual_plan_id}."
        )

    execution_status = (
        plan.get(
            "execution_status"
        )
    )

    # Идемпотентное чтение уже записанного intent.
    if (
        execution_status
        == EXECUTION_SEND_INTENT
    ):

        return {

            "created": False,

            "already_exists": True,

            "plan_id": (
                actual_plan_id
            ),

            "send_intent_at_fp": (
                plan.get(
                    "send_intent_at_fp"
                )
            ),

            "send_attempt_id": (
                plan.get(
                    "send_attempt_id"
                )
            ),

            "request": copy.deepcopy(
                plan.get(
                    "send_request"
                )
            ),

            "state_path": str(
                TRADE_STATE_PATH
            ),
        }

    if (
        execution_status
        != EXECUTION_NOT_SENT
    ):

        raise RuntimeError(
            "Невозможно создать SEND_INTENT: "
            "execution_status="
            f"{execution_status}."
        )

    if (
        plan.get(
            "pending_ticket"
        )
        is not None
    ):

        raise RuntimeError(
            "Невозможно создать SEND_INTENT: "
            "active_plan уже содержит pending_ticket."
        )

    if (
        plan.get(
            "position_ticket"
        )
        is not None
    ):

        raise RuntimeError(
            "Невозможно создать SEND_INTENT: "
            "active_plan уже содержит position_ticket."
        )

    current_time = (
        now_fp().isoformat()
    )

    send_attempt_id = (
        uuid.uuid4().hex
    )

    plan[
        "execution_status"
    ] = EXECUTION_SEND_INTENT

    plan[
        "send_intent_at_fp"
    ] = current_time

    plan[
        "send_attempt_id"
    ] = send_attempt_id

    plan[
        "send_request"
    ] = copy.deepcopy(
        request
    )

    plan[
        "updated_at_fp"
    ] = current_time

    save_trade_state(
        state
    )

    return {

        "created": True,

        "already_exists": False,

        "plan_id": (
            actual_plan_id
        ),

        "send_intent_at_fp": (
            current_time
        ),

        "send_attempt_id": (
            send_attempt_id
        ),

        "request": copy.deepcopy(
            request
        ),

        "state_path": str(
            TRADE_STATE_PATH
        ),
    }


# ============================================================
# ORDER SEND RESULT
# ============================================================

def record_active_plan_order_send_result(
    plan_id: str,
    order_send_result: dict | None,
) -> dict:
    """
    Сохраняет ответ mt5.order_send() в active_plan.

    Это НЕ завершает lifecycle и НЕ разрешает
    повторную отправку. execution_status остаётся
    SEND_INTENT до reconciliation/attach либо
    до подтверждённого execution failure.
    """

    state = load_trade_state()

    plan = state.get(
        "active_plan"
    )

    if plan is None:

        return {

            "recorded": False,

            "reason": (
                "active_plan отсутствует."
            ),

            "plan_id": None,

            "state_path": str(
                TRADE_STATE_PATH
            ),
        }

    actual_plan_id = str(
        plan.get(
            "plan_id",
            "",
        )
    )

    if (
        actual_plan_id
        != str(
            plan_id
        )
    ):

        raise RuntimeError(
            "Невозможно сохранить order_send result: "
            "Plan ID уже изменился. "
            f"Expected={plan_id}, "
            f"actual={actual_plan_id}."
        )

    if (
        plan.get(
            "execution_status"
        )
        != EXECUTION_SEND_INTENT
    ):

        raise RuntimeError(
            "Невозможно сохранить order_send result: "
            "execution_status="
            f"{plan.get('execution_status')}."
        )

    current_time = (
        now_fp().isoformat()
    )

    plan[
        "order_send_result"
    ] = copy.deepcopy(
        order_send_result
    )

    plan[
        "order_send_result_at_fp"
    ] = current_time

    plan[
        "updated_at_fp"
    ] = current_time

    save_trade_state(
        state
    )

    return {

        "recorded": True,

        "reason": (
            "order_send result сохранён."
        ),

        "plan_id": (
            actual_plan_id
        ),

        "recorded_at_fp": (
            current_time
        ),

        "state_path": str(
            TRADE_STATE_PATH
        ),
    }


# ============================================================
# MARKET SEND FAILED
# ============================================================

def mark_active_plan_execution_failed(
    plan_id: str,
    reason: str,
    order_send_result: dict | None = None,
) -> dict:
    """
    Завершает active_plan после ДОКАЗАННОГО
    отказа mt5.order_send().

    ВАЖНО:
    применять только если результат MT5
    однозначно означает, что сделка НЕ была
    исполнена, и reconciliation не нашёл
    позиции/ордера/deal этого плана.

    После этого план архивируется и повторно
    на той же H1 не отправляется.
    """

    state = load_trade_state()

    plan = state.get(
        "active_plan"
    )

    if plan is None:

        return {

            "failed": False,

            "reason": (
                "active_plan отсутствует."
            ),

            "plan_id": None,

            "state_path": str(
                TRADE_STATE_PATH
            ),
        }

    actual_plan_id = str(
        plan.get(
            "plan_id",
            "",
        )
    )

    if (
        actual_plan_id
        != str(
            plan_id
        )
    ):

        raise RuntimeError(
            "Невозможно завершить failed execution: "
            "Plan ID уже изменился. "
            f"Expected={plan_id}, "
            f"actual={actual_plan_id}."
        )

    execution_status = (
        plan.get(
            "execution_status"
        )
    )

    if (
        execution_status
        not in (
            EXECUTION_NOT_SENT,
            EXECUTION_SEND_INTENT,
        )
    ):

        raise RuntimeError(
            "Невозможно завершить failed execution: "
            "execution_status="
            f"{execution_status}."
        )

    current_time = (
        now_fp().isoformat()
    )

    failed_plan = copy.deepcopy(
        plan
    )

    failed_plan[
        "status"
    ] = PLAN_STATUS_EXECUTION_FAILED

    failed_plan[
        "execution_status"
    ] = EXECUTION_SEND_FAILED

    failed_plan[
        "execution_failed_at_fp"
    ] = current_time

    failed_plan[
        "execution_failure_reason"
    ] = str(
        reason
    )

    failed_plan[
        "order_send_result"
    ] = copy.deepcopy(
        order_send_result
    )

    failed_plan[
        "updated_at_fp"
    ] = current_time

    archive_plan(
        state=state,
        plan=failed_plan,
        final_status=(
            PLAN_STATUS_EXECUTION_FAILED
        ),
        reason=reason,
    )

    state[
        "active_plan"
    ] = None

    state[
        "last_decision"
    ] = {

        "recorded_at_fp": (
            current_time
        ),

        "analysis_time": (
            plan.get(
                "source_analysis_time"
            )
        ),

        "decision": (
            "EXECUTION_FAILED"
        ),

        "action": (
            plan.get(
                "action"
            )
        ),

        "order_type": (
            plan.get(
                "order_type"
            )
        ),

        "confidence": (
            plan.get(
                "confidence"
            )
        ),

        "risk_approved": True,

        "note": str(
            reason
        ),
    }

    save_trade_state(
        state
    )

    return {

        "failed": True,

        "reason": str(
            reason
        ),

        "plan_id": (
            actual_plan_id
        ),

        "failed_at_fp": (
            current_time
        ),

        "archived_plan": (
            failed_plan
        ),

        "state_path": str(
            TRADE_STATE_PATH
        ),
    }


# ============================================================
# ATTACH POSITION TICKET
# ============================================================

def attach_position_ticket(
    plan_id: str,
    ticket: int,
    position_identifier: int | None = None,
    actual_volume: float | None = None,
    actual_open_price: float | None = None,
    actual_stop_loss: float | None = None,
    actual_take_profit: float | None = None,
    order_ticket: int | None = None,
    deal_ticket: int | None = None,
):
    """
    Переносит Trade Plan из active_plan
    в managed_positions после открытия позиции.
    """

    state = load_trade_state()

    plan = (
        state.get(
            "active_plan"
        )
    )

    if plan is None:

        # Идемпотентность.
        for existing in state.get(
            "managed_positions",
            [],
        ):

            if (
                existing.get(
                    "plan_id"
                )
                == plan_id
                and
                int(
                    existing.get(
                        "position_ticket",
                        0,
                    )
                    or 0
                )
                == int(
                    ticket
                )
            ):

                return

        raise RuntimeError(
            "Невозможно записать position ticket: "
            "active_plan отсутствует."
        )

    if (
        plan.get(
            "plan_id"
        )
        != plan_id
    ):

        raise RuntimeError(
            "Plan ID не совпадает "
            "с active_plan."
        )

    managed = (
        copy.deepcopy(
            plan
        )
    )

    # После реального fill источником истины
    # становятся фактические параметры MT5.
    #
    # Executor имеет право УМЕНЬШИТЬ lot,
    # поэтому managed position не должна
    # продолжать хранить старый plan volume.
    if actual_volume is not None:

        managed[
            "volume"
        ] = float(
            actual_volume
        )

        managed[
            "executed_volume"
        ] = float(
            actual_volume
        )

    if actual_open_price is not None:

        managed[
            "executed_entry_price"
        ] = float(
            actual_open_price
        )

    if actual_stop_loss is not None:

        managed[
            "stop_loss"
        ] = float(
            actual_stop_loss
        )

    if actual_take_profit is not None:

        managed[
            "take_profit"
        ] = float(
            actual_take_profit
        )

    if order_ticket is not None:

        managed[
            "mt5_order_ticket"
        ] = int(
            order_ticket
        )

    if deal_ticket is not None:

        managed[
            "mt5_deal_ticket"
        ] = int(
            deal_ticket
        )

    old_pending_ticket = (
        managed.get(
            "pending_ticket"
        )
    )

    managed[
        "source_pending_ticket"
    ] = (
        old_pending_ticket
    )

    managed[
        "pending_ticket"
    ] = None

    if position_identifier is not None:

        managed[
            "position_identifier"
        ] = int(
            position_identifier
        )

    managed[
        "position_ticket"
    ] = int(
        ticket
    )

    managed[
        "status"
    ] = PLAN_STATUS_FILLED

    managed[
        "execution_status"
    ] = EXECUTION_POSITION_OPEN

    current_time = (
        now_fp().isoformat()
    )

    managed[
        "filled_at_fp"
    ] = current_time

    managed[
        "managed_since_fp"
    ] = current_time

    managed[
        "updated_at_fp"
    ] = current_time

    managed[
        "requires_order_cancel"
    ] = False

    already_exists = any(
        existing.get(
            "plan_id"
        )
        == plan_id
        for existing
        in state.get(
            "managed_positions",
            [],
        )
    )

    if already_exists:

        raise RuntimeError(
            "Managed position для "
            f"Plan ID {plan_id} уже существует."
        )

    state.setdefault(
        "managed_positions",
        [],
    ).append(
        managed
    )

    state[
        "active_plan"
    ] = None

    save_trade_state(
        state
    )


# ============================================================
# MARK ACTIVE PLAN CANCELLED
# ============================================================

def mark_active_plan_cancelled(
    reason: str,
):
    """
    Завершает active pending plan
    после реальной отмены MT5-order.
    """

    state = load_trade_state()

    plan = (
        state.get(
            "active_plan"
        )
    )

    if plan is None:
        return

    plan[
        "execution_status"
    ] = EXECUTION_CANCELLED

    plan[
        "status"
    ] = PLAN_STATUS_CANCELLED

    plan[
        "requires_order_cancel"
    ] = False

    archive_plan(
        state=state,
        plan=plan,
        final_status=(
            PLAN_STATUS_CANCELLED
        ),
        reason=reason,
    )

    state[
        "active_plan"
    ] = None

    save_trade_state(
        state
    )


# ============================================================
# MARK MANAGED POSITION CLOSED
# ============================================================

def mark_managed_position_closed(
    reason: str,
    plan_id: str | None = None,
    position_ticket: int | None = None,
    close_info: dict | None = None,
):
    """
    Завершает managed position только после подтверждённого
    закрытия позиции в MT5.

    close_info сохраняется в history, чтобы Trade State содержал
    фактические данные MT5 о закрытии, а не только текст reason.
    """

    if (
        plan_id is None
        and
        position_ticket is None
    ):
        raise ValueError(
            "Нужно указать plan_id "
            "или position_ticket."
        )

    state = load_trade_state()

    managed_positions = state.get(
        "managed_positions",
        [],
    )

    found = None
    remaining = []

    for position in managed_positions:
        match = False

        if (
            plan_id is not None
            and
            position.get(
                "plan_id"
            )
            == plan_id
        ):
            match = True

        if (
            position_ticket is not None
            and
            int(
                position.get(
                    "position_ticket",
                    0,
                )
                or 0
            )
            == int(
                position_ticket
            )
        ):
            match = True

        if (
            match
            and
            found is None
        ):
            found = copy.deepcopy(
                position
            )
        else:
            remaining.append(
                position
            )

    if found is None:
        raise RuntimeError(
            "Managed position не найдена."
        )

    current_time = now_fp().isoformat()

    found[
        "status"
    ] = PLAN_STATUS_CLOSED

    found[
        "execution_status"
    ] = EXECUTION_CLOSED

    found[
        "closed_detected_at_fp"
    ] = current_time

    if isinstance(
        close_info,
        dict,
    ):
        # Сохраняем только сериализуемые нормализованные поля.
        fields = {
            "close_reason": close_info.get(
                "close_reason"
            ),
            "close_price": close_info.get(
                "close_price"
            ),
            "close_time_fp": close_info.get(
                "close_time_fp"
            ),
            "net_result": close_info.get(
                "net_result"
            ),
            "closing_deal_ticket": close_info.get(
                "deal_ticket"
            ),
            "closing_deal_reason": close_info.get(
                "raw_reason"
            ),
            "position_identifier": close_info.get(
                "position_identifier"
            )
            or found.get(
                "position_identifier"
            ),
            "close_lookup_method": close_info.get(
                "lookup_method"
            ),
            "position_identifier_source": close_info.get(
                "identifier_source"
            ),
            "close_deals_count": close_info.get(
                "deals_count"
            ),
        }

        for key, value in fields.items():
            if value is not None:
                found[
                    key
                ] = value

        found[
            "closed_at_fp"
        ] = (
            close_info.get(
                "close_time_fp"
            )
            or current_time
        )

    else:
        found[
            "closed_at_fp"
        ] = current_time

    archive_plan(
        state=state,
        plan=found,
        final_status=PLAN_STATUS_CLOSED,
        reason=reason,
    )

    state[
        "managed_positions"
    ] = remaining

    save_trade_state(
        state
    )


# ============================================================
# ACTIVE PLAN PRICE INVALIDATION
# ============================================================

def check_active_plan_price_invalidation(
    symbol: str = SYMBOL,
) -> dict:
    """
    Проверяет wave invalidation
    активного entry/pending плана.

    Ничего автоматически не закрывает.
    """

    plan = get_active_plan()

    if plan is None:

        return {

            "has_active_plan": False,

            "breached": False,

            "requires_reanalysis": False,

            "reason": (
                "Активного торгового плана нет."
            ),
        }

    invalidation_level = (
        plan.get(
            "wave_invalidation_level"
        )
    )

    if invalidation_level is None:

        return {

            "has_active_plan": True,

            "plan_id": (
                plan.get(
                    "plan_id"
                )
            ),

            "breached": False,

            "requires_reanalysis": False,

            "reason": (
                "У Trade Plan отсутствует "
                "wave_invalidation_level."
            ),
        }

    tick = (
        mt5.symbol_info_tick(
            symbol
        )
    )

    if tick is None:

        raise RuntimeError(
            f"Не удалось получить tick {symbol}. "
            f"MT5 error: {mt5.last_error()}"
        )

    action = (
        plan.get(
            "action"
        )
    )

    invalidation_level = float(
        invalidation_level
    )

    bid = float(
        tick.bid
    )

    ask = float(
        tick.ask
    )

    breached = False

    trigger_price = None

    if (
        action
        == "enter_long"
    ):

        trigger_price = bid

        breached = (
            bid
            <= invalidation_level
        )

    elif (
        action
        == "enter_short"
    ):

        trigger_price = ask

        breached = (
            ask
            >= invalidation_level
        )

    else:

        return {

            "has_active_plan": True,

            "plan_id": (
                plan.get(
                    "plan_id"
                )
            ),

            "breached": False,

            "requires_reanalysis": False,

            "reason": (
                f"Неизвестный action: "
                f"{action}"
            ),
        }

    return {

        "has_active_plan": True,

        "plan_id": (
            plan.get(
                "plan_id"
            )
        ),

        "action": (
            action
        ),

        "invalidation_level": (
            invalidation_level
        ),

        "bid": (
            bid
        ),

        "ask": (
            ask
        ),

        "trigger_price": (
            trigger_price
        ),

        "breached": (
            breached
        ),

        "requires_reanalysis": (
            breached
        ),

        "reason": (
            "Цена достигла wave_invalidation_level."
            if breached
            else
            "Wave invalidation level не достигнут."
        ),
    }


# ============================================================
# MANAGED POSITIONS INVALIDATION
# ============================================================

def check_managed_positions_price_invalidation(
    symbol: str = SYMBOL,
) -> list[dict]:
    """
    Сохраняется как диагностическая функция.

    По текущей стратегии достижение
    wave invalidation НЕ меняет уже
    открытую позицию.

    Открытая позиция живёт до SL или TP.
    """

    positions = (
        get_managed_positions()
    )

    results = []

    tick = (
        mt5.symbol_info_tick(
            symbol
        )
    )

    if tick is None:

        raise RuntimeError(
            f"Не удалось получить tick {symbol}. "
            f"MT5 error: {mt5.last_error()}"
        )

    bid = float(
        tick.bid
    )

    ask = float(
        tick.ask
    )

    for position in positions:

        if (
            position.get(
                "symbol"
            )
            != symbol
        ):

            continue

        invalidation_level = (
            position.get(
                "wave_invalidation_level"
            )
        )

        if invalidation_level is None:

            results.append({

                "plan_id": (
                    position.get(
                        "plan_id"
                    )
                ),

                "position_ticket": (
                    position.get(
                        "position_ticket"
                    )
                ),

                "breached": False,

                "requires_reanalysis": False,

                "reason": (
                    "Wave invalidation level отсутствует."
                ),
            })

            continue

        invalidation_level = float(
            invalidation_level
        )

        action = (
            position.get(
                "action"
            )
        )

        breached = False

        trigger_price = None

        if (
            action
            == "enter_long"
        ):

            trigger_price = bid

            breached = (
                bid
                <= invalidation_level
            )

        elif (
            action
            == "enter_short"
        ):

            trigger_price = ask

            breached = (
                ask
                >= invalidation_level
            )

        results.append({

            "plan_id": (
                position.get(
                    "plan_id"
                )
            ),

            "position_ticket": (
                position.get(
                    "position_ticket"
                )
            ),

            "action": (
                action
            ),

            "invalidation_level": (
                invalidation_level
            ),

            "trigger_price": (
                trigger_price
            ),

            "bid": (
                bid
            ),

            "ask": (
                ask
            ),

            "breached": (
                breached
            ),

            # ------------------------------------------------
            # В ТЕКУЩЕЙ СТРАТЕГИИ:
            #
            # POSITION OPEN -> HOLD -> SL / TP
            # ------------------------------------------------

            "requires_reanalysis": False,

            "reason": (
                "Уровень wave invalidation достигнут, "
                "но открытая позиция по стратегии "
                "не переоценивается и живёт до SL/TP."
                if breached
                else
                "Wave invalidation level "
                "не достигнут."
            ),
        })

    return results


# ============================================================
# PRINT PLAN
# ============================================================

def print_plan(
    plan: dict,
):
    """
    Выводит Trade Plan.
    """

    print(
        f"Plan ID:       "
        f"{plan.get('plan_id')}"
    )

    print(
        f"Status:        "
        f"{plan.get('status')}"
    )

    print(
        f"Execution:     "
        f"{plan.get('execution_status')}"
    )

    print(
        f"Created:       "
        f"{plan.get('created_at_fp')}"
    )

    print(
        f"Analysis:      "
        f"{plan.get('source_analysis_time')}"
    )

    print(
        f"H1 closed bar: "
        f"{plan.get('source_h1_closed_bar_time')}"
    )

    print()

    print(
        f"Symbol:        "
        f"{plan.get('symbol')}"
    )

    print(
        f"Action:        "
        f"{plan.get('action')}"
    )

    print(
        f"Order type:    "
        f"{plan.get('order_type')}"
    )

    print(
        f"Confidence:    "
        f"{plan.get('confidence')}"
    )

    print(
        f"Setup:         "
        f"{plan.get('setup_type')}"
    )

    print(
        f"Market regime: "
        f"{plan.get('market_regime')}"
    )

    print(
        f"Current phase: "
        f"{plan.get('current_phase') or plan.get('market_phase')}"
    )

    print(
        f"Phase status:  "
        f"{plan.get('phase_status')}"
    )

    print(
        f"Setup quality: "
        f"{plan.get('setup_quality')}"
    )

    print(
        f"Entry quality: "
        f"{plan.get('entry_quality')}"
    )

    print()

    print(
        f"Entry:         "
        f"{plan.get('entry_price')}"
    )

    print(
        f"Stop Loss:     "
        f"{plan.get('stop_loss')}"
    )

    print(
        f"Take Profit:   "
        f"{plan.get('take_profit')}"
    )

    print(
        f"Setup invalid: "
        f"{plan.get('setup_invalidation_level', plan.get('wave_invalidation_level'))}"
    )

    print()

    print(
        f"Volume:        "
        f"{plan.get('volume')}"
    )

    print(
        f"Risk:          "
        f"{plan.get('expected_loss')}"
    )

    print(
        f"Potential:     "
        f"{plan.get('expected_profit')}"
    )

    print(
        f"R:R:           "
        f"{plan.get('risk_reward')}"
    )

    print()

    print(
        f"Confirmations: "
        f"{plan.get('confirmation_count')}"
    )

    print(
        f"Pending ticket:"
        f" {plan.get('pending_ticket')}"
    )

    print(
        f"Position:      "
        f"{plan.get('position_ticket')}"
    )


# ============================================================
# PRINT PENDING ACTION
# ============================================================

def print_pending_action(
    action: dict,
):
    """
    Выводит action.
    """

    print(
        f"Action ID:     "
        f"{action.get('action_id')}"
    )

    print(
        f"Type:          "
        f"{action.get('type')}"
    )

    print(
        f"Status:        "
        f"{action.get('status')}"
    )

    print(
        f"Plan ID:       "
        f"{action.get('plan_id')}"
    )

    print(
        f"Symbol:        "
        f"{action.get('symbol')}"
    )

    print(
        f"MT5 ticket:    "
        f"{action.get('ticket')}"
    )

    print(
        f"Reason:        "
        f"{action.get('reason')}"
    )


# ============================================================
# PRINT RESULT
# ============================================================

def print_trade_state_result(
    result: dict,
):
    """
    Выводит Trade State.
    """

    print()
    print("=" * 80)
    print(
        "TRADE STATE"
    )
    print("=" * 80)

    print(
        f"State action:  "
        f"{result['state_action']}"
    )

    print(
        f"Plan changed:  "
        f"{result['plan_changed']}"
    )

    print(
        f"Cancel old:    "
        f"{result['cancel_previous_order']}"
    )

    print(
        f"Managed pos:   "
        f"{result['managed_positions_count']}"
    )

    print(
        f"Pending actions:"
        f" {result['pending_actions_count']}"
    )

    # ========================================================
    # ACTIVE PLAN
    # ========================================================

    active_plan = (
        result.get(
            "active_plan"
        )
    )

    print()
    print(
        "АКТИВНЫЙ ENTRY PLAN"
    )
    print("-" * 80)

    if active_plan is None:

        print(
            "Активного entry-плана нет."
        )

    else:

        print_plan(
            active_plan
        )

    # ========================================================
    # MANAGED POSITIONS
    # ========================================================

    managed_positions = (
        result.get(
            "managed_positions",
            [],
        )
    )

    print()
    print(
        "MANAGED POSITIONS"
    )
    print("-" * 80)

    if not managed_positions:

        print(
            "Открытых управляемых позиций нет."
        )

    else:

        for index, position in enumerate(
            managed_positions,
            start=1,
        ):

            print(
                f"[{index}]"
            )

            print_plan(
                position
            )

            if (
                index
                < len(
                    managed_positions
                )
            ):

                print(
                    "-" * 40
                )

    # ========================================================
    # NEW ACTIONS
    # ========================================================

    new_actions = (
        result.get(
            "new_actions",
            [],
        )
    )

    print()
    print(
        "НОВЫЕ СЛУЖЕБНЫЕ ACTIONS"
    )
    print("-" * 80)

    if not new_actions:

        print(
            "Новых действий нет."
        )

    else:

        for index, action in enumerate(
            new_actions,
            start=1,
        ):

            print(
                f"[{index}]"
            )

            print_pending_action(
                action
            )

            if (
                index
                < len(
                    new_actions
                )
            ):

                print(
                    "-" * 40
                )

    print()
    print(
        f"State file:    "
        f"{result['state_path']}"
    )

    print("=" * 80)

# ============================================================
# REQUEST ACTIVE PENDING CANCELLATION
# ============================================================

def request_active_pending_cancellation(
    reason: str,
) -> dict:
    """
    Идемпотентно переводит активный pending-plan
    в CANCEL_REQUESTED и создаёт cancel_pending action.

    Сам MT5 order здесь НЕ удаляется.
    """

    state = load_trade_state()
    plan = state.get(
        "active_plan"
    )

    if plan is None:
        return {
            "requested": False,
            "reason": "active_plan отсутствует.",
            "action": None,
            "state_path": str(TRADE_STATE_PATH),
        }

    ticket = plan.get(
        "pending_ticket"
    )

    if ticket is None:
        raise RuntimeError(
            "Невозможно запросить отмену: "
            "у active_plan нет pending_ticket."
        )

    execution_status = plan.get(
        "execution_status"
    )

    if execution_status not in (
        EXECUTION_PENDING_SENT,
        EXECUTION_CANCEL_REQUESTED,
    ):
        raise RuntimeError(
            "Невозможно запросить отмену pending: "
            "execution_status="
            f"{execution_status}."
        )

    action, created = queue_cancel_pending_action(
        state=state,
        plan=plan,
        reason=reason,
    )

    current_time = now_fp().isoformat()

    plan[
        "status"
    ] = PLAN_STATUS_CANCEL_REQUESTED
    plan[
        "execution_status"
    ] = EXECUTION_CANCEL_REQUESTED
    plan[
        "requires_order_cancel"
    ] = True
    plan[
        "cancel_requested_at_fp"
    ] = (
        plan.get(
            "cancel_requested_at_fp"
        )
        or current_time
    )
    plan[
        "cancel_reason"
    ] = str(
        reason
    )
    plan[
        "updated_at_fp"
    ] = current_time

    state[
        "active_plan"
    ] = plan

    save_trade_state(
        state
    )

    return {
        "requested": True,
        "action_created": bool(
            created
        ),
        "action": copy.deepcopy(
            action
        ),
        "plan_id": plan.get(
            "plan_id"
        ),
        "ticket": int(
            ticket
        ),
        "requested_at_fp": current_time,
        "state_path": str(
            TRADE_STATE_PATH
        ),
    }


# ============================================================
# FINALIZE ACTIVE PENDING PLAN
# ============================================================

def finalize_active_pending_plan(
    final_status: str,
    reason: str,
    mt5_history_order: dict | None = None,
) -> dict:
    """
    Завершает active pending-plan после доказанного
    CANCELLED / EXPIRED / REJECTED состояния MT5.
    """

    state = load_trade_state()
    plan = state.get(
        "active_plan"
    )

    if plan is None:
        return {
            "finalized": False,
            "reason": "active_plan отсутствует.",
            "plan_id": None,
            "state_path": str(TRADE_STATE_PATH),
        }

    allowed = {
        PLAN_STATUS_CANCELLED: EXECUTION_CANCELLED,
        PLAN_STATUS_EXPIRED: EXECUTION_EXPIRED,
        PLAN_STATUS_EXECUTION_FAILED: EXECUTION_SEND_FAILED,
    }

    if final_status not in allowed:
        raise ValueError(
            "Неподдерживаемый final_status pending-plan: "
            f"{final_status}"
        )

    current_time = now_fp().isoformat()
    archived_plan = copy.deepcopy(
        plan
    )
    archived_plan[
        "status"
    ] = final_status
    archived_plan[
        "execution_status"
    ] = allowed[
        final_status
    ]
    archived_plan[
        "requires_order_cancel"
    ] = False
    archived_plan[
        "pending_finished_at_fp"
    ] = current_time
    archived_plan[
        "pending_finish_reason"
    ] = str(
        reason
    )

    if mt5_history_order is not None:
        archived_plan[
            "pending_history_order"
        ] = copy.deepcopy(
            mt5_history_order
        )

    archive_plan(
        state=state,
        plan=archived_plan,
        final_status=final_status,
        reason=reason,
    )

    state[
        "active_plan"
    ] = None

    save_trade_state(
        state
    )

    return {
        "finalized": True,
        "plan_id": archived_plan.get(
            "plan_id"
        ),
        "final_status": final_status,
        "finished_at_fp": current_time,
        "state_path": str(
            TRADE_STATE_PATH
        ),
    }
