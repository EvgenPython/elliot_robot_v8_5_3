from observability import observe, emit
from datetime import datetime

import MetaTrader5 as mt5

from risk_manager import (
    STARTING_ACCOUNT_SIZE,
    TARGET_RISK_PER_TRADE_PERCENT,
    MAX_MARKET_ENTRY_DEVIATION_PERCENT,
    get_account_info,
    get_positions,
    get_daily_state,
    calculate_fundingpips_limits,
    enrich_limits,
    calculate_open_positions_risk,
    calculate_position_size,
    calculate_trade_loss,
    calculate_trade_profit,
    calculate_required_margin,
    validate_order_prices,
    calculate_rr,
    now_fp,
)

from trade_state import (
    ACTION_CANCEL_PENDING,
    ACTION_MANAGE_POSITION,
    ACTION_REANALYZE,
    EXECUTION_NOT_SENT,
    get_active_plan,
    get_pending_actions,
    get_managed_positions,
    extract_latest_closed_h1_time,
    check_active_plan_price_invalidation,
    expire_active_plan,
)


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument

SYMBOL = active_instrument()


# ============================================================
# VALIDATION LAYER
# ============================================================

# Этот модуль только строит и проверяет MT5 request через order_check().
# Реальный order_send() намеренно расположен в live_executor.py и
# pending_executor.py, где вокруг него есть durable SEND_INTENT и broker
# reconciliation. Старое имя DRY_RUN вводило в заблуждение: оно не было
# глобальным запретом торговли, но в журналах выглядело именно так.


# ============================================================
# MT5 SETTINGS
# ============================================================

MAGIC_NUMBER = 26081001

COMMENT_PREFIX = "claude"

DEVIATION_POINTS = 50


# ============================================================
# MARKET PLAN TTL
# ============================================================

# Market-сигнал должен быть исполнен
# практически сразу после анализа.
#
# Если за 5 минут позиция не была открыта,
# market-plan считается мёртвым.
#
MAX_MARKET_PLAN_AGE_SECONDS = 300


# ============================================================
# ORDER CHECK RETCODES
# ============================================================

ORDER_CHECK_OK = 0

TRADE_RETCODE_TRADE_DISABLED = 10017

TRADE_RETCODE_SERVER_DISABLES_AT = 10026

TRADE_RETCODE_CLIENT_DISABLES_AT = 10027


# ============================================================
# PENDING ORDER TYPES
# ============================================================

PENDING_ORDER_TYPES = {

    mt5.ORDER_TYPE_BUY_LIMIT,

    mt5.ORDER_TYPE_SELL_LIMIT,

    mt5.ORDER_TYPE_BUY_STOP,

    mt5.ORDER_TYPE_SELL_STOP,
}


if hasattr(
    mt5,
    "ORDER_TYPE_BUY_STOP_LIMIT",
):

    PENDING_ORDER_TYPES.add(
        mt5.ORDER_TYPE_BUY_STOP_LIMIT
    )


if hasattr(
    mt5,
    "ORDER_TYPE_SELL_STOP_LIMIT",
):

    PENDING_ORDER_TYPES.add(
        mt5.ORDER_TYPE_SELL_STOP_LIMIT
    )


# ============================================================
# MT5 OBJECT -> DICT
# ============================================================

def mt5_object_to_dict(
    value,
):
    """
    Преобразует MT5 namedtuple в dict.
    """

    if value is None:
        return None

    if hasattr(
        value,
        "_asdict",
    ):

        result = (
            value._asdict()
        )

        for key, item in list(
            result.items()
        ):

            if hasattr(
                item,
                "_asdict",
            ):

                result[
                    key
                ] = item._asdict()

        return result

    return str(
        value
    )


# ============================================================
# PRICE
# ============================================================

def normalize_price(
    symbol: str,
    price: float,
) -> float:
    """
    Округляет цену до digits инструмента.
    """

    info = (
        mt5.symbol_info(
            symbol
        )
    )

    if info is None:

        raise RuntimeError(
            f"Не удалось получить "
            f"symbol_info({symbol})."
        )

    return round(
        float(
            price
        ),
        int(
            info.digits
        ),
    )


# ============================================================
# CURRENT TICK
# ============================================================

def get_current_tick(
    symbol: str,
):
    """
    Возвращает текущий tick.
    """

    tick = (
        mt5.symbol_info_tick(
            symbol
        )
    )

    if tick is None:

        raise RuntimeError(
            f"Не удалось получить tick "
            f"{symbol}. "
            f"MT5 error: {mt5.last_error()}"
        )

    return tick


# ============================================================
# DATETIME
# ============================================================

def parse_fp_datetime(
    value: str | None,
) -> datetime | None:
    """
    Разбирает ISO datetime.
    """

    if not value:
        return None

    try:

        return (
            datetime.fromisoformat(
                value
            )
        )

    except ValueError:

        return None


# ============================================================
# PLAN AGE
# ============================================================

def calculate_plan_age_seconds(
    plan: dict,
) -> float | None:
    """
    Возвращает возраст Trade Plan.
    """

    created_at = (
        parse_fp_datetime(
            plan.get(
                "created_at_fp"
            )
        )
    )

    if created_at is None:
        return None

    return max(
        0.0,
        (
            now_fp()
            - created_at
        ).total_seconds(),
    )


# ============================================================
# MARKET PLAN EXPIRATION
# ============================================================

def get_market_plan_expiration(
    plan: dict,
) -> dict:
    """
    Проверяет TTL market-plan.

    Pending orders здесь НЕ истекают.

    Они будут иметь отдельную
    политику срока жизни.
    """

    order_type = (
        plan.get(
            "order_type"
        )
    )

    if (
        order_type
        != "market"
    ):

        return {

            "applicable": False,

            "expired": False,

            "age_seconds": (
                calculate_plan_age_seconds(
                    plan
                )
            ),

            "max_age_seconds": (
                MAX_MARKET_PLAN_AGE_SECONDS
            ),

            "reason": (
                "TTL market-plan "
                "к этому типу ордера "
                "не применяется."
            ),
        }

    age_seconds = (
        calculate_plan_age_seconds(
            plan
        )
    )

    if age_seconds is None:

        return {

            "applicable": True,

            "expired": False,

            "age_seconds": None,

            "max_age_seconds": (
                MAX_MARKET_PLAN_AGE_SECONDS
            ),

            "reason": (
                "Не удалось определить "
                "возраст market-plan."
            ),
        }

    expired = (
        age_seconds
        > MAX_MARKET_PLAN_AGE_SECONDS
    )

    return {

        "applicable": True,

        "expired": (
            expired
        ),

        "age_seconds": (
            age_seconds
        ),

        "max_age_seconds": (
            MAX_MARKET_PLAN_AGE_SECONDS
        ),

        "reason": (
            "Market Trade Plan превысил "
            "допустимый срок жизни."
            if expired
            else
            "Market Trade Plan ещё актуален."
        ),
    }


# ============================================================
# H1 FRESHNESS
# ============================================================

def validate_plan_h1_bar(
    plan: dict,
    snapshot: dict,
    *,
    allow_prior_h1_for_pending: bool = False,
) -> list[str]:
    """
    Проверяет H1, на которой создан план.
    """

    errors = []

    plan_h1 = (
        plan.get(
            "source_h1_closed_bar_time"
        )
    )

    current_h1 = (
        extract_latest_closed_h1_time(
            snapshot
        )
    )

    if not plan_h1:

        errors.append(
            "Trade Plan не содержит "
            "source_h1_closed_bar_time."
        )

        return errors

    if not current_h1:

        errors.append(
            "Не удалось определить "
            "последнюю закрытую H1-свечу."
        )

        return errors

    if (
        str(
            plan_h1
        )
        !=
        str(
            current_h1
        )
    ):

        if (
            allow_prior_h1_for_pending
            and str(plan.get("order_type") or "") in {"limit", "stop"}
        ):
            # The pending executor has already applied its explicit bounded
            # multi-H1 TTL immediately before this shared validation. Market
            # orders never receive this exception.
            return errors

        errors.append(
            "Trade Plan относится "
            "к старой H1: "
            f"{plan_h1}; "
            f"текущая закрытая H1: "
            f"{current_h1}."
        )

    return errors


# ============================================================
# EXECUTION STATE
# ============================================================

def validate_execution_state(
    plan: dict,
) -> list[str]:
    """
    Защита от повторного исполнения.
    """

    errors = []

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

    if (
        execution_status
        != EXECUTION_NOT_SENT
    ):

        errors.append(
            "Trade Plan уже имеет "
            "execution_status="
            f"{execution_status}."
        )

    if pending_ticket is not None:

        errors.append(
            "Trade Plan уже связан "
            "с pending order "
            f"#{pending_ticket}."
        )

    if position_ticket is not None:

        errors.append(
            "Trade Plan уже связан "
            "с позицией "
            f"#{position_ticket}."
        )

    return errors


# ============================================================
# TERMINAL TRADE STATUS
# ============================================================

def get_terminal_trade_allowed():
    """
    Возвращает terminal.trade_allowed.
    """

    terminal = (
        mt5.terminal_info()
    )

    if terminal is None:
        return None

    return bool(
        terminal.trade_allowed
    )


# ============================================================
# GENERIC ORDER CHECK
# ============================================================

def perform_order_check(
    request: dict,
) -> dict:
    """
    Выполняет mt5.order_check().

    Никакой торговой операции
    не отправляет.
    """

    result = (
        mt5.order_check(
            request
        )
    )

    terminal_trade_allowed = (
        get_terminal_trade_allowed()
    )

    if result is None:

        return {

            "performed": True,

            "result_received": False,

            "passed": False,

            "safe_mode_block": False,

            "retcode": None,

            "comment": None,

            "terminal_trade_allowed": (
                terminal_trade_allowed
            ),

            "last_error": (
                mt5.last_error()
            ),

            "balance": None,

            "equity": None,

            "profit": None,

            "margin": None,

            "margin_free": None,

            "margin_level": None,

            "raw": None,
        }

    retcode = int(
        result.retcode
    )

    passed = (
        retcode
        == ORDER_CHECK_OK
    )

    safe_mode_block = (
        retcode
        in (
            TRADE_RETCODE_TRADE_DISABLED,
            TRADE_RETCODE_SERVER_DISABLES_AT,
            TRADE_RETCODE_CLIENT_DISABLES_AT,
        )
    )

    return {

        "performed": True,

        "result_received": True,

        "passed": (
            passed
        ),

        "safe_mode_block": (
            safe_mode_block
        ),

        "retcode": (
            retcode
        ),

        "comment": str(
            result.comment
        ),

        "terminal_trade_allowed": (
            terminal_trade_allowed
        ),

        "balance": float(
            result.balance
        ),

        "equity": float(
            result.equity
        ),

        "profit": float(
            result.profit
        ),

        "margin": float(
            result.margin
        ),

        "margin_free": float(
            result.margin_free
        ),

        "margin_level": float(
            result.margin_level
        ),

        "last_error": (
            mt5.last_error()
        ),

        "raw": (
            mt5_object_to_dict(
                result
            )
        ),
    }


# ============================================================
# GET ACTIVE ORDER
# ============================================================

def get_active_order_by_ticket(
    ticket: int,
):
    """
    Получает active pending order
    по ticket.
    """

    orders = (
        mt5.orders_get(
            ticket=int(
                ticket
            )
        )
    )

    if orders is None:

        raise RuntimeError(
            "Не удалось выполнить "
            f"orders_get(ticket={ticket}). "
            f"MT5 error: {mt5.last_error()}"
        )

    if len(
        orders
    ) == 0:

        return None

    return orders[
        0
    ]


# ============================================================
# VALIDATE CANCEL TARGET
# ============================================================

def validate_cancel_target(
    action: dict,
    order,
) -> tuple[list[str], list[str]]:
    """
    Проверяет, что pending order
    принадлежит нашему роботу.
    """

    errors = []

    warnings = []

    expected_ticket = int(
        action[
            "ticket"
        ]
    )

    expected_symbol = str(
        action.get(
            "symbol",
            "",
        )
    )

    actual_ticket = int(
        order.ticket
    )

    actual_symbol = str(
        order.symbol
    )

    actual_magic = int(
        order.magic
    )

    actual_type = int(
        order.type
    )

    actual_comment = str(
        order.comment
    )

    if (
        actual_ticket
        != expected_ticket
    ):

        errors.append(
            "MT5 ticket не совпадает "
            "с Cancel Action."
        )

    if (
        expected_symbol
        and
        actual_symbol
        != expected_symbol
    ):

        errors.append(
            "Symbol pending order "
            "не совпадает с Trade State: "
            f"{actual_symbol} != "
            f"{expected_symbol}."
        )

    if (
        actual_magic
        != MAGIC_NUMBER
    ):

        errors.append(
            "Pending order имеет "
            "чужой Magic Number: "
            f"{actual_magic} != "
            f"{MAGIC_NUMBER}."
        )

    if (
        actual_type
        not in PENDING_ORDER_TYPES
    ):

        errors.append(
            "Ticket не является "
            "поддерживаемым pending order. "
            f"type={actual_type}."
        )

    if (
        actual_comment
        and
        not actual_comment.startswith(
            COMMENT_PREFIX
        )
    ):

        warnings.append(
            "Комментарий MT5-order "
            "не начинается с "
            f"'{COMMENT_PREFIX}'."
        )

    return (
        errors,
        warnings,
    )


# ============================================================
# CANCEL REQUEST
# ============================================================

def build_cancel_pending_request(
    action: dict,
) -> dict:
    """
    Формирует TRADE_ACTION_REMOVE.
    """

    ticket = int(
        action[
            "ticket"
        ]
    )

    action_id = str(
        action.get(
            "action_id",
            "",
        )
    )

    return {

        "action": (
            mt5.TRADE_ACTION_REMOVE
        ),

        "order": (
            ticket
        ),

        "comment": (
            f"cancel_{action_id[:8]}"
        ),
    }


# ============================================================
# CANCEL ACTION DRY RUN
# ============================================================

def inspect_cancel_pending_action(
    action: dict,
) -> dict:
    """
    DRY RUN отмены pending-order.
    """

    errors = []

    warnings = []

    ticket = (
        action.get(
            "ticket"
        )
    )

    if ticket is None:

        return {

            "action_id": (
                action.get(
                    "action_id"
                )
            ),

            "type": (
                action.get(
                    "type"
                )
            ),

            "ticket": None,

            "plan_id": (
                action.get(
                    "plan_id"
                )
            ),

            "symbol": (
                action.get(
                    "symbol"
                )
            ),

            "order_found": False,

            "ownership_verified": False,

            "blocks_new_entry": True,

            "decision": (
                "INVALID_CANCEL_ACTION"
            ),

            "errors": [
                "Cancel Action не содержит ticket."
            ],

            "warnings": [],

            "order": None,

            "request": None,

            "order_check": None,
        }

    ticket = int(
        ticket
    )

    order = (
        get_active_order_by_ticket(
            ticket
        )
    )

    # ========================================================
    # ORDER НЕ ACTIVE
    # ========================================================

    if order is None:

        warnings.append(
            "Pending order "
            f"#{ticket} не найден "
            "среди active MT5 orders."
        )

        warnings.append(
            "Он мог быть отменён, истечь "
            "или исполниться. "
            "Требуется reconciliation."
        )

        return {

            "action_id": (
                action.get(
                    "action_id"
                )
            ),

            "type": (
                action.get(
                    "type"
                )
            ),

            "ticket": (
                ticket
            ),

            "plan_id": (
                action.get(
                    "plan_id"
                )
            ),

            "symbol": (
                action.get(
                    "symbol"
                )
            ),

            "order_found": False,

            "ownership_verified": False,

            "blocks_new_entry": True,

            "decision": (
                "ORDER_NOT_ACTIVE_RECONCILIATION_REQUIRED"
            ),

            "errors": [],

            "warnings": (
                warnings
            ),

            "order": None,

            "request": None,

            "order_check": None,
        }

    # ========================================================
    # OWNERSHIP
    # ========================================================

    ownership_errors, ownership_warnings = (
        validate_cancel_target(
            action=action,
            order=order,
        )
    )

    errors.extend(
        ownership_errors
    )

    warnings.extend(
        ownership_warnings
    )

    ownership_verified = (
        len(
            ownership_errors
        )
        == 0
    )

    if not ownership_verified:

        return {

            "action_id": (
                action.get(
                    "action_id"
                )
            ),

            "type": (
                action.get(
                    "type"
                )
            ),

            "ticket": (
                ticket
            ),

            "plan_id": (
                action.get(
                    "plan_id"
                )
            ),

            "symbol": (
                action.get(
                    "symbol"
                )
            ),

            "order_found": True,

            "ownership_verified": False,

            "blocks_new_entry": True,

            "decision": (
                "OWNERSHIP_CHECK_FAILED"
            ),

            "errors": (
                errors
            ),

            "warnings": (
                warnings
            ),

            "order": (
                mt5_object_to_dict(
                    order
                )
            ),

            "request": None,

            "order_check": None,
        }

    # ========================================================
    # REQUEST + CHECK
    # ========================================================

    request = (
        build_cancel_pending_request(
            action
        )
    )

    order_check = (
        perform_order_check(
            request
        )
    )

    if order_check[
        "passed"
    ]:

        decision = (
            "CANCEL_CHECK_PASSED"
        )

    elif order_check[
        "safe_mode_block"
    ]:

        decision = (
            "CANCEL_SAFE_MODE_BLOCKED"
        )

        warnings.append(
            "MT5 не разрешил торговую "
            "операцию отмены."
        )

    else:

        decision = (
            "CANCEL_CHECK_FAILED"
        )

        errors.append(
            "order_check() не подтвердил "
            "удаление pending order. "
            f"Retcode={order_check['retcode']}, "
            f"comment={order_check['comment']}."
        )

    return {

        "action_id": (
            action.get(
                "action_id"
            )
        ),

        "type": (
            action.get(
                "type"
            )
        ),

        "ticket": (
            ticket
        ),

        "plan_id": (
            action.get(
                "plan_id"
            )
        ),

        "symbol": (
            action.get(
                "symbol"
            )
        ),

        "order_found": True,

        "ownership_verified": True,

        # Реальной отмены ещё нет.
        "blocks_new_entry": True,

        "decision": (
            decision
        ),

        "errors": (
            errors
        ),

        "warnings": (
            warnings
        ),

        "order": (
            mt5_object_to_dict(
                order
            )
        ),

        "request": (
            request
        ),

        "order_check": (
            order_check
        ),
    }


# ============================================================
# NON-CANCEL ACTION
# ============================================================

def inspect_non_cancel_action(
    action: dict,
) -> dict:
    """
    Неизвестные/неподдерживаемые actions
    блокируют новый entry.
    """

    action_type = (
        action.get(
            "type"
        )
    )

    if (
        action_type
        == ACTION_MANAGE_POSITION
    ):

        decision = (
            "POSITION_MANAGEMENT_REQUIRED"
        )

        message = (
            "Есть необработанный action "
            "управления позицией."
        )

    elif (
        action_type
        == ACTION_REANALYZE
    ):

        decision = (
            "REANALYSIS_REQUIRED"
        )

        message = (
            "Есть необработанный "
            "reanalyse action."
        )

    else:

        decision = (
            "UNKNOWN_PENDING_ACTION"
        )

        message = (
            "Неизвестный pending action: "
            f"{action_type}."
        )

    return {

        "action_id": (
            action.get(
                "action_id"
            )
        ),

        "type": (
            action_type
        ),

        "ticket": (
            action.get(
                "ticket"
            )
        ),

        "plan_id": (
            action.get(
                "plan_id"
            )
        ),

        "symbol": (
            action.get(
                "symbol"
            )
        ),

        "order_found": None,

        "ownership_verified": None,

        "blocks_new_entry": True,

        "decision": (
            decision
        ),

        "errors": [
            message
        ],

        "warnings": [],

        "order": None,

        "request": None,

        "order_check": None,
    }


# ============================================================
# PROCESS PENDING ACTIONS
# ============================================================

def process_pending_actions_dry_run() -> dict:
    """
    Pending actions всегда обрабатываются
    раньше нового входа.
    """

    actions = (
        get_pending_actions()
    )

    reports = []

    errors = []

    warnings = []

    blocking_actions = 0

    for action in actions:

        action_type = (
            action.get(
                "type"
            )
        )

        if (
            action_type
            == ACTION_CANCEL_PENDING
        ):

            report = (
                inspect_cancel_pending_action(
                    action
                )
            )

        else:

            report = (
                inspect_non_cancel_action(
                    action
                )
            )

        reports.append(
            report
        )

        if report[
            "blocks_new_entry"
        ]:

            blocking_actions += 1

        for error in report[
            "errors"
        ]:

            errors.append(
                f"Action "
                f"{report['action_id']}: "
                f"{error}"
            )

        for warning in report[
            "warnings"
        ]:

            warnings.append(
                f"Action "
                f"{report['action_id']}: "
                f"{warning}"
            )

    return {

        "pending_count": (
            len(
                actions
            )
        ),

        "blocking_count": (
            blocking_actions
        ),

        "blocks_new_entry": (
            blocking_actions
            > 0
        ),

        "errors": (
            errors
        ),

        "warnings": (
            warnings
        ),

        "actions": (
            reports
        ),
    }


# ============================================================
# CURRENT EXPOSURE
# ============================================================

def check_existing_symbol_exposure(
    symbol: str,
) -> dict:
    """
    Любая существующая позиция или
    pending XAUUSD блокирует новый entry.
    """

    positions = (
        mt5.positions_get(
            symbol=symbol
        )
    )

    if positions is None:

        raise RuntimeError(
            "Не удалось получить позиции "
            f"{symbol}. "
            f"MT5 error: {mt5.last_error()}"
        )

    orders = (
        mt5.orders_get(
            symbol=symbol
        )
    )

    if orders is None:

        raise RuntimeError(
            "Не удалось получить orders "
            f"{symbol}. "
            f"MT5 error: {mt5.last_error()}"
        )

    return {

        "positions_count": (
            len(
                positions
            )
        ),

        "orders_count": (
            len(
                orders
            )
        ),

        "position_tickets": [
            int(
                position.ticket
            )
            for position
            in positions
        ],

        "order_tickets": [
            int(
                order.ticket
            )
            for order
            in orders
        ],

        "positions": [
            mt5_object_to_dict(
                position
            )
            for position
            in positions
        ],

        "orders": [
            mt5_object_to_dict(
                order
            )
            for order
            in orders
        ],
    }


# ============================================================
# MANAGED POSITION CONTEXT
# ============================================================

def get_managed_position_context() -> dict:
    """
    Managed positions из Trade State.
    """

    managed = (
        get_managed_positions()
    )

    return {

        "count": (
            len(
                managed
            )
        ),

        "positions": (
            managed
        ),
    }


# ============================================================
# LIVE ENTRY
# ============================================================

def determine_execution_entry(
    plan: dict,
    tick,
) -> dict:
    """
    MARKET LONG  -> Ask
    MARKET SHORT -> Bid

    LIMIT / STOP -> плановая Entry
    """

    action = (
        plan[
            "action"
        ]
    )

    order_type = (
        plan[
            "order_type"
        ]
    )

    planned_entry = float(
        plan[
            "entry_price"
        ]
    )

    bid = float(
        tick.bid
    )

    ask = float(
        tick.ask
    )

    if (
        order_type
        == "market"
    ):

        if (
            action
            == "enter_long"
        ):

            execution_entry = ask

        elif (
            action
            == "enter_short"
        ):

            execution_entry = bid

        else:

            raise ValueError(
                "Неизвестный action: "
                f"{action}"
            )

    elif (
        order_type
        in (
            "limit",
            "stop",
        )
    ):

        execution_entry = (
            planned_entry
        )

    else:

        raise ValueError(
            "Неизвестный order_type: "
            f"{order_type}"
        )

    return {

        "planned_entry": (
            planned_entry
        ),

        "execution_entry": (
            execution_entry
        ),

        "bid": (
            bid
        ),

        "ask": (
            ask
        ),
    }


# ============================================================
# MARKET DEVIATION
# ============================================================

def calculate_market_deviation_percent(
    planned_entry: float,
    live_entry: float,
) -> float:
    """
    Процент отклонения от Entry Claude.
    """

    if (
        planned_entry
        <= 0
    ):

        raise ValueError(
            "planned_entry должен "
            "быть больше нуля."
        )

    return (
        abs(
            live_entry
            - planned_entry
        )
        /
        planned_entry
        *
        100.0
    )


# ============================================================
# MARKET VALIDATION
# ============================================================

def validate_market_plan(
    plan: dict,
    planned_entry: float,
    live_entry: float,
) -> list[str]:
    """
    Проверяет market price deviation.

    TTL здесь уже НЕ проверяется:
    истечение market-plan теперь
    обрабатывается отдельно и изменяет
    Trade State.
    """

    errors = []

    if (
        plan.get(
            "order_type"
        )
        != "market"
    ):

        return errors

    deviation_percent = (
        calculate_market_deviation_percent(
            planned_entry=(
                planned_entry
            ),
            live_entry=(
                live_entry
            ),
        )
    )

    if (
        deviation_percent
        >
        MAX_MARKET_ENTRY_DEVIATION_PERCENT
    ):

        errors.append(
            "Цена ушла слишком далеко "
            "от Entry Claude: "
            f"{deviation_percent:.5f}% > "
            f"{MAX_MARKET_ENTRY_DEVIATION_PERCENT:.5f}%."
        )

    return errors


# ============================================================
# EXECUTION RISK CONTEXT
# ============================================================

def calculate_execution_risk_context() -> dict:
    """
    Пересчитывает состояние счёта
    непосредственно перед исполнением.
    """

    account = (
        get_account_info()
    )

    positions = (
        get_positions()
    )

    daily_state = (
        get_daily_state(
            account,
            positions,
        )
    )

    limits = (
        calculate_fundingpips_limits(
            daily_state
        )
    )

    open_risk = (
        calculate_open_positions_risk()
    )

    balance = float(
        account.balance
    )

    equity = float(
        account.equity
    )

    free_margin = float(
        account.margin_free
    )

    existing_positions_risk = float(
        open_risk[
            "total_remaining_risk"
        ]
    )

    limits = (
        enrich_limits(
            limits=limits,
            balance=balance,
            equity=equity,
            existing_positions_risk=(
                existing_positions_risk
            ),
        )
    )

    internal_risk_limit = (
        STARTING_ACCOUNT_SIZE
        *
        TARGET_RISK_PER_TRADE_PERCENT
        /
        100.0
    )

    available_prop_risk = min(
        limits[
            "daily_available_for_new_trade"
        ],
        limits[
            "max_available_for_new_trade"
        ],
    )

    available_prop_risk = max(
        0.0,
        available_prop_risk,
    )

    risk_budget = min(
        internal_risk_limit,
        available_prop_risk,
    )

    return {

        "account": {

            "balance": (
                balance
            ),

            "equity": (
                equity
            ),

            "free_margin": (
                free_margin
            ),
        },

        "daily_state": (
            daily_state
        ),

        "limits": (
            limits
        ),

        "open_risk": (
            open_risk
        ),

        "existing_positions_risk": (
            existing_positions_risk
        ),

        "internal_risk_limit": (
            internal_risk_limit
        ),

        "available_prop_risk": (
            available_prop_risk
        ),

        "risk_budget": (
            risk_budget
        ),
    }


# ============================================================
# RISK VALIDATION
# ============================================================

def validate_execution_risk_context(
    context: dict,
) -> list[str]:
    """
    Проверяет risk context.
    """

    errors = []

    daily_state = (
        context[
            "daily_state"
        ]
    )

    limits = (
        context[
            "limits"
        ]
    )

    open_risk = (
        context[
            "open_risk"
        ]
    )

    if not bool(
        daily_state.get(
            "trusted",
            False,
        )
    ):

        errors.append(
            "FundingPips daily baseline "
            "не является trusted."
        )

    if not bool(
        open_risk.get(
            "risk_known",
            False,
        )
    ):

        errors.append(
            "Невозможно определить риск "
            "существующих позиций."
        )

    if (
        context[
            "risk_budget"
        ]
        <= 0
    ):

        errors.append(
            "Нет доступного risk budget."
        )

    safety_buffer = float(
        limits[
            "safety_buffer_money"
        ]
    )

    if (
        limits[
            "current_daily_room"
        ]
        <= safety_buffer
    ):

        errors.append(
            "Счёт слишком близко "
            "к Daily Loss limit."
        )

    if (
        limits[
            "current_max_room"
        ]
        <= safety_buffer
    ):

        errors.append(
            "Счёт слишком близко "
            "к Max Loss limit."
        )

    return errors


# ============================================================
# RECALCULATE VOLUME
# ============================================================

def recalculate_execution_volume(
    plan: dict,
    symbol: str,
    entry_price: float,
    stop_loss: float,
    risk_budget: float,
) -> dict:
    """
    Executor может уменьшить lot,
    но не увеличить относительно
    уже разрешённого Risk Manager.
    """

    plan_volume = float(
        plan[
            "volume"
        ]
    )

    sizing = (
        calculate_position_size(
            symbol=symbol,
            action=plan[
                "action"
            ],
            entry_price=entry_price,
            stop_loss=stop_loss,
            risk_money=risk_budget,
        )
    )

    if not sizing[
        "possible"
    ]:

        return {

            **sizing,

            "plan_volume": (
                plan_volume
            ),

            "safe_calculated_volume": 0.0,

            "final_volume": 0.0,
        }

    calculated_volume = float(
        sizing[
            "volume"
        ]
    )

    final_volume = min(
        plan_volume,
        calculated_volume,
    )

    info = (
        mt5.symbol_info(
            symbol
        )
    )

    if info is None:

        raise RuntimeError(
            f"Нет symbol_info({symbol})."
        )

    volume_min = float(
        info.volume_min
    )

    if (
        final_volume
        < volume_min
    ):

        return {

            **sizing,

            "possible": False,

            "reason": (
                "После повторного расчёта "
                "безопасный объём меньше "
                "минимального lot."
            ),

            "plan_volume": (
                plan_volume
            ),

            "safe_calculated_volume": (
                calculated_volume
            ),

            "final_volume": 0.0,
        }

    expected_loss = (
        calculate_trade_loss(
            symbol=symbol,
            action=plan[
                "action"
            ],
            volume=final_volume,
            entry_price=entry_price,
            stop_loss=stop_loss,
        )
    )

    return {

        **sizing,

        "possible": True,

        "reason": None,

        "plan_volume": (
            plan_volume
        ),

        "safe_calculated_volume": (
            calculated_volume
        ),

        "final_volume": (
            final_volume
        ),

        "expected_loss": (
            expected_loss
        ),
    }


# ============================================================
# MT5 ORDER TYPE
# ============================================================

def determine_mt5_order_type(
    action: str,
    order_type: str,
):
    """
    Наш action -> MT5 ENUM_ORDER_TYPE.
    """

    if (
        action
        == "enter_long"
        and
        order_type
        == "market"
    ):

        return (
            mt5.ORDER_TYPE_BUY
        )

    if (
        action
        == "enter_short"
        and
        order_type
        == "market"
    ):

        return (
            mt5.ORDER_TYPE_SELL
        )

    if (
        action
        == "enter_long"
        and
        order_type
        == "limit"
    ):

        return (
            mt5.ORDER_TYPE_BUY_LIMIT
        )

    if (
        action
        == "enter_short"
        and
        order_type
        == "limit"
    ):

        return (
            mt5.ORDER_TYPE_SELL_LIMIT
        )

    if (
        action
        == "enter_long"
        and
        order_type
        == "stop"
    ):

        return (
            mt5.ORDER_TYPE_BUY_STOP
        )

    if (
        action
        == "enter_short"
        and
        order_type
        == "stop"
    ):

        return (
            mt5.ORDER_TYPE_SELL_STOP
        )

    raise ValueError(
        "Не удалось определить "
        "MT5 order type: "
        f"action={action}, "
        f"order_type={order_type}"
    )


# ============================================================
# FILLING
# ============================================================

def determine_market_filling_mode(
    symbol: str,
):
    """
    Filling policy market order.
    """

    info = (
        mt5.symbol_info(
            symbol
        )
    )

    if info is None:

        raise RuntimeError(
            f"Нет symbol_info({symbol})."
        )

    trade_exemode = int(
        info.trade_exemode
    )

    filling_mode = int(
        info.filling_mode
    )

    market_execution = getattr(
        mt5,
        "SYMBOL_TRADE_EXECUTION_MARKET",
        2,
    )

    if (
        trade_exemode
        != market_execution
    ):

        return (
            mt5.ORDER_FILLING_RETURN
        )

    symbol_filling_fok = getattr(
        mt5,
        "SYMBOL_FILLING_FOK",
        1,
    )

    symbol_filling_ioc = getattr(
        mt5,
        "SYMBOL_FILLING_IOC",
        2,
    )

    if (
        filling_mode
        &
        symbol_filling_ioc
    ):

        return (
            mt5.ORDER_FILLING_IOC
        )

    if (
        filling_mode
        &
        symbol_filling_fok
    ):

        return (
            mt5.ORDER_FILLING_FOK
        )

    raise RuntimeError(
        "Для Market Execution "
        "не удалось определить filling mode."
    )


def determine_filling_mode(
    symbol: str,
    order_type: str,
):
    """
    Filling policy.
    """

    if (
        order_type
        in (
            "limit",
            "stop",
        )
    ):

        return (
            mt5.ORDER_FILLING_RETURN
        )

    if (
        order_type
        == "market"
    ):

        return (
            determine_market_filling_mode(
                symbol
            )
        )

    raise ValueError(
        "Неизвестный order_type: "
        f"{order_type}"
    )


# ============================================================
# TRADE ACTION
# ============================================================

def determine_trade_action(
    order_type: str,
):
    """
    Market -> DEAL
    Pending -> PENDING
    """

    if (
        order_type
        == "market"
    ):

        return (
            mt5.TRADE_ACTION_DEAL
        )

    if (
        order_type
        in (
            "limit",
            "stop",
        )
    ):

        return (
            mt5.TRADE_ACTION_PENDING
        )

    raise ValueError(
        "Неизвестный order_type: "
        f"{order_type}"
    )


# ============================================================
# COMMENT
# ============================================================

def build_order_comment(
    plan: dict,
) -> str:
    """
    Связывает MT5 order с Plan ID.
    """

    plan_id = str(
        plan[
            "plan_id"
        ]
    )

    return (
        f"{COMMENT_PREFIX}_"
        f"{plan_id[:8]}"
    )


# ============================================================
# BUILD ENTRY REQUEST
# ============================================================

def build_mt5_request(
    plan: dict,
    symbol: str,
    volume: float,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
) -> dict:
    """
    Формирует будущий entry request.
    """

    order_type_name = (
        plan[
            "order_type"
        ]
    )

    mt5_order_type = (
        determine_mt5_order_type(
            action=plan[
                "action"
            ],
            order_type=(
                order_type_name
            ),
        )
    )

    trade_action = (
        determine_trade_action(
            order_type_name
        )
    )

    filling_mode = (
        determine_filling_mode(
            symbol=symbol,
            order_type=(
                order_type_name
            ),
        )
    )

    return {

        "action": (
            trade_action
        ),

        "magic": (
            MAGIC_NUMBER
        ),

        "symbol": (
            symbol
        ),

        "volume": float(
            volume
        ),

        "type": (
            mt5_order_type
        ),

        "price": (
            normalize_price(
                symbol,
                entry_price,
            )
        ),

        "sl": (
            normalize_price(
                symbol,
                stop_loss,
            )
        ),

        "tp": (
            normalize_price(
                symbol,
                take_profit,
            )
        ),

        "deviation": (
            DEVIATION_POINTS
        ),

        "type_time": (
            mt5.ORDER_TIME_GTC
        ),

        "type_filling": (
            filling_mode
        ),

        "comment": (
            build_order_comment(
                plan
            )
        ),
    }


# ============================================================
# PLAN SUMMARY
# ============================================================

def build_plan_summary(
    plan: dict | None,
) -> dict | None:
    """
    Формирует компактное представление плана.
    """

    if plan is None:
        return None

    return {

        "plan_id": (
            plan.get(
                "plan_id"
            )
        ),

        "execution_status": (
            plan.get(
                "execution_status"
            )
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

        "source_analysis_time": (
            plan.get(
                "source_analysis_time"
            )
        ),

        "source_h1_closed_bar_time": (
            plan.get(
                "source_h1_closed_bar_time"
            )
        ),

        "age_seconds": (
            calculate_plan_age_seconds(
                plan
            )
        ),
    }


# ============================================================
# EARLY REPORT
# ============================================================

def build_early_report(
    decision: str,
    errors: list[str],
    warnings: list[str],
    pending_actions_report: dict,
    plan: dict | None = None,
    expiration: dict | None = None,
) -> dict:
    """
    Формирует отчёт при раннем STOP.
    """

    return {

        "dry_run": True,

        "ready_for_send": False,

        "decision": (
            decision
        ),

        "fp_time": (
            now_fp().isoformat()
        ),

        "errors": (
            errors
        ),

        "warnings": (
            warnings
        ),

        "pending_actions": (
            pending_actions_report
        ),

        "managed_positions": (
            get_managed_position_context()
        ),

        "plan": (
            build_plan_summary(
                plan
            )
        ),

        "expiration": (
            expiration
        ),

        "market": None,

        "levels": None,

        "risk": None,

        "exposure": None,

        "invalidation_check": None,

        "request": None,

        "order_check": None,
    }


# ============================================================
# MAIN REQUEST VALIDATION
# ============================================================

@observe("execution")
def validate_active_plan(
    snapshot: dict,
    symbol: str = SYMBOL,
    *,
    allow_prior_h1_for_pending: bool = False,
) -> dict:
    """
    Валидирует активный план и формирует request для live executor.

    ПОРЯДОК:

        1. pending_actions
        2. active_plan
        3. market-plan expiration
        4. execution state
        5. H1 freshness
        6. wave invalidation
        7. MT5 exposure
        8. live price
        9. risk
       10. lot
       11. request
       12. order_check
       13. вернуть request вызывающему live executor
    """

    current_time = (
        now_fp()
    )

    errors = []

    warnings = []

    # ========================================================
    # 1. PENDING ACTIONS
    # ========================================================

    pending_actions_report = (
        process_pending_actions_dry_run()
    )

    warnings.extend(
        pending_actions_report[
            "warnings"
        ]
    )

    if (
        pending_actions_report[
            "blocks_new_entry"
        ]
    ):

        return (
            build_early_report(
                decision=(
                    "PENDING_ACTIONS_REQUIRED"
                ),
                errors=[
                    "Перед новым входом "
                    "нужно завершить "
                    "pending_actions."
                ],
                warnings=warnings,
                pending_actions_report=(
                    pending_actions_report
                ),
                plan=(
                    get_active_plan()
                ),
            )
        )

    # ========================================================
    # 2. ACTIVE PLAN
    # ========================================================

    plan = (
        get_active_plan()
    )

    if plan is None:

        return (
            build_early_report(
                decision=(
                    "NO_ACTIVE_PLAN"
                ),
                errors=[
                    "Активного Trade Plan нет."
                ],
                warnings=warnings,
                pending_actions_report=(
                    pending_actions_report
                ),
                plan=None,
            )
        )

    # ========================================================
    # 3. MARKET PLAN EXPIRATION
    # ========================================================

    expiration_check = (
        get_market_plan_expiration(
            plan
        )
    )

    if (
        expiration_check[
            "applicable"
        ]
        and
        expiration_check[
            "age_seconds"
        ]
        is None
    ):

        return (
            build_early_report(
                decision=(
                    "REJECTED"
                ),
                errors=[
                    "Не удалось определить возраст "
                    "market Trade Plan."
                ],
                warnings=warnings,
                pending_actions_report=(
                    pending_actions_report
                ),
                plan=plan,
                expiration=(
                    expiration_check
                ),
            )
        )

    if (
        expiration_check[
            "expired"
        ]
    ):

        age_seconds = float(
            expiration_check[
                "age_seconds"
            ]
        )

        expiration_reason = (
            "Market Trade Plan не был исполнен "
            f"за {age_seconds:.1f} секунд. "
            "Максимальный срок жизни "
            f"{MAX_MARKET_PLAN_AGE_SECONDS} секунд. "
            "План автоматически переведён "
            "в EXPIRED."
        )

        expire_result = (
            expire_active_plan(
                reason=(
                    expiration_reason
                ),
                expected_plan_id=(
                    plan.get(
                        "plan_id"
                    )
                ),
            )
        )

        expiration_report = {

            **expiration_check,

            "state_updated": (
                expire_result[
                    "expired"
                ]
            ),

            "expired_plan_id": (
                expire_result[
                    "plan_id"
                ]
            ),

            "expired_at_fp": (
                expire_result.get(
                    "expired_at_fp"
                )
            ),

            "state_path": (
                expire_result[
                    "state_path"
                ]
            ),
        }

        return (
            build_early_report(
                decision=(
                    "PLAN_EXPIRED"
                ),
                errors=[],
                warnings=[
                    expiration_reason
                ],
                pending_actions_report=(
                    pending_actions_report
                ),
                # Показываем старый план
                # только как контекст отчёта.
                plan=plan,
                expiration=(
                    expiration_report
                ),
            )
        )

    # ========================================================
    # 4. SYMBOL
    # ========================================================

    plan_symbol = str(
        plan.get(
            "symbol",
            symbol,
        )
    )

    if (
        plan_symbol
        != symbol
    ):

        errors.append(
            "Symbol Trade Plan "
            "не совпадает: "
            f"{plan_symbol} != {symbol}."
        )

    symbol = (
        plan_symbol
    )

    # ========================================================
    # 5. EXECUTION STATE
    # ========================================================

    errors.extend(
        validate_execution_state(
            plan
        )
    )

    # ========================================================
    # 6. H1 FRESHNESS
    # ========================================================

    errors.extend(
        validate_plan_h1_bar(
            plan=plan,
            snapshot=snapshot,
            allow_prior_h1_for_pending=allow_prior_h1_for_pending,
        )
    )

    # ========================================================
    # 7. INVALIDATION
    # ========================================================

    invalidation_check = (
        check_active_plan_price_invalidation(
            symbol=symbol
        )
    )

    if (
        invalidation_check.get(
            "requires_reanalysis",
            False,
        )
    ):

        errors.append(
            "Цена достигла "
            "wave_invalidation_level. "
            "Entry запрещён."
        )

    # ========================================================
    # 8. MT5 EXPOSURE
    # ========================================================

    exposure = (
        check_existing_symbol_exposure(
            symbol
        )
    )

    if (
        exposure[
            "positions_count"
        ]
        > 0
    ):

        errors.append(
            "По XAUUSD уже существует "
            f"{exposure['positions_count']} "
            "открытая позиция."
        )

    if (
        exposure[
            "orders_count"
        ]
        > 0
    ):

        errors.append(
            "По XAUUSD уже существует "
            f"{exposure['orders_count']} "
            "active pending order."
        )

    # ========================================================
    # 9. LIVE TICK
    # ========================================================

    tick = (
        get_current_tick(
            symbol
        )
    )

    entry_context = (
        determine_execution_entry(
            plan=plan,
            tick=tick,
        )
    )

    planned_entry = float(
        entry_context[
            "planned_entry"
        ]
    )

    execution_entry = float(
        entry_context[
            "execution_entry"
        ]
    )

    # ========================================================
    # 10. MARKET VALIDATION
    # ========================================================

    errors.extend(
        validate_market_plan(
            plan=plan,
            planned_entry=(
                planned_entry
            ),
            live_entry=(
                execution_entry
            ),
        )
    )

    # ========================================================
    # 11. LEVELS
    # ========================================================

    stop_loss = float(
        plan[
            "stop_loss"
        ]
    )

    take_profit = float(
        plan[
            "take_profit"
        ]
    )

    errors.extend(
        validate_order_prices(
            symbol=symbol,
            action=plan[
                "action"
            ],
            order_type=plan[
                "order_type"
            ],
            entry=execution_entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
    )

    # ========================================================
    # 12. RISK CONTEXT
    # ========================================================

    risk_context = (
        calculate_execution_risk_context()
    )

    errors.extend(
        validate_execution_risk_context(
            risk_context
        )
    )

    risk_budget = float(
        risk_context[
            "risk_budget"
        ]
    )

    sizing = None

    final_volume = 0.0

    expected_loss = None

    expected_profit = None

    required_margin = None

    rr = None

    projected_equity_all_stops = None

    projected_balance_all_stops = None

    # ========================================================
    # 13. LOT
    # ========================================================

    if (
        risk_budget
        > 0
        and
        len(
            errors
        )
        == 0
    ):

        try:

            sizing = (
                recalculate_execution_volume(
                    plan=plan,
                    symbol=symbol,
                    entry_price=(
                        execution_entry
                    ),
                    stop_loss=(
                        stop_loss
                    ),
                    risk_budget=(
                        risk_budget
                    ),
                )
            )

        except Exception as error:

            errors.append(
                "Не удалось повторно "
                "рассчитать lot: "
                f"{error}"
            )

    # ========================================================
    # 14. SIZING RESULT
    # ========================================================

    if sizing is not None:

        if not sizing[
            "possible"
        ]:

            errors.append(
                str(
                    sizing.get(
                        "reason",
                        "Lot calculation rejected.",
                    )
                )
            )

        else:

            final_volume = float(
                sizing[
                    "final_volume"
                ]
            )

            expected_loss = (
                calculate_trade_loss(
                    symbol=symbol,
                    action=plan[
                        "action"
                    ],
                    volume=(
                        final_volume
                    ),
                    entry_price=(
                        execution_entry
                    ),
                    stop_loss=(
                        stop_loss
                    ),
                )
            )

            expected_profit = (
                calculate_trade_profit(
                    symbol=symbol,
                    action=plan[
                        "action"
                    ],
                    volume=(
                        final_volume
                    ),
                    entry_price=(
                        execution_entry
                    ),
                    take_profit=(
                        take_profit
                    ),
                )
            )

            required_margin = (
                calculate_required_margin(
                    symbol=symbol,
                    action=plan[
                        "action"
                    ],
                    volume=(
                        final_volume
                    ),
                    entry_price=(
                        execution_entry
                    ),
                )
            )

            rr = (
                calculate_rr(
                    entry=(
                        execution_entry
                    ),
                    stop_loss=(
                        stop_loss
                    ),
                    take_profit=(
                        take_profit
                    ),
                )
            )

            # =================================================
            # LOSS <= BUDGET
            # =================================================

            if (
                expected_loss
                >
                risk_budget
                + 0.01
            ):

                errors.append(
                    "Убыток "
                    f"${expected_loss:.2f} "
                    "превышает risk budget "
                    f"${risk_budget:.2f}."
                )

            # =================================================
            # MARGIN
            # =================================================

            free_margin = float(
                risk_context[
                    "account"
                ][
                    "free_margin"
                ]
            )

            if (
                required_margin
                >
                free_margin
            ):

                errors.append(
                    "Недостаточно Free Margin: "
                    f"нужно "
                    f"${required_margin:.2f}, "
                    f"доступно "
                    f"${free_margin:.2f}."
                )

            # =================================================
            # WORST CASE
            # =================================================

            equity = float(
                risk_context[
                    "account"
                ][
                    "equity"
                ]
            )

            balance = float(
                risk_context[
                    "account"
                ][
                    "balance"
                ]
            )

            existing_risk = float(
                risk_context[
                    "existing_positions_risk"
                ]
            )

            projected_equity_all_stops = (
                equity
                - existing_risk
                - expected_loss
            )

            projected_balance_all_stops = (
                balance
                - existing_risk
                - expected_loss
            )

            projected_value = min(
                projected_equity_all_stops,
                projected_balance_all_stops,
            )

            limits = (
                risk_context[
                    "limits"
                ]
            )

            safety_buffer = float(
                limits[
                    "safety_buffer_money"
                ]
            )

            if (
                projected_value
                <=
                (
                    limits[
                        "daily_floor"
                    ]
                    +
                    safety_buffer
                )
            ):

                errors.append(
                    "После SL счёт оказался бы "
                    "слишком близко к "
                    "Daily Loss safety level."
                )

            if (
                projected_value
                <=
                (
                    limits[
                        "max_loss_floor"
                    ]
                    +
                    safety_buffer
                )
            ):

                errors.append(
                    "После SL счёт оказался бы "
                    "слишком близко к "
                    "Max Loss safety level."
                )

    # ========================================================
    # 15. BUILD REQUEST
    # ========================================================

    request = None

    order_check = None

    if (
        len(
            errors
        )
        == 0
        and
        final_volume
        > 0
    ):

        request = (
            build_mt5_request(
                plan=plan,
                symbol=symbol,
                volume=final_volume,
                entry_price=(
                    execution_entry
                ),
                stop_loss=stop_loss,
                take_profit=take_profit,
            )
        )

        # ====================================================
        # 16. ORDER CHECK
        # ====================================================

        order_check = (
            perform_order_check(
                request
            )
        )

        if (
            order_check[
                "passed"
            ]
        ):

            pass

        elif (
            order_check[
                "safe_mode_block"
            ]
        ):

            warnings.append(
                "MT5 не разрешил торговую "
                "операцию терминалу."
            )

        else:

            errors.append(
                "order_check() "
                "не подтвердил entry request. "
                f"Retcode={order_check['retcode']}, "
                f"comment={order_check['comment']}."
            )

    # ========================================================
    # FINAL DECISION
    # ========================================================

    if errors:

        decision = (
            "REJECTED"
        )

        ready_for_send = False

    elif (
        order_check is not None
        and
        order_check[
            "passed"
        ]
    ):

        decision = (
            "ORDER_CHECK_PASSED"
        )

        ready_for_send = True

    elif (
        order_check is not None
        and
        order_check[
            "safe_mode_block"
        ]
    ):

        decision = (
            "SAFE_MODE_BLOCKED"
        )

        ready_for_send = False

    else:

        decision = (
            "CHECK_NOT_COMPLETED"
        )

        ready_for_send = False

    # ========================================================
    # FULL RESULT
    # ========================================================

    return {

        "dry_run": True,

        "ready_for_send": (
            ready_for_send
        ),

        "decision": (
            decision
        ),

        "fp_time": (
            current_time.isoformat()
        ),

        "errors": (
            errors
        ),

        "warnings": (
            warnings
        ),

        "pending_actions": (
            pending_actions_report
        ),

        "managed_positions": (
            get_managed_position_context()
        ),

        "plan": (
            build_plan_summary(
                plan
            )
        ),

        "expiration": (
            expiration_check
        ),

        "market": {

            "bid": (
                entry_context[
                    "bid"
                ]
            ),

            "ask": (
                entry_context[
                    "ask"
                ]
            ),

            "planned_entry": (
                planned_entry
            ),

            "execution_entry": (
                execution_entry
            ),

            "market_deviation_percent": (
                calculate_market_deviation_percent(
                    planned_entry=(
                        planned_entry
                    ),
                    live_entry=(
                        execution_entry
                    ),
                )
                if (
                    plan[
                        "order_type"
                    ]
                    == "market"
                )
                else 0.0
            ),
        },

        "levels": {

            "stop_loss": (
                stop_loss
            ),

            "take_profit": (
                take_profit
            ),

            "wave_invalidation_level": (
                plan.get(
                    "wave_invalidation_level"
                )
            ),
        },

        "risk": {

            "risk_budget": (
                risk_budget
            ),

            "plan_volume": float(
                plan[
                    "volume"
                ]
            ),

            "safe_calculated_volume": (
                sizing.get(
                    "safe_calculated_volume"
                )
                if sizing
                else None
            ),

            "final_volume": (
                final_volume
            ),

            "expected_loss": (
                expected_loss
            ),

            "expected_profit": (
                expected_profit
            ),

            "risk_reward": (
                rr
            ),

            "required_margin": (
                required_margin
            ),

            "projected_equity_all_stops": (
                projected_equity_all_stops
            ),

            "projected_balance_all_stops": (
                projected_balance_all_stops
            ),

            "daily_room": (
                risk_context[
                    "limits"
                ][
                    "current_daily_room"
                ]
            ),

            "max_room": (
                risk_context[
                    "limits"
                ][
                    "current_max_room"
                ]
            ),
        },

        "exposure": (
            exposure
        ),

        "invalidation_check": (
            invalidation_check
        ),

        "request": (
            request
        ),

        "order_check": (
            order_check
        ),
    }


# Backward-compatible import for old diagnostic scripts. This is a request
# validator, not a global trading switch; production code uses the explicit
# validate_active_plan() name.
dry_run_active_plan = validate_active_plan


# ============================================================
# PRINT ORDER CHECK
# ============================================================

def print_order_check(
    check: dict | None,
):
    """
    Выводит order_check.
    """

    if check is None:

        print(
            "order_check() не выполнялся."
        )

        return

    print(
        f"Result received: "
        f"{check['result_received']}"
    )

    print(
        f"Passed:          "
        f"{check['passed']}"
    )

    print(
        f"Safe mode block: "
        f"{check['safe_mode_block']}"
    )

    print(
        f"Retcode:         "
        f"{check['retcode']}"
    )

    print(
        f"Comment:         "
        f"{check['comment']}"
    )

    print(
        f"Trade allowed:   "
        f"{check['terminal_trade_allowed']}"
    )

    print(
        f"MT5 last_error:  "
        f"{check['last_error']}"
    )

    if (
        check[
            "result_received"
        ]
    ):

        print()

        print(
            f"Balance:         "
            f"{check['balance']:.2f}"
        )

        print(
            f"Equity:          "
            f"{check['equity']:.2f}"
        )

        print(
            f"Margin:          "
            f"{check['margin']:.2f}"
        )

        print(
            f"Free Margin:     "
            f"{check['margin_free']:.2f}"
        )

        print(
            f"Margin Level:    "
            f"{check['margin_level']:.2f}"
        )


# ============================================================
# PRINT REQUEST
# ============================================================

def print_mt5_request(
    request: dict | None,
):
    """
    Выводит MqlTradeRequest.
    """

    if request is None:

        print(
            "Request не сформирован."
        )

        return

    for key, value in request.items():

        print(
            f"{key:<15} "
            f"{value}"
        )


# ============================================================
# PRINT PENDING ACTIONS
# ============================================================

def print_pending_actions_report(
    report: dict,
):
    """
    Выводит pending action queue.
    """

    print()
    print(
        "PENDING ACTION QUEUE"
    )
    print("-" * 80)

    print(
        f"Pending actions: "
        f"{report['pending_count']}"
    )

    print(
        f"Blocking:        "
        f"{report['blocking_count']}"
    )

    print(
        f"Blocks new entry:"
        f" {report['blocks_new_entry']}"
    )

    actions = (
        report[
            "actions"
        ]
    )

    if not actions:

        print()
        print(
            "Очередь пуста."
        )

        return

    for index, action in enumerate(
        actions,
        start=1,
    ):

        print()
        print(
            f"ACTION #{index}"
        )
        print("-" * 40)

        print(
            f"Action ID:       "
            f"{action['action_id']}"
        )

        print(
            f"Type:            "
            f"{action['type']}"
        )

        print(
            f"Decision:        "
            f"{action['decision']}"
        )

        print(
            f"Plan ID:         "
            f"{action['plan_id']}"
        )

        print(
            f"Symbol:          "
            f"{action['symbol']}"
        )

        print(
            f"MT5 ticket:      "
            f"{action['ticket']}"
        )

        print(
            f"Order found:     "
            f"{action['order_found']}"
        )

        print(
            f"Ownership OK:    "
            f"{action['ownership_verified']}"
        )

        print(
            f"Blocks entry:    "
            f"{action['blocks_new_entry']}"
        )

        if action[
            "errors"
        ]:

            print()
            print(
                "Ошибки:"
            )

            for error in action[
                "errors"
            ]:

                print(
                    f"- {error}"
                )

        if action[
            "warnings"
        ]:

            print()
            print(
                "Предупреждения:"
            )

            for warning in action[
                "warnings"
            ]:

                print(
                    f"- {warning}"
                )

        if (
            action[
                "request"
            ]
            is not None
        ):

            print()
            print(
                "CANCEL REQUEST:"
            )

            print_mt5_request(
                action[
                    "request"
                ]
            )

        if (
            action[
                "order_check"
            ]
            is not None
        ):

            print()
            print(
                "CANCEL ORDER CHECK:"
            )

            print_order_check(
                action[
                    "order_check"
                ]
            )


# ============================================================
# PRINT EXPIRATION
# ============================================================

def print_expiration_report(
    expiration: dict | None,
):
    """
    Выводит состояние TTL.
    """

    if expiration is None:
        return

    print()
    print(
        "PLAN EXPIRATION"
    )
    print("-" * 80)

    print(
        f"Applicable:      "
        f"{expiration.get('applicable')}"
    )

    print(
        f"Expired:         "
        f"{expiration.get('expired')}"
    )

    print(
        f"Age:             "
        f"{expiration.get('age_seconds')}"
    )

    print(
        f"Max age:         "
        f"{expiration.get('max_age_seconds')}"
    )

    if (
        expiration.get(
            "state_updated"
        )
        is not None
    ):

        print(
            f"State updated:   "
            f"{expiration.get('state_updated')}"
        )

    if (
        expiration.get(
            "expired_at_fp"
        )
        is not None
    ):

        print(
            f"Expired at:      "
            f"{expiration.get('expired_at_fp')}"
        )

    print(
        f"Reason:          "
        f"{expiration.get('reason')}"
    )


# ============================================================
# PRINT EXECUTOR REPORT
# ============================================================

def print_executor_report(
    report: dict,
):
    """
    Выводит результат проверки request перед live executor.
    """

    print()
    print("=" * 80)
    print(
        "TRADE EXECUTOR — PRE-SEND VALIDATION"
    )
    print("=" * 80)

    print(
        f"FP Time:         "
        f"{report['fp_time']}"
    )

    print(
        f"Decision:        "
        f"{report['decision']}"
    )

    print(
        f"Ready for send:  "
        f"{report['ready_for_send']}"
    )

    # ========================================================
    # ACTIONS
    # ========================================================

    print_pending_actions_report(
        report[
            "pending_actions"
        ]
    )

    # ========================================================
    # MANAGED POSITIONS
    # ========================================================

    managed = (
        report[
            "managed_positions"
        ]
    )

    print()
    print(
        "MANAGED POSITIONS"
    )
    print("-" * 80)

    print(
        f"Количество:      "
        f"{managed['count']}"
    )

    # ========================================================
    # ERRORS
    # ========================================================

    print()
    print(
        "ОШИБКИ"
    )
    print("-" * 80)

    if report[
        "errors"
    ]:

        for error in report[
            "errors"
        ]:

            print(
                f"- {error}"
            )

    else:

        print(
            "Нет."
        )

    # ========================================================
    # WARNINGS
    # ========================================================

    print()
    print(
        "ПРЕДУПРЕЖДЕНИЯ"
    )
    print("-" * 80)

    if report[
        "warnings"
    ]:

        for warning in report[
            "warnings"
        ]:

            print(
                f"- {warning}"
            )

    else:

        print(
            "Нет."
        )

    # ========================================================
    # PLAN
    # ========================================================

    plan = (
        report.get(
            "plan"
        )
    )

    print()
    print(
        "TRADE PLAN"
    )
    print("-" * 80)

    if plan is None:

        print(
            "Активного Trade Plan нет."
        )

    else:

        print(
            f"Plan ID:         "
            f"{plan['plan_id']}"
        )

        print(
            f"Execution:       "
            f"{plan['execution_status']}"
        )

        print(
            f"Action:          "
            f"{plan['action']}"
        )

        print(
            f"Order type:      "
            f"{plan['order_type']}"
        )

        print(
            f"Confidence:      "
            f"{plan['confidence']}"
        )

        print(
            f"Analysis:        "
            f"{plan['source_analysis_time']}"
        )

        print(
            f"H1 closed bar:   "
            f"{plan['source_h1_closed_bar_time']}"
        )

        if (
            plan[
                "age_seconds"
            ]
            is not None
        ):

            print(
                f"Plan age:        "
                f"{plan['age_seconds']:.1f} sec"
            )

    # ========================================================
    # EXPIRATION
    # ========================================================

    print_expiration_report(
        report.get(
            "expiration"
        )
    )

    # ========================================================
    # EARLY STOP
    # ========================================================

    if (
        report.get(
            "market"
        )
        is None
    ):

        print()
        print("=" * 80)

        if (
            report[
                "decision"
            ]
            == "PLAN_EXPIRED"
        ):

            print(
                "[EXPIRED] "
                "Market Trade Plan "
                "превысил срок жизни."
            )

            print(
                "[EXPIRED] "
                "План перенесён в history."
            )

            print(
                "[EXPIRED] "
                "active_plan очищен."
            )

            print(
                "[WAIT] "
                "Повторный Claude-анализ "
                "этой H1 не выполняется."
            )

            print(
                "[WAIT] "
                "Ждём следующую H1."
            )

        elif (
            report[
                "decision"
            ]
            == "PENDING_ACTIONS_REQUIRED"
        ):

            print(
                "[BLOCKED] "
                "Есть незавершённые actions."
            )

        elif (
            report[
                "decision"
            ]
            == "NO_ACTIVE_PLAN"
        ):

            print(
                "[NO ACTIVE PLAN] "
                "Исполнять нечего."
            )

        else:

            print(
                "[STOP] "
                "Trade Executor остановлен."
            )

        print(
            "[VALIDATION STOP] Request не готов к передаче live executor."
        )

        print("=" * 80)

        return

    # ========================================================
    # MARKET
    # ========================================================

    market = (
        report[
            "market"
        ]
    )

    print()
    print(
        "LIVE MARKET"
    )
    print("-" * 80)

    print(
        f"Bid:             "
        f"{market['bid']}"
    )

    print(
        f"Ask:             "
        f"{market['ask']}"
    )

    print(
        f"Claude Entry:    "
        f"{market['planned_entry']}"
    )

    print(
        f"Execution Entry: "
        f"{market['execution_entry']}"
    )

    if (
        plan is not None
        and
        plan[
            "order_type"
        ]
        == "market"
    ):

        print(
            f"Deviation:       "
            f"{market['market_deviation_percent']:.5f}%"
        )

        print(
            f"Max deviation:   "
            f"{MAX_MARKET_ENTRY_DEVIATION_PERCENT:.5f}%"
        )

    # ========================================================
    # LEVELS
    # ========================================================

    levels = (
        report[
            "levels"
        ]
    )

    print()
    print(
        "LEVELS"
    )
    print("-" * 80)

    print(
        f"Stop Loss:       "
        f"{levels['stop_loss']}"
    )

    print(
        f"Take Profit:     "
        f"{levels['take_profit']}"
    )

    print(
        f"Wave invalid:    "
        f"{levels['wave_invalidation_level']}"
    )

    # ========================================================
    # RISK
    # ========================================================

    risk = (
        report[
            "risk"
        ]
    )

    print()
    print(
        "EXECUTION RISK"
    )
    print("-" * 80)

    print(
        f"Risk budget:     "
        f"{risk['risk_budget']:.2f}"
    )

    print(
        f"Plan lot:        "
        f"{risk['plan_volume']}"
    )

    print(
        f"Safe calc lot:   "
        f"{risk['safe_calculated_volume']}"
    )

    print(
        f"Final lot:       "
        f"{risk['final_volume']}"
    )

    if (
        risk[
            "expected_loss"
        ]
        is not None
    ):

        print(
            f"Loss at SL:      "
            f"{risk['expected_loss']:.2f}"
        )

    if (
        risk[
            "expected_profit"
        ]
        is not None
    ):

        print(
            f"Profit at TP:    "
            f"{risk['expected_profit']:.2f}"
        )

    if (
        risk[
            "risk_reward"
        ]
        is not None
    ):

        print(
            f"R:R:             "
            f"{risk['risk_reward']:.2f}"
        )

    if (
        risk[
            "required_margin"
        ]
        is not None
    ):

        print(
            f"Margin:          "
            f"{risk['required_margin']:.2f}"
        )

    print()

    print(
        f"Daily room:      "
        f"{risk['daily_room']:.2f}"
    )

    print(
        f"Max room:        "
        f"{risk['max_room']:.2f}"
    )

    if (
        risk[
            "projected_equity_all_stops"
        ]
        is not None
    ):

        print(
            f"Equity @ SL:     "
            f"{risk['projected_equity_all_stops']:.2f}"
        )

    # ========================================================
    # EXPOSURE
    # ========================================================

    exposure = (
        report[
            "exposure"
        ]
    )

    print()
    print(
        "CURRENT XAUUSD EXPOSURE"
    )
    print("-" * 80)

    print(
        f"Positions:       "
        f"{exposure['positions_count']}"
    )

    print(
        f"Pending orders:  "
        f"{exposure['orders_count']}"
    )

    # ========================================================
    # INVALIDATION
    # ========================================================

    invalidation = (
        report[
            "invalidation_check"
        ]
    )

    print()
    print(
        "WAVE INVALIDATION"
    )
    print("-" * 80)

    print(
        f"Breached:        "
        f"{invalidation.get('breached')}"
    )

    print(
        f"Reanalysis:      "
        f"{invalidation.get('requires_reanalysis')}"
    )

    print(
        f"Reason:          "
        f"{invalidation.get('reason')}"
    )

    # ========================================================
    # ENTRY REQUEST
    # ========================================================

    print()
    print(
        "MT5 ENTRY REQUEST"
    )
    print("-" * 80)

    print_mt5_request(
        report.get(
            "request"
        )
    )

    # ========================================================
    # ORDER CHECK
    # ========================================================

    print()
    print(
        "MT5 ENTRY ORDER CHECK"
    )
    print("-" * 80)

    print_order_check(
        report.get(
            "order_check"
        )
    )

    # ========================================================
    # FINAL
    # ========================================================

    print()
    print("=" * 80)

    if (
        report[
            "decision"
        ]
        == "ORDER_CHECK_PASSED"
    ):

        print(
            "[VALIDATION OK] Entry request прошёл order_check()."
        )
        print(
            "[NEXT] Live executor повторно проверяет safety/reconciliation "
            "и только затем решает, вызывать ли order_send()."
        )

    elif (
        report[
            "decision"
        ]
        == "SAFE_MODE_BLOCKED"
    ):

        print(
            "[SAFE MODE] "
            "Trading request сформирован, "
            "но реальное исполнение отключено."
        )

    elif (
        report[
            "decision"
        ]
        == "REJECTED"
    ):

        print(
            "[REJECTED] "
            "Trade Executor "
            "заблокировал entry."
        )

    else:

        print(
            "[STOP] "
            "Entry не разрешён."
        )

    print(
        "[NOTE] Фактический результат order_send печатается отдельным "
        "блоком LIVE EXECUTION сразу после этой проверки."
    )

    print("=" * 80)
