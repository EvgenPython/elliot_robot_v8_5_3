from observability import observe, emit
from datetime import datetime, timedelta, timezone
import time

import MetaTrader5 as mt5

from execution_control import (
    EXECUTION_MODE_LIVE,
    EXECUTION_MODE_DRY_RUN,
    inspect_execution_safety_gate,
)

from trade_executor import (
    MAGIC_NUMBER,
    build_order_comment,
    validate_active_plan,
    mt5_object_to_dict,
)

from trade_state import (
    EXECUTION_NOT_SENT,
    EXECUTION_SEND_INTENT,
    attach_position_ticket,
    get_active_plan,
    mark_active_plan_execution_failed,
    mark_active_plan_send_intent,
    record_active_plan_order_send_result,
)


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument

SYMBOL = active_instrument()

RECONCILIATION_POLL_ATTEMPTS = 6

RECONCILIATION_POLL_DELAY_SECONDS = 0.20

HISTORY_LOOKBACK_HOURS = 48


# ============================================================
# MT5 SUCCESS RETCODES
# ============================================================

SUCCESS_RETCODES = {
    int(
        getattr(
            mt5,
            "TRADE_RETCODE_DONE",
            10009,
        )
    ),
    int(
        getattr(
            mt5,
            "TRADE_RETCODE_PLACED",
            10008,
        )
    ),
}

PARTIAL_RETCODE = int(
    getattr(
        mt5,
        "TRADE_RETCODE_DONE_PARTIAL",
        10010,
    )
)


# ============================================================
# MT5 OBJECT -> SAFE DICT
# ============================================================

def result_to_dict(
    value,
):
    """
    Преобразует MT5 namedtuple в обычный dict.
    """

    return mt5_object_to_dict(
        value
    )


# ============================================================
# EXPECTED POSITION TYPE
# ============================================================

def get_expected_position_type(
    action: str,
) -> int | None:
    """
    Возвращает MT5 POSITION_TYPE
    для enter_long / enter_short.
    """

    if action == "enter_long":
        return int(
            mt5.POSITION_TYPE_BUY
        )

    if action == "enter_short":
        return int(
            mt5.POSITION_TYPE_SELL
        )

    return None


# ============================================================
# EXPECTED DEAL TYPE
# ============================================================

def get_expected_deal_type(
    action: str,
) -> int | None:
    """
    Возвращает MT5 DEAL_TYPE
    для входного market deal.
    """

    if action == "enter_long":
        return int(
            mt5.DEAL_TYPE_BUY
        )

    if action == "enter_short":
        return int(
            mt5.DEAL_TYPE_SELL
        )

    return None


# ============================================================
# TOLERANCES
# ============================================================

def get_symbol_tolerances(
    symbol: str,
) -> tuple[float, float]:
    """
    Возвращает допустимое отклонение цены
    и объёма при reconciliation.
    """

    info = mt5.symbol_info(
        symbol
    )

    if info is None:

        return (
            0.00001,
            0.00000001,
        )

    price_tolerance = max(
        float(
            info.point
        )
        * 2.0,
        0.00001,
    )

    volume_tolerance = max(
        float(
            info.volume_step
        )
        / 10.0,
        0.00000001,
    )

    return (
        price_tolerance,
        volume_tolerance,
    )


# ============================================================
# SEND REQUEST
# ============================================================

def get_expected_request(
    plan: dict,
) -> dict:
    """
    Возвращает request, который был записан
    перед order_send().

    Для ещё не отправленного плана request
    может отсутствовать.
    """

    request = (
        plan.get(
            "send_request"
        )
        or {}
    )

    return dict(
        request
    )


# ============================================================
# OWNERSHIP
# ============================================================

def is_owned_by_plan(
    item,
    plan: dict,
) -> bool:
    """
    Проверяет Magic + exact comment + symbol.
    """

    expected_comment = (
        get_expected_request(
            plan
        ).get(
            "comment"
        )
        or
        build_order_comment(
            plan
        )
    )

    return (
        str(
            getattr(
                item,
                "symbol",
                "",
            )
        )
        ==
        str(
            plan.get(
                "symbol",
                SYMBOL,
            )
        )
        and
        int(
            getattr(
                item,
                "magic",
                -1,
            )
        )
        ==
        int(
            MAGIC_NUMBER
        )
        and
        str(
            getattr(
                item,
                "comment",
                "",
            )
        )
        ==
        str(
            expected_comment
        )
    )


# ============================================================
# VALIDATE POSITION
# ============================================================

def validate_position_against_plan(
    position,
    plan: dict,
) -> list[str]:
    """
    Проверяет найденную MT5 position
    против фактически отправленного request.
    """

    errors = []

    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    request = (
        get_expected_request(
            plan
        )
    )

    price_tolerance, volume_tolerance = (
        get_symbol_tolerances(
            symbol
        )
    )

    expected_type = (
        get_expected_position_type(
            str(
                plan.get(
                    "action",
                    "",
                )
            )
        )
    )

    actual_type = int(
        getattr(
            position,
            "type",
            -1,
        )
    )

    if (
        expected_type is None
        or
        actual_type != expected_type
    ):

        errors.append(
            "Направление найденной позиции "
            "не совпадает с Trade Plan."
        )

    expected_volume = float(
        request.get(
            "volume",
            plan.get(
                "volume",
                0.0,
            ),
        )
    )

    actual_volume = float(
        getattr(
            position,
            "volume",
            0.0,
        )
    )

    if (
        abs(
            actual_volume
            - expected_volume
        )
        >
        volume_tolerance
    ):

        errors.append(
            "Volume найденной позиции "
            "не совпадает с отправленным request: "
            f"{actual_volume} != {expected_volume}."
        )

    expected_sl = float(
        request.get(
            "sl",
            plan.get(
                "stop_loss",
                0.0,
            ),
        )
    )

    expected_tp = float(
        request.get(
            "tp",
            plan.get(
                "take_profit",
                0.0,
            ),
        )
    )

    actual_sl = float(
        getattr(
            position,
            "sl",
            0.0,
        )
    )

    actual_tp = float(
        getattr(
            position,
            "tp",
            0.0,
        )
    )

    if (
        actual_sl <= 0
        or
        abs(
            actual_sl
            - expected_sl
        )
        >
        price_tolerance
    ):

        errors.append(
            "SL найденной позиции "
            "не совпадает с request: "
            f"MT5={actual_sl}, "
            f"expected={expected_sl}."
        )

    if (
        actual_tp <= 0
        or
        abs(
            actual_tp
            - expected_tp
        )
        >
        price_tolerance
    ):

        errors.append(
            "TP найденной позиции "
            "не совпадает с request: "
            f"MT5={actual_tp}, "
            f"expected={expected_tp}."
        )

    return errors


# ============================================================
# FIND POSITIONS
# ============================================================

def find_owned_positions(
    plan: dict,
) -> dict:
    """
    Ищет открытые позиции конкретного plan_id.
    """

    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    positions = mt5.positions_get(
        symbol=symbol
    )

    if positions is None:

        return {

            "ok": False,

            "error": (
                "positions_get() вернул None. "
                f"MT5 error: {mt5.last_error()}"
            ),

            "owned": [],

            "valid": [],

            "invalid": [],
        }

    owned = []
    valid = []
    invalid = []

    for position in positions:

        if not is_owned_by_plan(
            position,
            plan,
        ):
            continue

        item = {

            "raw": position,

            "dict": (
                result_to_dict(
                    position
                )
            ),

            "errors": (
                validate_position_against_plan(
                    position,
                    plan,
                )
            ),
        }

        owned.append(
            item
        )

        if item[
            "errors"
        ]:

            invalid.append(
                item
            )

        else:

            valid.append(
                item
            )

    return {

        "ok": True,

        "error": None,

        "owned": owned,

        "valid": valid,

        "invalid": invalid,
    }


# ============================================================
# FIND ORDERS
# ============================================================

def find_owned_orders(
    plan: dict,
) -> dict:
    """
    Ищет активные MT5 orders конкретного plan_id.

    Для market SEND_INTENT наличие такого order
    считается доказательством неопределённого
    состояния и блокирует повторную отправку.
    """

    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    orders = mt5.orders_get(
        symbol=symbol
    )

    if orders is None:

        return {

            "ok": False,

            "error": (
                "orders_get() вернул None. "
                f"MT5 error: {mt5.last_error()}"
            ),

            "owned": [],
        }

    owned = []

    for order in orders:

        if is_owned_by_plan(
            order,
            plan,
        ):

            owned.append(
                result_to_dict(
                    order
                )
            )

    return {

        "ok": True,

        "error": None,

        "owned": owned,
    }


# ============================================================
# INTENT TIME
# ============================================================

def get_send_intent_epoch_msc(
    plan: dict,
) -> int | None:
    """
    Возвращает SEND_INTENT время в epoch milliseconds.
    """

    value = (
        plan.get(
            "send_intent_at_fp"
        )
    )

    if not value:
        return None

    try:

        parsed = (
            datetime.fromisoformat(
                str(
                    value
                )
            )
        )

        return int(
            parsed.timestamp()
            * 1000
        )

    except (
        ValueError,
        TypeError,
    ):

        return None


# ============================================================
# HISTORY DEALS
# ============================================================

def find_owned_entry_deals(
    plan: dict,
) -> dict:
    """
    Ищет входной deal по Magic/comment/symbol.

    Это позволяет восстановить ситуацию,
    когда после order_send() позиция успела
    открыться и уже закрыться до рестарта Python.
    """

    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    expected_deal_type = (
        get_expected_deal_type(
            str(
                plan.get(
                    "action",
                    "",
                )
            )
        )
    )

    request = (
        get_expected_request(
            plan
        )
    )

    expected_volume = float(
        request.get(
            "volume",
            plan.get(
                "volume",
                0.0,
            ),
        )
    )

    _, volume_tolerance = (
        get_symbol_tolerances(
            symbol
        )
    )

    now_local = datetime.now(timezone.utc)

    history_from = (
        now_local
        - timedelta(
            hours=HISTORY_LOOKBACK_HOURS
        )
    )

    history_to = (
        now_local
        + timedelta(
            minutes=5
        )
    )

    deals = mt5.history_deals_get(
        history_from,
        history_to,
    )

    if deals is None:

        return {

            "ok": False,

            "error": (
                "history_deals_get() вернул None. "
                f"MT5 error: {mt5.last_error()}"
            ),

            "owned": [],
        }

    send_intent_msc = (
        get_send_intent_epoch_msc(
            plan
        )
    )

    entry_in = int(
        getattr(
            mt5,
            "DEAL_ENTRY_IN",
            0,
        )
    )

    entry_inout = int(
        getattr(
            mt5,
            "DEAL_ENTRY_INOUT",
            2,
        )
    )

    owned = []

    for deal in deals:

        if not is_owned_by_plan(
            deal,
            plan,
        ):
            continue

        deal_entry = int(
            getattr(
                deal,
                "entry",
                -1,
            )
        )

        if (
            deal_entry
            not in (
                entry_in,
                entry_inout,
            )
        ):
            continue

        if (
            expected_deal_type is not None
            and
            int(
                getattr(
                    deal,
                    "type",
                    -1,
                )
            )
            != expected_deal_type
        ):
            continue

        deal_volume = float(
            getattr(
                deal,
                "volume",
                0.0,
            )
        )

        # Полный market fill ожидается одним объёмом.
        # Partial fill не считаем автоматически разрешённым.
        if (
            abs(
                deal_volume
                - expected_volume
            )
            >
            volume_tolerance
        ):
            continue

        if (
            send_intent_msc is not None
        ):

            deal_time_msc = int(
                getattr(
                    deal,
                    "time_msc",
                    0,
                )
                or
                (
                    int(
                        getattr(
                            deal,
                            "time",
                            0,
                        )
                    )
                    * 1000
                )
            )

            # Допускаем 5 секунд разницы часов/записи.
            if (
                deal_time_msc
                <
                (
                    send_intent_msc
                    - 5000
                )
            ):
                continue

        owned.append(
            {

                "raw": deal,

                "dict": (
                    result_to_dict(
                        deal
                    )
                ),
            }
        )

    return {

        "ok": True,

        "error": None,

        "owned": owned,
    }


# ============================================================
# ATTACH LIVE POSITION
# ============================================================

def attach_live_position(
    plan: dict,
    position,
    order_ticket: int | None = None,
    deal_ticket: int | None = None,
) -> dict:
    """
    Привязывает найденную реальную позицию
    к Trade State.
    """

    ticket = int(
        position.ticket
    )

    attach_position_ticket(
        plan_id=str(
            plan[
                "plan_id"
            ]
        ),
        ticket=ticket,
        position_identifier=int(
            getattr(
                position,
                "identifier",
                ticket,
            )
        ),
        actual_volume=float(
            position.volume
        ),
        actual_open_price=float(
            position.price_open
        ),
        actual_stop_loss=float(
            position.sl
        ),
        actual_take_profit=float(
            position.tp
        ),
        order_ticket=order_ticket,
        deal_ticket=deal_ticket,
    )

    return {

        "attached": True,

        "position_ticket": (
            ticket
        ),

        "position": (
            result_to_dict(
                position
            )
        ),
    }


# ============================================================
# ATTACH HISTORICAL POSITION
# ============================================================

def attach_historical_position(
    plan: dict,
    deal,
) -> dict:
    """
    Восстанавливает position ticket из history deal.

    Затем обычный Position Gate main.py
    найдёт закрывающий deal и завершит lifecycle.
    """

    position_ticket = int(
        getattr(
            deal,
            "position_id",
            0,
        )
    )

    if position_ticket <= 0:

        raise RuntimeError(
            "History deal найден, но position_id отсутствует."
        )

    request = (
        get_expected_request(
            plan
        )
    )

    attach_position_ticket(
        plan_id=str(
            plan[
                "plan_id"
            ]
        ),
        ticket=position_ticket,
        position_identifier=position_ticket,
        actual_volume=float(
            getattr(
                deal,
                "volume",
                request.get(
                    "volume",
                    plan.get(
                        "volume",
                        0.0,
                    ),
                ),
            )
        ),
        actual_open_price=float(
            getattr(
                deal,
                "price",
                request.get(
                    "price",
                    plan.get(
                        "entry_price",
                        0.0,
                    ),
                ),
            )
        ),
        actual_stop_loss=float(
            request.get(
                "sl",
                plan.get(
                    "stop_loss",
                    0.0,
                ),
            )
        ),
        actual_take_profit=float(
            request.get(
                "tp",
                plan.get(
                    "take_profit",
                    0.0,
                ),
            )
        ),
        order_ticket=(
            int(
                getattr(
                    deal,
                    "order",
                    0,
                )
            )
            or None
        ),
        deal_ticket=(
            int(
                getattr(
                    deal,
                    "ticket",
                    0,
                )
            )
            or None
        ),
    )

    return {

        "attached": True,

        "position_ticket": (
            position_ticket
        ),

        "deal": (
            result_to_dict(
                deal
            )
        ),
    }


# ============================================================
# RECONCILIATION
# ============================================================

def reconcile_plan_execution(
    plan: dict,
) -> dict:
    """
    Ищет доказательства уже выполненного
    market order.

    Приоритет:

        1. open position
        2. active order
        3. history entry deal

    НИЧЕГО повторно не отправляет.
    """

    positions = (
        find_owned_positions(
            plan
        )
    )

    if not positions[
        "ok"
    ]:

        return {

            "resolved": False,

            "blocked": True,

            "decision": (
                "RECONCILIATION_ERROR"
            ),

            "errors": [
                positions[
                    "error"
                ]
            ],

            "position_ticket": None,

            "evidence": None,
        }

    if (
        len(
            positions[
                "valid"
            ]
        )
        > 1
    ):

        return {

            "resolved": False,

            "blocked": True,

            "decision": (
                "MULTIPLE_MATCHING_POSITIONS"
            ),

            "errors": [
                "Найдено несколько позиций "
                "с Magic/comment этого plan_id."
            ],

            "position_ticket": None,

            "evidence": {
                "positions": [
                    item[
                        "dict"
                    ]
                    for item
                    in positions[
                        "valid"
                    ]
                ],
            },
        }

    if (
        len(
            positions[
                "invalid"
            ]
        )
        > 0
    ):

        return {

            "resolved": False,

            "blocked": True,

            "decision": (
                "POSITION_MISMATCH"
            ),

            "errors": [
                error
                for item
                in positions[
                    "invalid"
                ]
                for error
                in item[
                    "errors"
                ]
            ],

            "position_ticket": None,

            "evidence": {
                "positions": [
                    item[
                        "dict"
                    ]
                    for item
                    in positions[
                        "invalid"
                    ]
                ],
            },
        }

    if (
        len(
            positions[
                "valid"
            ]
        )
        == 1
    ):

        position = (
            positions[
                "valid"
            ][
                0
            ][
                "raw"
            ]
        )

        attached = (
            attach_live_position(
                plan=plan,
                position=position,
            )
        )

        return {

            "resolved": True,

            "blocked": False,

            "decision": (
                "POSITION_RECOVERED"
            ),

            "errors": [],

            "position_ticket": (
                attached[
                    "position_ticket"
                ]
            ),

            "evidence": (
                attached
            ),
        }

    orders = (
        find_owned_orders(
            plan
        )
    )

    if not orders[
        "ok"
    ]:

        return {

            "resolved": False,

            "blocked": True,

            "decision": (
                "RECONCILIATION_ERROR"
            ),

            "errors": [
                orders[
                    "error"
                ]
            ],

            "position_ticket": None,

            "evidence": None,
        }

    if orders[
        "owned"
    ]:

        return {

            "resolved": False,

            "blocked": True,

            "decision": (
                "MATCHING_ACTIVE_ORDER_FOUND"
            ),

            "errors": [
                "Найден active MT5 order "
                "этого plan_id. Повторный "
                "order_send запрещён."
            ],

            "position_ticket": None,

            "evidence": {
                "orders": (
                    orders[
                        "owned"
                    ]
                ),
            },
        }

    deals = (
        find_owned_entry_deals(
            plan
        )
    )

    if not deals[
        "ok"
    ]:

        return {

            "resolved": False,

            "blocked": True,

            "decision": (
                "RECONCILIATION_ERROR"
            ),

            "errors": [
                deals[
                    "error"
                ]
            ],

            "position_ticket": None,

            "evidence": None,
        }

    if (
        len(
            deals[
                "owned"
            ]
        )
        > 1
    ):

        # Несколько entry deals могут означать partial fill.
        # На первом DEMO этапе автоматически это не склеиваем.
        return {

            "resolved": False,

            "blocked": True,

            "decision": (
                "MULTIPLE_ENTRY_DEALS_FOUND"
            ),

            "errors": [
                "Найдено несколько входных deals "
                "этого plan_id. Возможен partial fill. "
                "Автоматический повторный send запрещён."
            ],

            "position_ticket": None,

            "evidence": {
                "deals": [
                    item[
                        "dict"
                    ]
                    for item
                    in deals[
                        "owned"
                    ]
                ],
            },
        }

    if (
        len(
            deals[
                "owned"
            ]
        )
        == 1
    ):

        deal = (
            deals[
                "owned"
            ][
                0
            ][
                "raw"
            ]
        )

        attached = (
            attach_historical_position(
                plan=plan,
                deal=deal,
            )
        )

        return {

            "resolved": True,

            "blocked": False,

            "decision": (
                "HISTORICAL_POSITION_RECOVERED"
            ),

            "errors": [],

            "position_ticket": (
                attached[
                    "position_ticket"
                ]
            ),

            "evidence": (
                attached
            ),
        }

    return {

        "resolved": False,

        "blocked": False,

        "decision": (
            "NO_EXECUTION_EVIDENCE"
        ),

        "errors": [],

        "position_ticket": None,

        "evidence": None,
    }


# ============================================================
# STARTUP SEND-INTENT RECONCILIATION
# ============================================================

def reconcile_active_send_intent(
    symbol: str = SYMBOL,
) -> dict:
    """
    Вызывается СРАЗУ после подключения к MT5,
    раньше Position Gate и раньше Claude.

    Если active_plan уже имеет SEND_INTENT,
    повторный send категорически запрещён.
    """

    plan = (
        get_active_plan()
    )

    if plan is None:

        return {

            "applicable": False,

            "resolved": False,

            "blocked": False,

            "decision": (
                "NO_ACTIVE_PLAN"
            ),

            "plan_id": None,

            "position_ticket": None,

            "errors": [],
        }

    if (
        str(
            plan.get(
                "symbol",
                symbol,
            )
        )
        != str(
            symbol
        )
    ):

        return {

            "applicable": False,

            "resolved": False,

            "blocked": True,

            "decision": (
                "ACTIVE_PLAN_SYMBOL_MISMATCH"
            ),

            "plan_id": (
                plan.get(
                    "plan_id"
                )
            ),

            "position_ticket": None,

            "errors": [
                "active_plan имеет другой symbol."
            ],
        }

    if (
        plan.get(
            "execution_status"
        )
        != EXECUTION_SEND_INTENT
    ):

        return {

            "applicable": False,

            "resolved": False,

            "blocked": False,

            "decision": (
                "NO_SEND_INTENT"
            ),

            "plan_id": (
                plan.get(
                    "plan_id"
                )
            ),

            "position_ticket": None,

            "errors": [],
        }

    reconciliation = (
        reconcile_plan_execution(
            plan
        )
    )

    if reconciliation[
        "resolved"
    ]:

        return {

            "applicable": True,

            "resolved": True,

            "blocked": False,

            "decision": (
                reconciliation[
                    "decision"
                ]
            ),

            "plan_id": (
                plan.get(
                    "plan_id"
                )
            ),

            "position_ticket": (
                reconciliation[
                    "position_ticket"
                ]
            ),

            "errors": [],

            "evidence": (
                reconciliation.get(
                    "evidence"
                )
            ),
        }

    # SEND_INTENT существует, но доказательств результата
    # пока нет. Fail closed: НИКОГДА не повторяем send.
    errors = list(
        reconciliation.get(
            "errors",
            [],
        )
    )

    if not errors:

        errors.append(
            "SEND_INTENT существует, но MT5 пока "
            "не дал однозначных доказательств результата. "
            "Повторный order_send запрещён."
        )

    return {

        "applicable": True,

        "resolved": False,

        "blocked": True,

        "decision": (
            reconciliation[
                "decision"
            ]
        ),

        "plan_id": (
            plan.get(
                "plan_id"
            )
        ),

        "position_ticket": None,

        "errors": (
            errors
        ),

        "evidence": (
            reconciliation.get(
                "evidence"
            )
        ),
    }


# ============================================================
# POST-SEND RECONCILIATION
# ============================================================

def reconcile_after_order_send(
    plan: dict,
) -> dict:
    """
    После order_send() коротко опрашивает MT5,
    чтобы получить фактическую позицию/deal.

    Это синхронный контроль текущего вызова,
    не фоновая задача.
    """

    last_result = None

    for attempt in range(
        1,
        RECONCILIATION_POLL_ATTEMPTS
        + 1,
    ):

        last_result = (
            reconcile_plan_execution(
                plan
            )
        )

        if (
            last_result[
                "resolved"
            ]
            or
            last_result[
                "blocked"
            ]
        ):

            return {

                **last_result,

                "attempts": (
                    attempt
                ),
            }

        if (
            attempt
            <
            RECONCILIATION_POLL_ATTEMPTS
        ):

            time.sleep(
                RECONCILIATION_POLL_DELAY_SECONDS
            )

    return {

        **(
            last_result
            or {
                "resolved": False,
                "blocked": False,
                "decision": (
                    "NO_EXECUTION_EVIDENCE"
                ),
                "errors": [],
                "position_ticket": None,
                "evidence": None,
            }
        ),

        "attempts": (
            RECONCILIATION_POLL_ATTEMPTS
        ),
    }


# ============================================================
# ORDER SEND RESULT
# ============================================================

def build_order_send_result(
    result,
) -> dict:
    """
    Нормализует ответ mt5.order_send().
    """

    if result is None:

        return {

            "received": False,

            "retcode": None,

            "comment": None,

            "order": None,

            "deal": None,

            "volume": None,

            "price": None,

            "last_error": (
                mt5.last_error()
            ),

            "raw": None,
        }

    return {

        "received": True,

        "retcode": int(
            result.retcode
        ),

        "comment": str(
            result.comment
        ),

        "order": (
            int(
                result.order
            )
            if getattr(
                result,
                "order",
                0,
            )
            else None
        ),

        "deal": (
            int(
                result.deal
            )
            if getattr(
                result,
                "deal",
                0,
            )
            else None
        ),

        "volume": (
            float(
                result.volume
            )
            if getattr(
                result,
                "volume",
                None,
            )
            is not None
            else None
        ),

        "price": (
            float(
                result.price
            )
            if getattr(
                result,
                "price",
                None,
            )
            is not None
            else None
        ),

        "last_error": (
            mt5.last_error()
        ),

        "raw": (
            result_to_dict(
                result
            )
        ),
    }


# ============================================================
# EXECUTE ACTIVE PLAN
# ============================================================

@observe("execution")
def execute_active_plan(
    snapshot: dict,
    symbol: str = SYMBOL,
) -> dict:
    """
    Унифицированный Executor.

    MARKET:
        1. dry-run validation + order_check;
        2. second execution safety gate;
        3. reconciliation перед send;
        4. durable SEND_INTENT;
        5. ровно один mt5.order_send();
        6. reconciliation;
        7. attach_position_ticket().

    LIMIT / STOP:
        передаёт lifecycle в pending_executor.py.
        Pending живёт ограниченное число H1, пока не истёк,
        не отменён новой идеей/сессией и не нарушена инвалидация.
    """

    current_plan = get_active_plan()

    if (
        current_plan is not None
        and str(
            current_plan.get(
                "order_type",
                "",
            )
        ) in (
            "limit",
            "stop",
        )
    ):
        # Локальный import исключает циклическую
        # зависимость модулей при старте проекта.
        from pending_executor import execute_pending_plan

        return execute_pending_plan(
            snapshot=snapshot,
            symbol=symbol,
        )

    # There is nothing to validate or send after an earlier execution moved
    # the plan into managed_positions.  Returning here is important: the
    # validator quite correctly reports a missing plan as a failed
    # validation, but presenting that as VALIDATION_BLOCKED made an ordinary
    # idempotent second call look like a trading restriction.
    if current_plan is None:
        no_plan_report = validate_active_plan(
            snapshot=snapshot,
            symbol=symbol,
        )
        return {
            "mode": None,
            "decision": "NO_ACTIVE_PLAN",
            # Keep the normal report contract so CLI/reporting code can print
            # this idempotent outcome without a secondary exception.
            "validation_report": no_plan_report,
            "execution_safety": None,
            "order_send_called": False,
            "order_send_result": None,
            "send_intent": None,
            "reconciliation": None,
            "position_ticket": None,
            "state_attached": False,
            "errors": [],
        }

    validation_report = (
        validate_active_plan(
            snapshot=snapshot,
            symbol=symbol,
        )
    )

    # Execution Safety Gate пересчитывается
    # непосредственно перед возможным order_send().
    # Нельзя полагаться на состояние терминала,
    # которое было прочитано в начале main.py.
    execution_safety = (
        inspect_execution_safety_gate()
    )

    mode = str(
        execution_safety.get(
            "mode",
            EXECUTION_MODE_DRY_RUN,
        )
    ).upper()

    result = {

        "mode": (
            mode
        ),

        "decision": (
            "VALIDATION_ONLY"
        ),

        "validation_report": (
            validation_report
        ),

        "execution_safety": (
            execution_safety
        ),

        "order_send_called": False,

        "order_send_result": None,

        "send_intent": None,

        "reconciliation": None,

        "position_ticket": None,

        "state_attached": False,

        "errors": [],
    }

    # ========================================================
    # DRY RUN
    # ========================================================

    if (
        mode
        == EXECUTION_MODE_DRY_RUN
    ):

        result[
            "decision"
        ] = (
            "DRY_RUN_ONLY"
        )

        return result

    # ========================================================
    # LIVE EXECUTION (all configured account types)
    # ========================================================

    if (
        mode
        != EXECUTION_MODE_LIVE
    ):

        result[
            "decision"
        ] = (
            "EXECUTION_MODE_BLOCKED"
        )

        result[
            "errors"
        ].append(
            f"Неизвестный execution mode: {mode}."
        )

        return result

    # ========================================================
    # VALIDATION MUST PASS
    # ========================================================

    if (
        validation_report.get(
            "decision"
        )
        != "ORDER_CHECK_PASSED"
        or
        not validation_report.get(
            "ready_for_send",
            False,
        )
    ):

        result[
            "decision"
        ] = (
            "VALIDATION_BLOCKED"
        )

        return result

    # ========================================================
    # ONLY MARKET LIVE FOR NOW
    # ========================================================

    plan = (
        get_active_plan()
    )

    if plan is None:

        result[
            "decision"
        ] = (
            "NO_ACTIVE_PLAN"
        )

        result[
            "errors"
        ].append(
            "После validation active_plan исчез."
        )

        return result

    plan_id = str(
        plan.get(
            "plan_id",
            "",
        )
    )

    if (
        plan.get(
            "order_type"
        )
        != "market"
    ):

        result[
            "decision"
        ] = (
            "LIVE_PENDING_NOT_IMPLEMENTED"
        )

        result[
            "errors"
        ].append(
            "На этом этапе LIVE разрешён "
            "только для market order."
        )

        return result

    # ========================================================
    # GLOBAL EXECUTION SAFETY
    # ========================================================

    if not execution_safety.get(
        "order_send_allowed",
        False,
    ):

        result[
            "decision"
        ] = (
            "EXECUTION_SAFETY_BLOCKED"
        )

        result[
            "errors"
        ].extend(
            execution_safety.get(
                "errors",
                [],
            )
        )

        if not result[
            "errors"
        ]:

            result[
                "errors"
            ].append(
                "Execution Safety Gate не разрешил order_send()."
            )

        return result

    # ========================================================
    # PRE-SEND RECONCILIATION
    # ========================================================

    reconciliation = (
        reconcile_plan_execution(
            plan
        )
    )

    result[
        "reconciliation"
    ] = (
        reconciliation
    )

    if reconciliation[
        "resolved"
    ]:

        result[
            "decision"
        ] = (
            "RECOVERED_EXISTING_EXECUTION"
        )

        result[
            "position_ticket"
        ] = (
            reconciliation[
                "position_ticket"
            ]
        )

        result[
            "state_attached"
        ] = True

        return result

    if reconciliation[
        "blocked"
    ]:

        result[
            "decision"
        ] = (
            "PRE_SEND_RECONCILIATION_BLOCKED"
        )

        result[
            "errors"
        ].extend(
            reconciliation.get(
                "errors",
                [],
            )
        )

        return result

    # ========================================================
    # RE-READ ACTIVE PLAN
    # ========================================================

    fresh_plan = (
        get_active_plan()
    )

    if (
        fresh_plan is None
        or
        str(
            fresh_plan.get(
                "plan_id",
                "",
            )
        )
        != plan_id
    ):

        result[
            "decision"
        ] = (
            "ACTIVE_PLAN_CHANGED"
        )

        result[
            "errors"
        ].append(
            "active_plan изменился непосредственно "
            "перед SEND_INTENT."
        )

        return result

    if (
        fresh_plan.get(
            "execution_status"
        )
        != EXECUTION_NOT_SENT
    ):

        result[
            "decision"
        ] = (
            "EXECUTION_STATE_CHANGED"
        )

        result[
            "errors"
        ].append(
            "execution_status изменился непосредственно "
            "перед SEND_INTENT: "
            f"{fresh_plan.get('execution_status')}."
        )

        return result

    request = (
        validation_report.get(
            "request"
        )
    )

    if not isinstance(
        request,
        dict,
    ):

        result[
            "decision"
        ] = (
            "NO_MT5_REQUEST"
        )

        result[
            "errors"
        ].append(
            "Validation report не содержит MT5 request."
        )

        return result

    # ========================================================
    # DURABLE SEND INTENT
    # ========================================================

    send_intent = (
        mark_active_plan_send_intent(
            plan_id=plan_id,
            request=request,
        )
    )

    result[
        "send_intent"
    ] = (
        send_intent
    )

    # После SEND_INTENT повторное исполнение запрещено.
    # С этого места order_send вызывается максимум один раз
    # в текущем процессе.
    # ========================================================
    # ORDER SEND
    # ========================================================

    emit("mt5", "order_send.request", data={"request": request})
    order_send_raw = mt5.order_send(request)
    emit("mt5", "order_send.response", data={"result": order_send_raw, "last_error": mt5.last_error() if order_send_raw is None else None})

    result[
        "order_send_called"
    ] = True

    order_send_result = (
        build_order_send_result(
            order_send_raw
        )
    )

    result[
        "order_send_result"
    ] = (
        order_send_result
    )

    # Сохраняем ответ, если процесс ещё жив.
    try:

        record_active_plan_order_send_result(
            plan_id=plan_id,
            order_send_result=(
                order_send_result
            ),
        )

    except Exception as error:

        result[
            "errors"
        ].append(
            "Не удалось сохранить order_send result: "
            f"{error}"
        )

    # ========================================================
    # POST-SEND RECONCILIATION
    # ========================================================

    send_intent_plan = (
        get_active_plan()
    )

    if (
        send_intent_plan is not None
        and
        str(
            send_intent_plan.get(
                "plan_id",
                "",
            )
        )
        == plan_id
    ):

        post_reconciliation = (
            reconcile_after_order_send(
                send_intent_plan
            )
        )

    else:

        post_reconciliation = {

            "resolved": False,

            "blocked": True,

            "decision": (
                "ACTIVE_PLAN_DISAPPEARED_AFTER_SEND"
            ),

            "errors": [
                "active_plan неожиданно исчез/изменился "
                "после order_send()."
            ],

            "position_ticket": None,

            "evidence": None,

            "attempts": 0,
        }

    result[
        "reconciliation"
    ] = (
        post_reconciliation
    )

    if post_reconciliation[
        "resolved"
    ]:

        result[
            "decision"
        ] = (
            "ORDER_SEND_SUCCESS"
        )

        result[
            "position_ticket"
        ] = (
            post_reconciliation[
                "position_ticket"
            ]
        )

        result[
            "state_attached"
        ] = True

        return result

    # ========================================================
    # PARTIAL FILL = FAIL CLOSED
    # ========================================================

    retcode = (
        order_send_result.get(
            "retcode"
        )
    )

    if (
        retcode
        == PARTIAL_RETCODE
    ):

        result[
            "decision"
        ] = (
            "PARTIAL_FILL_UNRESOLVED"
        )

        result[
            "errors"
        ].append(
            "MT5 сообщил DONE_PARTIAL. "
            "Автоматический повторный send запрещён. "
            "Требуется reconciliation."
        )

        return result

    # ========================================================
    # SUCCESS RETCODE BUT NO EVIDENCE = UNKNOWN
    # ========================================================

    if (
        order_send_result[
            "received"
        ]
        and
        retcode
        in SUCCESS_RETCODES
    ):

        result[
            "decision"
        ] = (
            "ORDER_SEND_STATE_UNKNOWN"
        )

        result[
            "errors"
        ].append(
            "MT5 вернул успешный retcode, "
            "но позиция/deal пока не подтверждены. "
            "SEND_INTENT сохранён. "
            "Повторный order_send запрещён."
        )

        return result

    # ========================================================
    # NO RESULT = UNKNOWN
    # ========================================================

    if not order_send_result[
        "received"
    ]:

        result[
            "decision"
        ] = (
            "ORDER_SEND_STATE_UNKNOWN"
        )

        result[
            "errors"
        ].append(
            "order_send() вернул None. "
            "SEND_INTENT сохранён. "
            "Повторный order_send запрещён "
            "до reconciliation."
        )

        return result

    # ========================================================
    # EXPLICIT REJECT
    # ========================================================

    # Перед архивированием уже был выполнен post-send
    # reconciliation и доказательств исполнения нет.
    failure_reason = (
        "MT5 отклонил market order. "
        f"Retcode={retcode}; "
        f"comment={order_send_result.get('comment')}."
    )

    try:

        failed_state = (
            mark_active_plan_execution_failed(
                plan_id=plan_id,
                reason=failure_reason,
                order_send_result=(
                    order_send_result
                ),
            )
        )

    except Exception as error:

        result[
            "decision"
        ] = (
            "EXECUTION_FAILURE_STATE_ERROR"
        )

        result[
            "errors"
        ].append(
            failure_reason
        )

        result[
            "errors"
        ].append(
            "Не удалось безопасно завершить "
            "failed execution в Trade State: "
            f"{error}"
        )

        return result

    result[
        "decision"
    ] = (
        "ORDER_SEND_REJECTED"
    )

    result[
        "errors"
    ].append(
        failure_reason
    )

    result[
        "failed_state"
    ] = (
        failed_state
    )

    return result


# ============================================================
# PRINT STARTUP RECONCILIATION
# ============================================================

def print_send_intent_reconciliation(
    report: dict,
):
    """
    Печатает startup reconciliation.
    """

    if not report.get(
        "applicable",
        False,
    ):

        return

    print()
    print("=" * 80)
    print(
        "MARKET SEND-INTENT RECONCILIATION"
    )
    print("=" * 80)

    print(
        f"Plan ID:          "
        f"{report.get('plan_id')}"
    )

    print(
        f"Decision:         "
        f"{report.get('decision')}"
    )

    print(
        f"Resolved:         "
        f"{report.get('resolved')}"
    )

    print(
        f"Blocked:          "
        f"{report.get('blocked')}"
    )

    print(
        f"Position ticket:  "
        f"{report.get('position_ticket')}"
    )

    errors = (
        report.get(
            "errors",
            [],
        )
    )

    if errors:

        print()
        print("ОШИБКИ / BLOCK")
        print("-" * 80)

        for error in errors:

            print(
                f"- {error}"
            )

    if report.get(
        "resolved"
    ):

        print()
        print(
            "[RECOVERED] Результат предыдущего "
            "order_send восстановлен из MT5."
        )

        print(
            "[RECOVERED] Повторный order_send "
            "НЕ выполнялся."
        )

    elif report.get(
        "blocked"
    ):

        print()
        print(
            "[FAIL CLOSED] SEND_INTENT не разрешён."
        )

        print(
            "[FAIL CLOSED] Повторный order_send "
            "КАТЕГОРИЧЕСКИ запрещён."
        )

    print("=" * 80)


# ============================================================
# PRINT LIVE EXECUTION
# ============================================================

def print_live_execution_report(
    report: dict,
):
    """
    Печатает итог реального/DRY исполнения.
    """

    print()
    print("=" * 80)
    print(
        "LIVE EXECUTION CONTROL"
    )
    print("=" * 80)

    print(
        f"Mode:              "
        f"{report.get('mode')}"
    )

    print(
        f"Decision:          "
        f"{report.get('decision')}"
    )

    print(
        f"order_send called: "
        f"{report.get('order_send_called')}"
    )

    print(
        f"Position ticket:   "
        f"{report.get('position_ticket')}"
    )

    print(
        f"Pending ticket:    "
        f"{report.get('pending_ticket')}"
    )

    print(
        f"State attached:    "
        f"{report.get('state_attached')}"
    )

    send_intent = (
        report.get(
            "send_intent"
        )
    )

    if send_intent:

        print()
        print("SEND INTENT")
        print("-" * 80)

        print(
            f"Attempt ID:        "
            f"{send_intent.get('send_attempt_id')}"
        )

        print(
            f"Recorded:          "
            f"{send_intent.get('send_intent_at_fp')}"
        )

    order_send_result = (
        report.get(
            "order_send_result"
        )
    )

    if order_send_result:

        print()
        print("MT5 ORDER SEND")
        print("-" * 80)

        print(
            f"Result received:   "
            f"{order_send_result.get('received')}"
        )

        print(
            f"Retcode:           "
            f"{order_send_result.get('retcode')}"
        )

        print(
            f"Comment:           "
            f"{order_send_result.get('comment')}"
        )

        print(
            f"Order ticket:      "
            f"{order_send_result.get('order')}"
        )

        print(
            f"Deal ticket:       "
            f"{order_send_result.get('deal')}"
        )

        print(
            f"Volume:            "
            f"{order_send_result.get('volume')}"
        )

        print(
            f"Price:             "
            f"{order_send_result.get('price')}"
        )

        print(
            f"MT5 last_error:    "
            f"{order_send_result.get('last_error')}"
        )

    reconciliation = (
        report.get(
            "reconciliation"
        )
    )

    if reconciliation:

        print()
        print("RECONCILIATION")
        print("-" * 80)

        print(
            f"Decision:          "
            f"{reconciliation.get('decision')}"
        )

        print(
            f"Resolved:          "
            f"{reconciliation.get('resolved')}"
        )

        print(
            f"Blocked:           "
            f"{reconciliation.get('blocked')}"
        )

        print(
            f"Attempts:          "
            f"{reconciliation.get('attempts')}"
        )

    errors = (
        report.get(
            "errors",
            [],
        )
    )

    if errors:

        print()
        print("ОШИБКИ / ПРЕДУПРЕЖДЕНИЯ")
        print("-" * 80)

        for error in errors:

            print(
                f"- {error}"
            )

    decision = (
        report.get(
            "decision"
        )
    )

    print()
    print("-" * 80)

    if (
        decision
        == "ORDER_SEND_SUCCESS"
    ):

        print(
            "[DEMO LIVE OK] Реальная DEMO-позиция "
            "подтверждена в MT5."
        )

        print(
            "[DEMO LIVE OK] Trade State переведён "
            "в managed_positions."
        )

    elif (
        decision
        == "RECOVERED_EXISTING_EXECUTION"
    ):

        print(
            "[RECOVERED] Уже существовавшее исполнение "
            "восстановлено без повторного send."
        )

    elif (
        decision
        == "DRY_RUN_ONLY"
    ):

        print(
            "[DRY RUN] order_send() не вызывался."
        )

    elif (
        decision
        == "EXECUTION_SAFETY_BLOCKED"
    ):

        print(
            "[BLOCKED] Execution Safety Gate "
            "не разрешил order_send()."
        )

    elif (
        decision
        in (
            "ORDER_SEND_STATE_UNKNOWN",
            "PARTIAL_FILL_UNRESOLVED",
        )
    ):

        print(
            "[FAIL CLOSED] Результат отправки "
            "неоднозначен."
        )

        print(
            "[FAIL CLOSED] SEND_INTENT сохранён; "
            "повторная отправка запрещена."
        )

    elif (
        decision
        == "ORDER_SEND_REJECTED"
    ):

        print(
            "[REJECTED] MT5 однозначно отклонил "
            "market order."
        )

        print(
            "[REJECTED] План завершён без "
            "автоматического повторного send."
        )

    elif (
        decision
        == "PENDING_ORDER_PLACED"
    ):

        print(
            "[DEMO LIVE OK] Pending order реально "
            "выставлен в MT5 и ticket сохранён."
        )

    elif (
        decision
        == "PENDING_ACTIVE"
    ):

        print(
            "[PENDING ACTIVE] Ордер остаётся активным. "
            "Claude не вызывается."
        )

    elif (
        decision
        in (
            "PENDING_FILLED",
            "PENDING_FILLED_IMMEDIATELY",
            "PENDING_FILLED_BEFORE_CANCEL",
        )
    ):

        print(
            "[DEMO LIVE OK] Pending исполнился в позицию. "
            "Trade State переведён в managed_positions."
        )

    elif (
        decision
        in (
            "PENDING_CANCELLED",
            "PENDING_EXPIRED",
            "PENDING_REJECTED",
            "PENDING_PLAN_EXPIRED_BEFORE_SEND",
        )
    ):

        print(
            "[PENDING CLOSED] Pending lifecycle завершён. "
            "Нового entry в этом же цикле нет."
        )

    elif (
        decision
        in (
            "PENDING_ORDER_SEND_STATE_UNKNOWN",
            "PENDING_CANCEL_STATE_UNKNOWN",
            "PENDING_RECONCILIATION_BLOCKED",
            "PENDING_CANCEL_RECONCILIATION_BLOCKED",
            "PENDING_PARTIAL_FILL_UNRESOLVED",
        )
    ):

        print(
            "[FAIL CLOSED] Pending lifecycle неоднозначен. "
            "Новые сделки запрещены до reconciliation."
        )

    else:

        print(
            "[NO SEND] Реальная DEMO-сделка "
            "не была подтверждена."
        )

    print("=" * 80)
