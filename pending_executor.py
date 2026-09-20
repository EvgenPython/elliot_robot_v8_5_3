from __future__ import annotations
from observability import observe, emit

from datetime import datetime, timedelta, timezone
import time

import MetaTrader5 as mt5

from execution_control import (
    EXECUTION_MODE_LIVE,
    EXECUTION_MODE_DRY_RUN,
    inspect_execution_safety_gate,
)

from risk_manager import now_fp

from trade_executor import (
    MAGIC_NUMBER,
    build_order_comment,
    build_early_report,
    validate_active_plan,
    inspect_cancel_pending_action,
    mt5_object_to_dict,
    process_pending_actions_dry_run,
)

from trade_state import (
    ACTION_CANCEL_PENDING,
    EXECUTION_CANCEL_REQUESTED,
    EXECUTION_NOT_SENT,
    EXECUTION_PENDING_SENT,
    EXECUTION_SEND_INTENT,
    PLAN_STATUS_CANCELLED,
    PLAN_STATUS_EXECUTION_FAILED,
    PLAN_STATUS_EXPIRED,
    attach_pending_ticket,
    attach_position_ticket,
    complete_pending_action,
    finalize_active_pending_plan,
    get_active_plan,
    get_pending_actions,
    mark_active_plan_execution_failed,
    mark_active_plan_send_intent,
    record_active_plan_order_send_result,
    request_active_pending_cancellation,
    extract_latest_closed_h1_time,
)


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument

SYMBOL = active_instrument()

PENDING_ORDER_TYPES = (
    "limit",
    "stop",
)

RECONCILIATION_POLL_ATTEMPTS = 8
RECONCILIATION_POLL_DELAY_SECONDS = 0.25
HISTORY_LOOKBACK_HOURS = 72

# A valid pending setup is allowed to work for three closed H1 bars. V8.5.2
# cancelled every limit/stop order on the very next H1 even when neither its
# confirmation nor invalidation had changed; that silently removed many valid
# entries. While it exists, later H1 analysis is observation-only and cannot
# create a competing plan. The runner cancels the order at TTL or session
# cutoff; price invalidation can request an earlier cancellation.
PENDING_MAX_H1_BARS = 3


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
# SAFE DICT
# ============================================================

def result_to_dict(value):
    return mt5_object_to_dict(
        value
    )


# ============================================================
# ORDER SEND RESULT
# ============================================================

def build_order_send_result(result) -> dict:
    """
    Нормализует mt5.order_send().
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
            "last_error": mt5.last_error(),
            "raw": None,
        }

    return {
        "received": True,
        "retcode": int(
            getattr(
                result,
                "retcode",
                -1,
            )
        ),
        "comment": str(
            getattr(
                result,
                "comment",
                "",
            )
        ),
        "order": (
            int(
                getattr(
                    result,
                    "order",
                    0,
                )
            )
            or None
        ),
        "deal": (
            int(
                getattr(
                    result,
                    "deal",
                    0,
                )
            )
            or None
        ),
        "volume": float(
            getattr(
                result,
                "volume",
                0.0,
            )
        ),
        "price": float(
            getattr(
                result,
                "price",
                0.0,
            )
        ),
        "last_error": mt5.last_error(),
        "raw": result_to_dict(
            result
        ),
    }


# ============================================================
# EXPECTED TYPES
# ============================================================

def get_expected_pending_order_type(
    action: str,
    order_type: str,
) -> int | None:
    if action == "enter_long" and order_type == "limit":
        return int(
            mt5.ORDER_TYPE_BUY_LIMIT
        )

    if action == "enter_short" and order_type == "limit":
        return int(
            mt5.ORDER_TYPE_SELL_LIMIT
        )

    if action == "enter_long" and order_type == "stop":
        return int(
            mt5.ORDER_TYPE_BUY_STOP
        )

    if action == "enter_short" and order_type == "stop":
        return int(
            mt5.ORDER_TYPE_SELL_STOP
        )

    return None


def get_expected_position_type(
    action: str,
) -> int | None:
    if action == "enter_long":
        return int(
            mt5.POSITION_TYPE_BUY
        )

    if action == "enter_short":
        return int(
            mt5.POSITION_TYPE_SELL
        )

    return None


def get_expected_deal_type(
    action: str,
) -> int | None:
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
    info = mt5.symbol_info(
        symbol
    )

    if info is None:
        return (
            0.00001,
            0.00000001,
        )

    return (
        max(
            float(
                info.point
            )
            * 2.0,
            0.00001,
        ),
        max(
            float(
                info.volume_step
            )
            / 10.0,
            0.00000001,
        ),
    )


# ============================================================
# EXPECTED REQUEST
# ============================================================

def get_expected_request(
    plan: dict,
) -> dict:
    return dict(
        plan.get(
            "send_request"
        )
        or {}
    )


# ============================================================
# OWNERSHIP
# ============================================================

def is_owned_by_plan(
    item,
    plan: dict,
    require_exact_comment: bool = True,
) -> bool:
    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    expected_comment = (
        get_expected_request(
            plan
        ).get(
            "comment"
        )
        or build_order_comment(
            plan
        )
    )

    actual_comment = str(
        getattr(
            item,
            "comment",
            "",
        )
    )

    if require_exact_comment:
        comment_ok = (
            actual_comment
            == str(
                expected_comment
            )
        )
    else:
        comment_ok = (
            not actual_comment
            or actual_comment
            == str(
                expected_comment
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
        == symbol
        and int(
            getattr(
                item,
                "magic",
                -1,
            )
        )
        == int(
            MAGIC_NUMBER
        )
        and comment_ok
    )


# ============================================================
# VALIDATE ACTIVE PENDING ORDER
# ============================================================

def validate_active_pending_order(
    order,
    plan: dict,
) -> list[str]:
    errors = []

    request = get_expected_request(
        plan
    )

    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    price_tolerance, volume_tolerance = (
        get_symbol_tolerances(
            symbol
        )
    )

    expected_type = (
        get_expected_pending_order_type(
            str(
                plan.get(
                    "action",
                    "",
                )
            ),
            str(
                plan.get(
                    "order_type",
                    "",
                )
            ),
        )
    )

    if expected_type is None:
        errors.append(
            "Trade Plan не содержит поддерживаемый pending type."
        )
    elif int(
        getattr(
            order,
            "type",
            -1,
        )
    ) != expected_type:
        errors.append(
            "Тип active pending order не совпадает "
            "с Trade Plan."
        )

    expected_ticket = plan.get(
        "pending_ticket"
    )

    if (
        expected_ticket is not None
        and int(
            getattr(
                order,
                "ticket",
                0,
            )
        )
        != int(
            expected_ticket
        )
    ):
        errors.append(
            "Ticket active pending order не совпадает "
            "с Trade State."
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
            order,
            "volume_initial",
            getattr(
                order,
                "volume_current",
                0.0,
            ),
        )
    )

    if abs(
        actual_volume
        - expected_volume
    ) > volume_tolerance:
        errors.append(
            "Volume active pending order не совпадает: "
            f"MT5={actual_volume}, expected={expected_volume}."
        )

    expected_price = float(
        request.get(
            "price",
            plan.get(
                "entry_price",
                0.0,
            ),
        )
    )

    actual_price = float(
        getattr(
            order,
            "price_open",
            0.0,
        )
    )

    if abs(
        actual_price
        - expected_price
    ) > price_tolerance:
        errors.append(
            "Entry active pending order не совпадает: "
            f"MT5={actual_price}, expected={expected_price}."
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
            order,
            "sl",
            0.0,
        )
    )
    actual_tp = float(
        getattr(
            order,
            "tp",
            0.0,
        )
    )

    if abs(
        actual_sl
        - expected_sl
    ) > price_tolerance:
        errors.append(
            "SL active pending order не совпадает: "
            f"MT5={actual_sl}, expected={expected_sl}."
        )

    if abs(
        actual_tp
        - expected_tp
    ) > price_tolerance:
        errors.append(
            "TP active pending order не совпадает: "
            f"MT5={actual_tp}, expected={expected_tp}."
        )

    if not is_owned_by_plan(
        order,
        plan,
        require_exact_comment=True,
    ):
        errors.append(
            "Ownership active pending order не подтверждён "
            "по symbol + magic + exact comment."
        )

    return errors


# ============================================================
# VALIDATE POSITION
# ============================================================

def validate_position_against_plan(
    position,
    plan: dict,
) -> list[str]:
    errors = []

    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    request = get_expected_request(
        plan
    )

    price_tolerance, volume_tolerance = (
        get_symbol_tolerances(
            symbol
        )
    )

    expected_type = get_expected_position_type(
        str(
            plan.get(
                "action",
                "",
            )
        )
    )

    if (
        expected_type is None
        or int(
            getattr(
                position,
                "type",
                -1,
            )
        )
        != expected_type
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

    if abs(
        actual_volume
        - expected_volume
    ) > volume_tolerance:
        errors.append(
            "Volume позиции не совпадает с request: "
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
        or abs(
            actual_sl
            - expected_sl
        ) > price_tolerance
    ):
        errors.append(
            "SL позиции не совпадает с request."
        )

    if (
        actual_tp <= 0
        or abs(
            actual_tp
            - expected_tp
        ) > price_tolerance
    ):
        errors.append(
            "TP позиции не совпадает с request."
        )

    if not is_owned_by_plan(
        position,
        plan,
        require_exact_comment=True,
    ):
        errors.append(
            "Ownership позиции не подтверждён "
            "по symbol + magic + exact comment."
        )

    return errors


# ============================================================
# ACTIVE ORDER LOOKUP
# ============================================================

def find_active_pending_orders(
    plan: dict,
) -> dict:
    symbol = str(
        plan.get(
            "symbol",
            SYMBOL,
        )
    )

    pending_ticket = plan.get(
        "pending_ticket"
    )

    if pending_ticket is not None:
        orders = mt5.orders_get(
            ticket=int(
                pending_ticket
            )
        )
    else:
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
            "valid": [],
            "invalid": [],
        }

    valid = []
    invalid = []

    for order in orders:
        if pending_ticket is None:
            if not is_owned_by_plan(
                order,
                plan,
                require_exact_comment=True,
            ):
                continue

        errors = validate_active_pending_order(
            order,
            plan,
        )

        item = {
            "raw": order,
            "dict": result_to_dict(
                order
            ),
            "errors": errors,
        }

        if errors:
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
        "valid": valid,
        "invalid": invalid,
    }


# ============================================================
# POSITION LOOKUP
# ============================================================

def find_owned_positions(
    plan: dict,
) -> dict:
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
            "valid": [],
            "invalid": [],
        }

    valid = []
    invalid = []

    for position in positions:
        if not is_owned_by_plan(
            position,
            plan,
            require_exact_comment=True,
        ):
            continue

        errors = validate_position_against_plan(
            position,
            plan,
        )

        item = {
            "raw": position,
            "dict": result_to_dict(
                position
            ),
            "errors": errors,
        }

        if errors:
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
        "valid": valid,
        "invalid": invalid,
    }


# ============================================================
# HISTORY ORDER
# ============================================================

def get_history_order_for_plan(
    plan: dict,
) -> dict:
    ticket = plan.get(
        "pending_ticket"
    )

    if ticket is None:
        order_send_result = plan.get(
            "order_send_result"
        ) or {}
        ticket = order_send_result.get(
            "order"
        )

    orders = None

    if ticket is not None:
        orders = mt5.history_orders_get(
            ticket=int(
                ticket
            )
        )
    else:
        now_local = datetime.now(timezone.utc)
        orders = mt5.history_orders_get(
            now_local
            - timedelta(
                hours=HISTORY_LOOKBACK_HOURS
            ),
            now_local
            + timedelta(
                minutes=5
            ),
            group=str(
                plan.get(
                    "symbol",
                    SYMBOL,
                )
            ),
        )

    if orders is None:
        return {
            "ok": False,
            "error": (
                "history_orders_get() вернул None. "
                f"MT5 error: {mt5.last_error()}"
            ),
            "matches": [],
        }

    matches = []

    for order in orders:
        if ticket is not None:
            if int(
                getattr(
                    order,
                    "ticket",
                    0,
                )
            ) != int(
                ticket
            ):
                continue

            if str(
                getattr(
                    order,
                    "symbol",
                    "",
                )
            ) != str(
                plan.get(
                    "symbol",
                    SYMBOL,
                )
            ):
                continue

            if int(
                getattr(
                    order,
                    "magic",
                    -1,
                )
            ) != int(
                MAGIC_NUMBER
            ):
                continue

            expected_type = get_expected_pending_order_type(
                str(
                    plan.get(
                        "action",
                        "",
                    )
                ),
                str(
                    plan.get(
                        "order_type",
                        "",
                    )
                ),
            )

            if (
                expected_type is not None
                and int(
                    getattr(
                        order,
                        "type",
                        -1,
                    )
                ) != expected_type
            ):
                continue

            expected_comment = build_order_comment(
                plan
            )
            actual_comment = str(
                getattr(
                    order,
                    "comment",
                    "",
                )
            )

            if (
                actual_comment
                and actual_comment != expected_comment
            ):
                continue
        else:
            if not is_owned_by_plan(
                order,
                plan,
                require_exact_comment=True,
            ):
                continue

        matches.append(
            {
                "raw": order,
                "dict": result_to_dict(
                    order
                ),
            }
        )

    return {
        "ok": True,
        "error": None,
        "matches": matches,
    }


# ============================================================
# ENTRY DEALS
# ============================================================

def find_entry_deals_for_plan(
    plan: dict,
) -> dict:
    ticket = plan.get(
        "pending_ticket"
    )

    if ticket is None:
        order_send_result = plan.get(
            "order_send_result"
        ) or {}
        ticket = order_send_result.get(
            "order"
        )

    lookup_note = None

    if ticket is not None:
        # MetaTrader5 documents ``ticket=`` for history_deals_get(), but
        # some terminal/Python-package combinations return
        # (-2, "Terminal: Invalid params") for this overload.  Never let
        # that compatibility issue strand a durable pending plan.  Fall back
        # to the universally supported date-range overload and then filter
        # strictly by DEAL_ORDER below.
        deals = mt5.history_deals_get(
            ticket=int(
                ticket
            )
        )

        if deals is None:
            direct_lookup_error = mt5.last_error()
            now_local = datetime.now(timezone.utc)
            deals = mt5.history_deals_get(
                now_local
                - timedelta(
                    hours=max(
                        HISTORY_LOOKBACK_HOURS,
                        24 * 30,
                    )
                ),
                now_local
                + timedelta(
                    minutes=5
                ),
                group=str(
                    plan.get(
                        "symbol",
                        SYMBOL,
                    )
                ),
            )
            lookup_note = (
                "history_deals_get(ticket=...) unavailable; "
                "used date-range fallback. Direct MT5 error: "
                f"{direct_lookup_error}"
            )
    else:
        now_local = datetime.now(timezone.utc)
        deals = mt5.history_deals_get(
            now_local
            - timedelta(
                hours=HISTORY_LOOKBACK_HOURS
            ),
            now_local
            + timedelta(
                minutes=5
            ),
            group=str(
                plan.get(
                    "symbol",
                    SYMBOL,
                )
            ),
        )

    if deals is None:
        return {
            "ok": False,
            "error": (
                "history_deals_get() вернул None. "
                f"MT5 error: {mt5.last_error()}"
            ),
            "matches": [],
        }

    expected_type = get_expected_deal_type(
        str(
            plan.get(
                "action",
                "",
            )
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

    expected_volume = float(
        get_expected_request(
            plan
        ).get(
            "volume",
            plan.get(
                "volume",
                0.0,
            ),
        )
    )

    _, volume_tolerance = get_symbol_tolerances(
        str(
            plan.get(
                "symbol",
                SYMBOL,
            )
        )
    )

    matches = []

    for deal in deals:
        if (
            ticket is not None
            and int(
                getattr(
                    deal,
                    "order",
                    0,
                )
            )
            != int(ticket)
        ):
            continue

        if int(
            getattr(
                deal,
                "entry",
                -1,
            )
        ) not in (
            entry_in,
            entry_inout,
        ):
            continue

        if (
            expected_type is not None
            and int(
                getattr(
                    deal,
                    "type",
                    -1,
                )
            ) != expected_type
        ):
            continue

        if str(
            getattr(
                deal,
                "symbol",
                "",
            )
        ) != str(
            plan.get(
                "symbol",
                SYMBOL,
            )
        ):
            continue

        if int(
            getattr(
                deal,
                "magic",
                -1,
            )
        ) != int(
            MAGIC_NUMBER
        ):
            continue

        if ticket is None:
            expected_comment = build_order_comment(
                plan
            )
            if str(
                getattr(
                    deal,
                    "comment",
                    "",
                )
            ) != str(
                expected_comment
            ):
                continue

        actual_volume = float(
            getattr(
                deal,
                "volume",
                0.0,
            )
        )

        if abs(
            actual_volume
            - expected_volume
        ) > volume_tolerance:
            continue

        matches.append(
            {
                "raw": deal,
                "dict": result_to_dict(
                    deal
                ),
            }
        )

    return {
        "ok": True,
        "error": None,
        "matches": matches,
        "lookup_note": lookup_note,
    }


# ============================================================
# CLEANUP CANCEL ACTIONS
# ============================================================

def complete_cancel_actions_for_plan(
    plan_id: str,
    success: bool,
    note: str,
    mt5_result=None,
):
    actions = get_pending_actions(
        action_type=ACTION_CANCEL_PENDING
    )

    for action in actions:
        if str(
            action.get(
                "plan_id",
                "",
            )
        ) != str(
            plan_id
        ):
            continue

        try:
            complete_pending_action(
                action_id=str(
                    action[
                        "action_id"
                    ]
                ),
                success=success,
                result_note=note,
                mt5_result=mt5_result,
            )
        except RuntimeError:
            pass


# ============================================================
# ATTACH POSITION
# ============================================================

def attach_live_position(
    plan: dict,
    position,
    order_ticket: int | None = None,
    deal_ticket: int | None = None,
) -> dict:
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

    complete_cancel_actions_for_plan(
        plan_id=str(
            plan[
                "plan_id"
            ]
        ),
        success=True,
        note=(
            "Pending исполнился в позицию; "
            "cancel action больше не требуется."
        ),
    )

    return {
        "attached": True,
        "position_ticket": ticket,
        "position": result_to_dict(
            position
        ),
    }


def attach_historical_position(
    plan: dict,
    deal,
) -> dict:
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

    request = get_expected_request(
        plan
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

    complete_cancel_actions_for_plan(
        plan_id=str(
            plan[
                "plan_id"
            ]
        ),
        success=True,
        note=(
            "Pending уже исполнился; "
            "cancel action закрыт reconciliation."
        ),
    )

    return {
        "attached": True,
        "position_ticket": position_ticket,
        "deal": result_to_dict(
            deal
        ),
    }


# ============================================================
# HISTORY STATE
# ============================================================

def classify_history_order_state(
    history_order,
) -> str:
    state = int(
        getattr(
            history_order,
            "state",
            -1,
        )
    )

    if state == int(
        getattr(
            mt5,
            "ORDER_STATE_CANCELED",
            2,
        )
    ):
        return "CANCELLED"

    if state == int(
        getattr(
            mt5,
            "ORDER_STATE_EXPIRED",
            6,
        )
    ):
        return "EXPIRED"

    if state == int(
        getattr(
            mt5,
            "ORDER_STATE_REJECTED",
            5,
        )
    ):
        return "REJECTED"

    if state == int(
        getattr(
            mt5,
            "ORDER_STATE_FILLED",
            4,
        )
    ):
        return "FILLED"

    if state == int(
        getattr(
            mt5,
            "ORDER_STATE_PARTIAL",
            3,
        )
    ):
        return "PARTIAL"

    return "OTHER"


def resolve_terminal_history_order(
    plan: dict,
    history_order: dict,
) -> dict | None:
    """Finalize terminal pending states before any deal lookup.

    A cancelled, expired or rejected history order is already conclusive MT5
    evidence.  Looking for an entry deal first is both unnecessary and, on
    affected terminals, can fail because ``history_deals_get(ticket=...)`` is
    rejected as an invalid overload.
    """
    plan_id = str(plan.get("plan_id", ""))
    raw = history_order["raw"]
    snapshot = history_order["dict"]
    classification = classify_history_order_state(raw)
    pending_ticket = plan.get("pending_ticket")

    if classification == "CANCELLED":
        finalize_active_pending_plan(
            final_status=PLAN_STATUS_CANCELLED,
            reason="MT5 history подтверждает отмену pending order.",
            mt5_history_order=snapshot,
        )
        complete_cancel_actions_for_plan(
            plan_id=plan_id,
            success=True,
            note="Отмена подтверждена MT5 history.",
            mt5_result=snapshot,
        )
        return {
            "resolved": True,
            "blocked": False,
            "decision": "PENDING_CANCELLED",
            "errors": [],
            "position_ticket": None,
            "pending_ticket": pending_ticket,
            "evidence": snapshot,
        }

    if classification == "EXPIRED":
        finalize_active_pending_plan(
            final_status=PLAN_STATUS_EXPIRED,
            reason="MT5 history подтверждает expiration pending order.",
            mt5_history_order=snapshot,
        )
        complete_cancel_actions_for_plan(
            plan_id=plan_id,
            success=True,
            note="Pending уже истёк в MT5.",
            mt5_result=snapshot,
        )
        return {
            "resolved": True,
            "blocked": False,
            "decision": "PENDING_EXPIRED",
            "errors": [],
            "position_ticket": None,
            "pending_ticket": pending_ticket,
            "evidence": snapshot,
        }

    if classification == "REJECTED":
        finalize_active_pending_plan(
            final_status=PLAN_STATUS_EXECUTION_FAILED,
            reason="MT5 history подтверждает rejected pending order.",
            mt5_history_order=snapshot,
        )
        complete_cancel_actions_for_plan(
            plan_id=plan_id,
            success=False,
            note="Pending order был rejected MT5.",
            mt5_result=snapshot,
        )
        return {
            "resolved": True,
            "blocked": False,
            "decision": "PENDING_REJECTED",
            "errors": [],
            "position_ticket": None,
            "pending_ticket": pending_ticket,
            "evidence": snapshot,
        }

    if classification == "PARTIAL":
        return {
            "resolved": False,
            "blocked": True,
            "decision": "PENDING_PARTIAL_FILL_UNRESOLVED",
            "errors": [
                "MT5 history показывает partial fill. Автоматическая "
                "обработка partial fill запрещена."
            ],
            "position_ticket": None,
            "pending_ticket": pending_ticket,
            "evidence": snapshot,
        }

    return None


# ============================================================
# RECONCILE PENDING PLAN
# ============================================================

def reconcile_pending_plan(
    plan: dict,
) -> dict:
    """
    MT5 является источником истины.

    Приоритет:
        1. position;
        2. active pending order;
        3. conclusive history order state;
        4. exact entry deal;
        5. fail closed.
    """

    plan_id = str(
        plan.get(
            "plan_id",
            "",
        )
    )

    positions = find_owned_positions(
        plan
    )

    if not positions[
        "ok"
    ]:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "RECONCILIATION_ERROR",
            "errors": [
                positions[
                    "error"
                ]
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if positions[
        "invalid"
    ]:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "POSITION_MISMATCH",
            "errors": [
                error
                for item in positions[
                    "invalid"
                ]
                for error in item[
                    "errors"
                ]
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if len(
        positions[
            "valid"
        ]
    ) > 1:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "MULTIPLE_MATCHING_POSITIONS",
            "errors": [
                "Найдено несколько позиций этого plan_id."
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if len(
        positions[
            "valid"
        ]
    ) == 1:
        item = positions[
            "valid"
        ][0]
        attached = attach_live_position(
            plan=plan,
            position=item[
                "raw"
            ],
            order_ticket=(
                int(
                    plan.get(
                        "pending_ticket",
                        0,
                    )
                    or 0
                )
                or None
            ),
        )

        return {
            "resolved": True,
            "blocked": False,
            "decision": "PENDING_FILLED_POSITION_RECOVERED",
            "errors": [],
            "position_ticket": attached[
                "position_ticket"
            ],
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
            "evidence": attached,
        }

    orders = find_active_pending_orders(
        plan
    )

    if not orders[
        "ok"
    ]:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "RECONCILIATION_ERROR",
            "errors": [
                orders[
                    "error"
                ]
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if orders[
        "invalid"
    ]:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "PENDING_ORDER_MISMATCH",
            "errors": [
                error
                for item in orders[
                    "invalid"
                ]
                for error in item[
                    "errors"
                ]
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if len(
        orders[
            "valid"
        ]
    ) > 1:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "MULTIPLE_MATCHING_PENDING_ORDERS",
            "errors": [
                "Найдено несколько active pending orders "
                "этого plan_id."
            ],
            "position_ticket": None,
            "pending_ticket": None,
        }

    if len(
        orders[
            "valid"
        ]
    ) == 1:
        item = orders[
            "valid"
        ][0]
        order = item[
            "raw"
        ]
        ticket = int(
            order.ticket
        )

        attach_pending_ticket(
            plan_id=plan_id,
            ticket=ticket,
            order_snapshot=item[
                "dict"
            ],
        )

        return {
            "resolved": True,
            "blocked": False,
            "decision": "PENDING_ACTIVE",
            "errors": [],
            "position_ticket": None,
            "pending_ticket": ticket,
            "evidence": item[
                "dict"
            ],
        }

    # A terminal history-order state is stronger and cheaper evidence than a
    # deal lookup.  In particular, CANCELLED/EXPIRED/REJECTED must close the
    # durable local lifecycle immediately.  This ordering prevents an MT5
    # compatibility error in history_deals_get(ticket=...) from masking a
    # conclusively cancelled order (the production V8.4 incident).
    terminal_history = get_history_order_for_plan(plan)
    if terminal_history.get("ok"):
        terminal_matches = terminal_history.get("matches", [])

        if len(terminal_matches) > 1:
            return {
                "resolved": False,
                "blocked": True,
                "decision": "MULTIPLE_HISTORY_ORDERS",
                "errors": [
                    "Найдено несколько history orders для одного "
                    "pending ticket."
                ],
                "position_ticket": None,
                "pending_ticket": plan.get("pending_ticket"),
            }

        if len(terminal_matches) == 1:
            terminal_result = resolve_terminal_history_order(
                plan,
                terminal_matches[0],
            )
            if terminal_result is not None:
                return terminal_result

    deals = find_entry_deals_for_plan(
        plan
    )

    if not deals[
        "ok"
    ]:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "RECONCILIATION_ERROR",
            "errors": [
                deals[
                    "error"
                ]
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if len(
        deals[
            "matches"
        ]
    ) > 1:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "MULTIPLE_ENTRY_DEALS_FOUND",
            "errors": [
                "Найдено несколько входных deals. "
                "Возможен partial fill; автоматическое "
                "продолжение запрещено."
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if len(
        deals[
            "matches"
        ]
    ) == 1:
        attached = attach_historical_position(
            plan=plan,
            deal=deals[
                "matches"
            ][0][
                "raw"
            ],
        )

        return {
            "resolved": True,
            "blocked": False,
            "decision": "PENDING_FILLED_HISTORY_RECOVERED",
            "errors": [],
            "position_ticket": attached[
                "position_ticket"
            ],
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
            "evidence": attached,
        }

    history = get_history_order_for_plan(
        plan
    )

    if not history[
        "ok"
    ]:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "RECONCILIATION_ERROR",
            "errors": [
                history[
                    "error"
                ]
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if len(
        history[
            "matches"
        ]
    ) > 1:
        return {
            "resolved": False,
            "blocked": True,
            "decision": "MULTIPLE_HISTORY_ORDERS",
            "errors": [
                "Найдено несколько history orders "
                "для одного pending ticket."
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
        }

    if len(
        history[
            "matches"
        ]
    ) == 1:
        history_order = history[
            "matches"
        ][0]
        classification = classify_history_order_state(
            history_order[
                "raw"
            ]
        )

        if classification == "CANCELLED":
            finalize_active_pending_plan(
                final_status=PLAN_STATUS_CANCELLED,
                reason=(
                    "MT5 history подтверждает отмену "
                    "pending order."
                ),
                mt5_history_order=history_order[
                    "dict"
                ],
            )
            complete_cancel_actions_for_plan(
                plan_id=plan_id,
                success=True,
                note="Отмена подтверждена MT5 history.",
                mt5_result=history_order[
                    "dict"
                ],
            )

            return {
                "resolved": True,
                "blocked": False,
                "decision": "PENDING_CANCELLED",
                "errors": [],
                "position_ticket": None,
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "evidence": history_order[
                    "dict"
                ],
            }

        if classification == "EXPIRED":
            finalize_active_pending_plan(
                final_status=PLAN_STATUS_EXPIRED,
                reason=(
                    "MT5 history подтверждает expiration "
                    "pending order."
                ),
                mt5_history_order=history_order[
                    "dict"
                ],
            )
            complete_cancel_actions_for_plan(
                plan_id=plan_id,
                success=True,
                note="Pending уже истёк в MT5.",
                mt5_result=history_order[
                    "dict"
                ],
            )

            return {
                "resolved": True,
                "blocked": False,
                "decision": "PENDING_EXPIRED",
                "errors": [],
                "position_ticket": None,
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "evidence": history_order[
                    "dict"
                ],
            }

        if classification == "REJECTED":
            finalize_active_pending_plan(
                final_status=PLAN_STATUS_EXECUTION_FAILED,
                reason=(
                    "MT5 history подтверждает rejected "
                    "pending order."
                ),
                mt5_history_order=history_order[
                    "dict"
                ],
            )
            complete_cancel_actions_for_plan(
                plan_id=plan_id,
                success=False,
                note="Pending order был rejected MT5.",
                mt5_result=history_order[
                    "dict"
                ],
            )

            return {
                "resolved": True,
                "blocked": False,
                "decision": "PENDING_REJECTED",
                "errors": [],
                "position_ticket": None,
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "evidence": history_order[
                    "dict"
                ],
            }

        if classification == "PARTIAL":
            return {
                "resolved": False,
                "blocked": True,
                "decision": "PENDING_PARTIAL_FILL_UNRESOLVED",
                "errors": [
                    "MT5 history показывает partial fill. "
                    "Автоматическая обработка partial fill "
                    "на этом этапе запрещена."
                ],
                "position_ticket": None,
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "evidence": history_order[
                    "dict"
                ],
            }

        if classification == "FILLED":
            return {
                "resolved": False,
                "blocked": True,
                "decision": "FILLED_ORDER_WITHOUT_ENTRY_DEAL",
                "errors": [
                    "History order имеет FILLED, но входной deal "
                    "не найден. Состояние неоднозначно."
                ],
                "position_ticket": None,
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "evidence": history_order[
                    "dict"
                ],
            }

        return {
            "resolved": False,
            "blocked": True,
            "decision": "UNKNOWN_HISTORY_ORDER_STATE",
            "errors": [
                "History order найден, но его state пока "
                "не классифицирован безопасно."
            ],
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
            "evidence": history_order[
                "dict"
            ],
        }

    return {
        "resolved": False,
        "blocked": True,
        "decision": "NO_PENDING_EXECUTION_EVIDENCE",
        "errors": [
            "Trade State указывает на отправленный pending, "
            "но MT5 не показывает ни active order, ни position, "
            "ни deal/history order. Повторный entry запрещён."
        ],
        "position_ticket": None,
        "pending_ticket": plan.get(
            "pending_ticket"
        ),
    }


# ============================================================
# STARTUP RECONCILIATION
# ============================================================

@observe("execution")
def reconcile_active_pending_execution(
    symbol: str = SYMBOL,
) -> dict:
    plan = get_active_plan()

    if plan is None:
        return {
            "applicable": False,
            "resolved": False,
            "blocked": False,
            "decision": "NO_ACTIVE_PLAN",
            "plan_id": None,
            "pending_ticket": None,
            "position_ticket": None,
            "errors": [],
        }

    if str(
        plan.get(
            "symbol",
            symbol,
        )
    ) != str(
        symbol
    ):
        return {
            "applicable": False,
            "resolved": False,
            "blocked": True,
            "decision": "ACTIVE_PLAN_SYMBOL_MISMATCH",
            "plan_id": plan.get(
                "plan_id"
            ),
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
            "position_ticket": None,
            "errors": [
                "active_plan имеет другой symbol."
            ],
        }

    if str(
        plan.get(
            "order_type",
            "",
        )
    ) not in PENDING_ORDER_TYPES:
        return {
            "applicable": False,
            "resolved": False,
            "blocked": False,
            "decision": "ACTIVE_PLAN_NOT_PENDING",
            "plan_id": plan.get(
                "plan_id"
            ),
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
            "position_ticket": None,
            "errors": [],
        }

    execution_status = plan.get(
        "execution_status"
    )

    if execution_status == EXECUTION_NOT_SENT:
        return {
            "applicable": True,
            "resolved": False,
            "blocked": False,
            "decision": "PENDING_NOT_SENT",
            "plan_id": plan.get(
                "plan_id"
            ),
            "pending_ticket": None,
            "position_ticket": None,
            "errors": [],
        }

    if execution_status not in (
        EXECUTION_SEND_INTENT,
        EXECUTION_PENDING_SENT,
        EXECUTION_CANCEL_REQUESTED,
    ):
        return {
            "applicable": True,
            "resolved": False,
            "blocked": True,
            "decision": "UNSUPPORTED_PENDING_EXECUTION_STATE",
            "plan_id": plan.get(
                "plan_id"
            ),
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
            "position_ticket": None,
            "errors": [
                "Неожиданный execution_status pending-plan: "
                f"{execution_status}."
            ],
        }

    reconciliation = reconcile_pending_plan(
        plan
    )

    return {
        "applicable": True,
        "resolved": reconciliation.get(
            "resolved",
            False,
        ),
        "blocked": reconciliation.get(
            "blocked",
            False,
        ),
        "decision": reconciliation.get(
            "decision"
        ),
        "plan_id": plan.get(
            "plan_id"
        ),
        "pending_ticket": reconciliation.get(
            "pending_ticket"
        ),
        "position_ticket": reconciliation.get(
            "position_ticket"
        ),
        "errors": reconciliation.get(
            "errors",
            [],
        ),
        "evidence": reconciliation.get(
            "evidence"
        ),
    }


def print_pending_startup_reconciliation(
    report: dict,
):
    if not report.get(
        "applicable",
        False,
    ):
        return

    print()
    print("=" * 80)
    print(
        "PENDING STARTUP RECONCILIATION"
    )
    print("=" * 80)
    print(
        f"Decision:          {report.get('decision')}"
    )
    print(
        f"Resolved:          {report.get('resolved')}"
    )
    print(
        f"Blocked:           {report.get('blocked')}"
    )
    print(
        f"Plan ID:           {report.get('plan_id')}"
    )
    print(
        f"Pending ticket:    {report.get('pending_ticket')}"
    )
    print(
        f"Position ticket:   {report.get('position_ticket')}"
    )

    errors = report.get(
        "errors",
        [],
    )

    if errors:
        print()
        print("ОШИБКИ")
        print("-" * 80)
        for error in errors:
            print(
                f"- {error}"
            )

    print("=" * 80)


# ============================================================
# PENDING H1 FRESHNESS
# ============================================================

def inspect_pending_h1_validity(plan: dict, current_h1) -> dict:
    source_h1 = plan.get(
        "source_h1_closed_bar_time"
    )

    try:
        source_dt = datetime.fromisoformat(str(source_h1))
        current_dt = datetime.fromisoformat(str(current_h1))
        if source_dt.tzinfo is None or current_dt.tzinfo is None:
            raise ValueError("timezone is required")
        elapsed_seconds = (current_dt - source_dt).total_seconds()
        age_h1_bars = int(elapsed_seconds // 3600)
        valid = 0 <= elapsed_seconds and age_h1_bars < PENDING_MAX_H1_BARS
    except (TypeError, ValueError):
        elapsed_seconds = None
        age_h1_bars = None
        valid = False

    return {
        "same_h1": (
            bool(
                source_h1
            )
            and bool(
                current_h1
            )
            and str(
                source_h1
            )
            == str(
                current_h1
            )
        ),
        "valid": valid,
        "age_h1_bars": age_h1_bars,
        "max_h1_bars": PENDING_MAX_H1_BARS,
        "elapsed_seconds": elapsed_seconds,
        "source_h1": source_h1,
        "current_h1": current_h1,
    }


def pending_plan_belongs_to_current_h1(
    plan: dict,
    snapshot: dict,
) -> dict:
    """Backward-compatible name returning the full multi-H1 validity."""

    return inspect_pending_h1_validity(
        plan,
        extract_latest_closed_h1_time(snapshot),
    )


# ============================================================
# VALIDATION REPORT FOR MONITORING
# ============================================================

def build_pending_monitor_report(
    plan: dict | None,
    decision: str,
    errors: list[str] | None = None,
    warnings: list[str] | None = None,
) -> dict:
    return build_early_report(
        decision=decision,
        errors=list(
            errors
            or []
        ),
        warnings=list(
            warnings
            or []
        ),
        pending_actions_report=(
            process_pending_actions_dry_run()
        ),
        plan=plan,
        expiration=None,
    )


# ============================================================
# EXECUTE CANCEL
# ============================================================

@observe("execution")
def execute_pending_cancel(
    plan: dict,
    reason: str,
) -> dict:
    execution_safety = inspect_execution_safety_gate()
    mode = str(
        execution_safety.get(
            "mode",
            EXECUTION_MODE_DRY_RUN,
        )
    ).upper()

    cancel_request = request_active_pending_cancellation(
        reason=reason
    )

    action = cancel_request.get(
        "action"
    )

    validation_report = build_pending_monitor_report(
        plan=get_active_plan(),
        decision="PENDING_CANCEL_REQUIRED",
        warnings=[
            reason
        ],
    )

    result = {
        "mode": mode,
        "decision": "PENDING_CANCEL_REQUIRED",
        "validation_report": validation_report,
        "execution_safety": execution_safety,
        "order_send_called": False,
        "order_send_result": None,
        "position_ticket": None,
        "pending_ticket": plan.get(
            "pending_ticket"
        ),
        "state_attached": False,
        "send_intent": None,
        "reconciliation": None,
        "errors": [],
        "warnings": [
            reason
        ],
    }

    current_plan = get_active_plan()

    if current_plan is None:
        result[
            "decision"
        ] = "PENDING_ALREADY_RESOLVED"
        return result

    reconciliation = reconcile_pending_plan(
        current_plan
    )
    result[
        "reconciliation"
    ] = reconciliation

    if reconciliation.get(
        "decision"
    ) in (
        "PENDING_FILLED_POSITION_RECOVERED",
        "PENDING_FILLED_HISTORY_RECOVERED",
    ):
        result[
            "decision"
        ] = "PENDING_FILLED_BEFORE_CANCEL"
        result[
            "position_ticket"
        ] = reconciliation.get(
            "position_ticket"
        )
        result[
            "state_attached"
        ] = True
        return result

    if reconciliation.get(
        "decision"
    ) in (
        "PENDING_CANCELLED",
        "PENDING_EXPIRED",
        "PENDING_REJECTED",
    ):
        result[
            "decision"
        ] = reconciliation.get(
            "decision"
        )
        return result

    if reconciliation.get(
        "blocked",
        False,
    ) and reconciliation.get(
        "decision"
    ) != "PENDING_ACTIVE":
        result[
            "decision"
        ] = "PENDING_CANCEL_RECONCILIATION_BLOCKED"
        result[
            "errors"
        ].extend(
            reconciliation.get(
                "errors",
                [],
            )
        )
        return result

    if action is None:
        result[
            "decision"
        ] = "PENDING_CANCEL_ACTION_MISSING"
        result[
            "errors"
        ].append(
            "Не удалось получить cancel_pending action."
        )
        return result

    cancel_check = inspect_cancel_pending_action(
        action
    )

    if cancel_check.get(
        "decision"
    ) == "ORDER_NOT_ACTIVE_RECONCILIATION_REQUIRED":
        refreshed = get_active_plan()
        if refreshed is None:
            result[
                "decision"
            ] = "PENDING_ALREADY_RESOLVED"
            return result

        reconciliation = reconcile_pending_plan(
            refreshed
        )
        result[
            "reconciliation"
        ] = reconciliation

        if reconciliation.get(
            "resolved",
            False,
        ):
            result[
                "decision"
            ] = reconciliation.get(
                "decision"
            )
            result[
                "position_ticket"
            ] = reconciliation.get(
                "position_ticket"
            )
            result[
                "state_attached"
            ] = bool(
                reconciliation.get(
                    "position_ticket"
                )
            )
            return result

        result[
            "decision"
        ] = "PENDING_CANCEL_RECONCILIATION_BLOCKED"
        result[
            "errors"
        ].extend(
            reconciliation.get(
                "errors",
                [],
            )
        )
        return result

    if cancel_check.get(
        "decision"
    ) != "CANCEL_CHECK_PASSED":
        result[
            "decision"
        ] = "PENDING_CANCEL_VALIDATION_BLOCKED"
        result[
            "errors"
        ].extend(
            cancel_check.get(
                "errors",
                [],
            )
        )
        result[
            "warnings"
        ].extend(
            cancel_check.get(
                "warnings",
                [],
            )
        )
        return result

    if mode != EXECUTION_MODE_LIVE:
        result[
            "decision"
        ] = "DRY_RUN_PENDING_CANCEL"
        return result

    if not execution_safety.get(
        "order_send_allowed",
        False,
    ):
        result[
            "decision"
        ] = "EXECUTION_SAFETY_BLOCKED"
        result[
            "errors"
        ].extend(
            execution_safety.get(
                "errors",
                [],
            )
        )
        return result

    request = cancel_check.get(
        "request"
    )

    if not request:
        result[
            "decision"
        ] = "PENDING_CANCEL_REQUEST_MISSING"
        result[
            "errors"
        ].append(
            "Cancel request отсутствует после order_check()."
        )
        return result

    emit("mt5", "order_send.request", data={"request": request})
    raw_result = mt5.order_send(request)
    emit("mt5", "order_send.response", data={"result": raw_result, "last_error": mt5.last_error() if raw_result is None else None})
    result[
        "order_send_called"
    ] = True
    send_result = build_order_send_result(
        raw_result
    )
    result[
        "order_send_result"
    ] = send_result

    # После REMOVE не считаем исчезновение order доказательством
    # само по себе. Обязательно подтверждаем history/position/deal.
    for attempt in range(
        1,
        RECONCILIATION_POLL_ATTEMPTS
        + 1,
    ):
        if attempt > 1:
            time.sleep(
                RECONCILIATION_POLL_DELAY_SECONDS
            )

        refreshed = get_active_plan()
        if refreshed is None:
            result[
                "decision"
            ] = "PENDING_CANCELLED"
            return result

        reconciliation = reconcile_pending_plan(
            refreshed
        )
        result[
            "reconciliation"
        ] = {
            **reconciliation,
            "attempts": attempt,
        }

        if reconciliation.get(
            "resolved",
            False,
        ) and reconciliation.get(
            "decision"
        ) != "PENDING_ACTIVE":
            result[
                "decision"
            ] = reconciliation.get(
                "decision"
            )
            result[
                "position_ticket"
            ] = reconciliation.get(
                "position_ticket"
            )
            result[
                "state_attached"
            ] = bool(
                reconciliation.get(
                    "position_ticket"
                )
            )
            return result

    result[
        "decision"
    ] = "PENDING_CANCEL_STATE_UNKNOWN"
    result[
        "errors"
    ].append(
        "После TRADE_ACTION_REMOVE MT5 не подтвердил "
        "однозначный финальный статус pending order. "
        "Новые сделки запрещены до reconciliation."
    )

    return result


# ============================================================
# EXECUTE / MONITOR PENDING PLAN
# ============================================================

@observe("execution")
def execute_pending_plan(
    snapshot: dict,
    symbol: str = SYMBOL,
) -> dict:
    plan = get_active_plan()

    if plan is None:
        validation = build_pending_monitor_report(
            plan=None,
            decision="NO_ACTIVE_PLAN",
            errors=[
                "Активного Trade Plan нет."
            ],
        )
        return {
            "mode": inspect_execution_safety_gate().get(
                "mode"
            ),
            "decision": "NO_ACTIVE_PLAN",
            "validation_report": validation,
            "execution_safety": inspect_execution_safety_gate(),
            "order_send_called": False,
            "order_send_result": None,
            "position_ticket": None,
            "pending_ticket": None,
            "state_attached": False,
            "send_intent": None,
            "reconciliation": None,
            "errors": [
                "Активного Trade Plan нет."
            ],
            "warnings": [],
        }

    if str(
        plan.get(
            "order_type",
            "",
        )
    ) not in PENDING_ORDER_TYPES:
        raise RuntimeError(
            "execute_pending_plan вызван для не-pending плана."
        )

    execution_safety = inspect_execution_safety_gate()
    mode = str(
        execution_safety.get(
            "mode",
            EXECUTION_MODE_DRY_RUN,
        )
    ).upper()

    execution_status = plan.get(
        "execution_status"
    )

    # --------------------------------------------------------
    # Уже отправленный / SEND_INTENT / cancel_requested.
    # Сначала только reconciliation.
    # --------------------------------------------------------
    if execution_status in (
        EXECUTION_SEND_INTENT,
        EXECUTION_PENDING_SENT,
        EXECUTION_CANCEL_REQUESTED,
    ):
        reconciliation = reconcile_pending_plan(
            plan
        )

        if reconciliation.get(
            "decision"
        ) in (
            "PENDING_FILLED_POSITION_RECOVERED",
            "PENDING_FILLED_HISTORY_RECOVERED",
        ):
            validation = build_pending_monitor_report(
                plan=plan,
                decision="PENDING_FILLED",
                warnings=[
                    "Pending order исполнился в позицию."
                ],
            )
            return {
                "mode": mode,
                "decision": "PENDING_FILLED",
                "validation_report": validation,
                "execution_safety": execution_safety,
                "order_send_called": False,
                "order_send_result": None,
                "position_ticket": reconciliation.get(
                    "position_ticket"
                ),
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "state_attached": True,
                "send_intent": None,
                "reconciliation": reconciliation,
                "errors": [],
                "warnings": [],
            }

        if reconciliation.get(
            "decision"
        ) in (
            "PENDING_CANCELLED",
            "PENDING_EXPIRED",
            "PENDING_REJECTED",
        ):
            validation = build_pending_monitor_report(
                plan=None,
                decision=reconciliation.get(
                    "decision"
                ),
            )
            return {
                "mode": mode,
                "decision": reconciliation.get(
                    "decision"
                ),
                "validation_report": validation,
                "execution_safety": execution_safety,
                "order_send_called": False,
                "order_send_result": None,
                "position_ticket": None,
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "state_attached": False,
                "send_intent": None,
                "reconciliation": reconciliation,
                "errors": [],
                "warnings": [],
            }

        if reconciliation.get(
            "blocked",
            False,
        ):
            validation = build_pending_monitor_report(
                plan=plan,
                decision="PENDING_RECONCILIATION_BLOCKED",
                errors=reconciliation.get(
                    "errors",
                    [],
                ),
            )
            return {
                "mode": mode,
                "decision": "PENDING_RECONCILIATION_BLOCKED",
                "validation_report": validation,
                "execution_safety": execution_safety,
                "order_send_called": False,
                "order_send_result": None,
                "position_ticket": None,
                "pending_ticket": plan.get(
                    "pending_ticket"
                ),
                "state_attached": False,
                "send_intent": None,
                "reconciliation": reconciliation,
                "errors": reconciliation.get(
                    "errors",
                    [],
                ),
                "warnings": [],
            }

        # После reconciliation active order точно существует.
        refreshed = get_active_plan()
        if refreshed is None:
            validation = build_pending_monitor_report(
                plan=None,
                decision="PENDING_ALREADY_RESOLVED",
            )
            return {
                "mode": mode,
                "decision": "PENDING_ALREADY_RESOLVED",
                "validation_report": validation,
                "execution_safety": execution_safety,
                "order_send_called": False,
                "order_send_result": None,
                "position_ticket": None,
                "pending_ticket": None,
                "state_attached": False,
                "send_intent": None,
                "reconciliation": reconciliation,
                "errors": [],
                "warnings": [],
            }

        h1 = pending_plan_belongs_to_current_h1(
            refreshed,
            snapshot,
        )

        if (
            refreshed.get(
                "execution_status"
            )
            == EXECUTION_CANCEL_REQUESTED
            or not h1["valid"]
        ):
            reason = (
                refreshed.get(
                    "cancel_reason"
                )
                or (
                    "Истёк срок pending-plan: "
                    f"source H1={h1['source_h1']}, current H1={h1['current_h1']}, "
                    f"age={h1['age_h1_bars']} bars, max={h1['max_h1_bars']}."
                )
            )
            return execute_pending_cancel(
                plan=refreshed,
                reason=reason,
            )

        validation = build_pending_monitor_report(
            plan=refreshed,
            decision="PENDING_ACTIVE",
            warnings=[
                "Pending order активен и относится "
                "к текущей последней закрытой H1."
            ],
        )

        return {
            "mode": mode,
            "decision": "PENDING_ACTIVE",
            "validation_report": validation,
            "execution_safety": execution_safety,
            "order_send_called": False,
            "order_send_result": None,
            "position_ticket": None,
            "pending_ticket": refreshed.get(
                "pending_ticket"
            ),
            "state_attached": True,
            "send_intent": None,
            "reconciliation": reconciliation,
            "errors": [],
            "warnings": [],
        }

    # --------------------------------------------------------
    # Неотправленная pending-идея.
    # --------------------------------------------------------
    if execution_status != EXECUTION_NOT_SENT:
        validation = build_pending_monitor_report(
            plan=plan,
            decision="UNSUPPORTED_PENDING_EXECUTION_STATE",
            errors=[
                "Неожиданный execution_status: "
                f"{execution_status}."
            ],
        )
        return {
            "mode": mode,
            "decision": "UNSUPPORTED_PENDING_EXECUTION_STATE",
            "validation_report": validation,
            "execution_safety": execution_safety,
            "order_send_called": False,
            "order_send_result": None,
            "position_ticket": None,
            "pending_ticket": plan.get(
                "pending_ticket"
            ),
            "state_attached": False,
            "send_intent": None,
            "reconciliation": None,
            "errors": [
                "Неожиданный execution_status: "
                f"{execution_status}."
            ],
            "warnings": [],
        }

    h1 = pending_plan_belongs_to_current_h1(
        plan,
        snapshot,
    )

    if not h1["valid"]:
        reason = (
            "Неотправленный pending-plan превысил срок жизни: "
            f"source H1={h1['source_h1']}, "
            f"current H1={h1['current_h1']}, "
            f"age={h1['age_h1_bars']} bars, max={h1['max_h1_bars']}."
        )

        finalize_active_pending_plan(
            final_status=PLAN_STATUS_EXPIRED,
            reason=reason,
        )

        validation = build_pending_monitor_report(
            plan=plan,
            decision="PENDING_PLAN_EXPIRED_BEFORE_SEND",
            warnings=[
                reason
            ],
        )

        return {
            "mode": mode,
            "decision": "PENDING_PLAN_EXPIRED_BEFORE_SEND",
            "validation_report": validation,
            "execution_safety": execution_safety,
            "order_send_called": False,
            "order_send_result": None,
            "position_ticket": None,
            "pending_ticket": None,
            "state_attached": False,
            "send_intent": None,
            "reconciliation": None,
            "errors": [],
            "warnings": [
                reason
            ],
        }

    validation_report = validate_active_plan(
        snapshot=snapshot,
        symbol=symbol,
        # inspect_pending_h1_validity() above already proved the explicit
        # three-H1 TTL. The shared market-order freshness rule must not undo
        # that pending policy after a terminal restart or transient outage.
        allow_prior_h1_for_pending=True,
    )

    result = {
        "mode": mode,
        "decision": "VALIDATION_ONLY",
        "validation_report": validation_report,
        "execution_safety": execution_safety,
        "order_send_called": False,
        "order_send_result": None,
        "position_ticket": None,
        "pending_ticket": None,
        "state_attached": False,
        "send_intent": None,
        "reconciliation": None,
        "errors": [],
        "warnings": [],
    }

    if not validation_report.get(
        "ready_for_send",
        False,
    ):
        result[
            "decision"
        ] = "VALIDATION_BLOCKED"
        return result

    if mode != EXECUTION_MODE_LIVE:
        result[
            "decision"
        ] = "DRY_RUN_ONLY"
        return result

    if not execution_safety.get(
        "order_send_allowed",
        False,
    ):
        result[
            "decision"
        ] = "EXECUTION_SAFETY_BLOCKED"
        result[
            "errors"
        ].extend(
            execution_safety.get(
                "errors",
                [],
            )
        )
        return result

    request = validation_report.get(
        "request"
    )

    if not request:
        result[
            "decision"
        ] = "PENDING_REQUEST_MISSING"
        result[
            "errors"
        ].append(
            "Validation прошла, но MT5 request отсутствует."
        )
        return result

    plan = get_active_plan()
    if plan is None:
        result[
            "decision"
        ] = "NO_ACTIVE_PLAN"
        result[
            "errors"
        ].append(
            "active_plan исчез перед SEND_INTENT."
        )
        return result

    plan_id = str(
        plan[
            "plan_id"
        ]
    )

    # Последняя reconciliation непосредственно перед SEND_INTENT.
    active_orders = find_active_pending_orders(
        plan
    )
    positions = find_owned_positions(
        plan
    )

    if (
        not active_orders[
            "ok"
        ]
        or not positions[
            "ok"
        ]
    ):
        result[
            "decision"
        ] = "PRE_SEND_RECONCILIATION_ERROR"
        result[
            "errors"
        ].append(
            "Не удалось безопасно проверить MT5 перед pending send."
        )
        return result

    if (
        active_orders[
            "valid"
        ]
        or active_orders[
            "invalid"
        ]
        or positions[
            "valid"
        ]
        or positions[
            "invalid"
        ]
    ):
        result[
            "decision"
        ] = "PRE_SEND_EXPOSURE_FOUND"
        result[
            "errors"
        ].append(
            "Перед order_send обнаружено уже существующее "
            "исполнение/экспозиция этого plan_id."
        )
        return result

    send_intent = mark_active_plan_send_intent(
        plan_id=plan_id,
        request=request,
    )
    result[
        "send_intent"
    ] = send_intent

    emit("mt5", "order_send.request", data={"request": request})
    raw_result = mt5.order_send(request)
    emit("mt5", "order_send.response", data={"result": raw_result, "last_error": mt5.last_error() if raw_result is None else None})
    result[
        "order_send_called"
    ] = True

    order_send_result = build_order_send_result(
        raw_result
    )
    result[
        "order_send_result"
    ] = order_send_result

    try:
        record_active_plan_order_send_result(
            plan_id=plan_id,
            order_send_result=order_send_result,
        )
    except RuntimeError as error:
        result[
            "warnings"
        ].append(
            "Не удалось сохранить order_send result: "
            f"{error}"
        )

    for attempt in range(
        1,
        RECONCILIATION_POLL_ATTEMPTS
        + 1,
    ):
        if attempt > 1:
            time.sleep(
                RECONCILIATION_POLL_DELAY_SECONDS
            )

        current_plan = get_active_plan()
        if current_plan is None:
            result[
                "decision"
            ] = "PENDING_EXECUTION_RESOLVED"
            return result

        reconciliation = reconcile_pending_plan(
            current_plan
        )
        result[
            "reconciliation"
        ] = {
            **reconciliation,
            "attempts": attempt,
        }

        if reconciliation.get(
            "decision"
        ) == "PENDING_ACTIVE":
            result[
                "decision"
            ] = "PENDING_ORDER_PLACED"
            result[
                "pending_ticket"
            ] = reconciliation.get(
                "pending_ticket"
            )
            result[
                "state_attached"
            ] = True
            return result

        if reconciliation.get(
            "decision"
        ) in (
            "PENDING_FILLED_POSITION_RECOVERED",
            "PENDING_FILLED_HISTORY_RECOVERED",
        ):
            result[
                "decision"
            ] = "PENDING_FILLED_IMMEDIATELY"
            result[
                "position_ticket"
            ] = reconciliation.get(
                "position_ticket"
            )
            result[
                "state_attached"
            ] = True
            return result

        if reconciliation.get(
            "decision"
        ) in (
            "PENDING_REJECTED",
            "PENDING_CANCELLED",
            "PENDING_EXPIRED",
        ):
            result[
                "decision"
            ] = reconciliation.get(
                "decision"
            )
            return result

        if reconciliation.get(
            "blocked",
            False,
        ) and reconciliation.get(
            "decision"
        ) != "NO_PENDING_EXECUTION_EVIDENCE":
            break

    retcode = order_send_result.get(
        "retcode"
    )

    if retcode == PARTIAL_RETCODE:
        result[
            "decision"
        ] = "PENDING_PARTIAL_FILL_UNRESOLVED"
        result[
            "errors"
        ].append(
            "MT5 сообщил partial fill. "
            "Повторная отправка запрещена."
        )
        return result

    if (
        order_send_result.get(
            "received",
            False,
        )
        and retcode not in SUCCESS_RETCODES
    ):
        current_plan = get_active_plan()
        if current_plan is not None:
            final_reconciliation = reconcile_pending_plan(
                current_plan
            )
            result[
                "reconciliation"
            ] = final_reconciliation

            if not final_reconciliation.get(
                "resolved",
                False,
            ) and final_reconciliation.get(
                "decision"
            ) == "NO_PENDING_EXECUTION_EVIDENCE":
                mark_active_plan_execution_failed(
                    plan_id=plan_id,
                    reason=(
                        "MT5 отклонил pending order_send: "
                        f"retcode={retcode}, "
                        f"comment={order_send_result.get('comment')}."
                    ),
                    order_send_result=order_send_result,
                )
                result[
                    "decision"
                ] = "PENDING_ORDER_SEND_REJECTED"
                return result

    result[
        "decision"
    ] = "PENDING_ORDER_SEND_STATE_UNKNOWN"
    result[
        "errors"
    ].append(
        "После pending order_send() результат не удалось "
        "однозначно подтвердить. SEND_INTENT сохранён; "
        "повторный entry запрещён до reconciliation."
    )

    return result
