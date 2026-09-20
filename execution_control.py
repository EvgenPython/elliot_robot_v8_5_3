from observability import observe, emit
import json
import os
from pathlib import Path

import MetaTrader5 as mt5

from trade_state import (
    get_active_plan,
    get_pending_actions,
    get_managed_positions,
)


# ============================================================
# EXECUTION MODES
# ============================================================

EXECUTION_MODE_DISABLED = "TRADING_DISABLED"
EXECUTION_MODE_LIVE = "LIVE"
EXECUTION_MODE_DEMO_LIVE = EXECUTION_MODE_LIVE

# Backward-compatible import name for old diagnostic scripts only.  The
# production configuration contains no DRY_RUN flag: trading_enabled=true
# means the executor may call mt5.order_send() after every other gate passes.
EXECUTION_MODE_DRY_RUN = EXECUTION_MODE_DISABLED

ALLOWED_EXECUTION_MODES = {
    EXECUTION_MODE_DISABLED,
    EXECUTION_MODE_LIVE,
}


# ============================================================
# ГЛАВНЫЙ ПЕРЕКЛЮЧАТЕЛЬ ИСПОЛНЕНИЯ
# ============================================================

# ВАЖНО:
#
# Настройка универсальна для любого MT5-брокера. Если файл отсутствует
# или повреждён, робот безопасно остаётся с отключённой торговлей.
BASE_DIR = Path(__file__).resolve().parent
EXECUTION_CONFIG_PATH = BASE_DIR / "config" / "execution.json"

DEFAULT_EXECUTION_CONFIG = {
    "trading_enabled": False,
    "allowed_account_modes": ["DEMO"],
}


def _env_bool(name: str) -> bool | None:
    value = os.environ.get(name)
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(
        f"{name} должен быть true/false, 1/0, yes/no или on/off."
    )


def load_execution_config() -> dict:
    config = dict(DEFAULT_EXECUTION_CONFIG)
    if EXECUTION_CONFIG_PATH.exists():
        raw = json.loads(EXECUTION_CONFIG_PATH.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise RuntimeError("config/execution.json должен содержать JSON object.")
        config.update(raw)

    env_enabled = _env_bool("ROBOT_TRADING_ENABLED")
    if env_enabled is not None:
        config["trading_enabled"] = env_enabled

    modes = config.get("allowed_account_modes")
    if not isinstance(modes, list) or not modes:
        raise RuntimeError("allowed_account_modes должен быть непустым массивом.")
    config["allowed_account_modes"] = sorted(
        {str(item).strip().upper() for item in modes}
    )
    return config


# ============================================================
# MT5 ACCOUNT TRADE MODE
# ============================================================

def get_account_trade_mode_name(
    trade_mode: int | None,
) -> str:
    """
    Возвращает читаемое имя режима торгового счёта MT5.
    """

    if trade_mode is None:
        return "UNKNOWN"

    mapping = {
        int(
            getattr(
                mt5,
                "ACCOUNT_TRADE_MODE_DEMO",
                0,
            )
        ): "DEMO",
        int(
            getattr(
                mt5,
                "ACCOUNT_TRADE_MODE_CONTEST",
                1,
            )
        ): "CONTEST",
        int(
            getattr(
                mt5,
                "ACCOUNT_TRADE_MODE_REAL",
                2,
            )
        ): "REAL",
    }

    return mapping.get(
        int(trade_mode),
        f"UNKNOWN({trade_mode})",
    )


# ============================================================
# EXECUTION SAFETY GATE
# ============================================================

@observe("execution")
def inspect_execution_safety_gate() -> dict:
    """
    Проверяет глобальное разрешение на реальный order_send().

    Эта функция НЕ отправляет ордера.

    В режиме TRADING_DISABLED:
        конфигурация считается корректной,
        но order_send запрещён.

    В режиме LIVE реальный order_send разрешён только если:

        1. trading_enabled == true;
        2. режим счёта разрешён в allowed_account_modes;
        3. MT5 account_info доступен;
        4. terminal_info доступен;
        5. терминал подключён;
        6. terminal.trade_allowed == True;
        7. account.trade_allowed == True, если поле доступно.

    Проверка login/server выполняется отдельно в connect_mt5().
    """

    errors = []
    warnings = []

    try:
        execution_config = load_execution_config()
    except Exception as error:
        execution_config = dict(DEFAULT_EXECUTION_CONFIG)
        errors.append(
            "Ошибка config/execution.json: "
            f"{type(error).__name__}: {error}"
        )

    trading_enabled = bool(execution_config.get("trading_enabled", False))
    allowed_account_modes = execution_config.get("allowed_account_modes", [])
    mode = EXECUTION_MODE_LIVE if trading_enabled else EXECUTION_MODE_DISABLED

    if mode not in ALLOWED_EXECUTION_MODES:
        errors.append(
            "Неизвестный EXECUTION_MODE: "
            f"{mode}."
        )

    account = mt5.account_info()
    terminal = mt5.terminal_info()

    account_trade_mode = None
    account_trade_mode_name = "UNKNOWN"

    account_login = None
    account_server = None
    account_trade_allowed = None

    terminal_connected = None
    terminal_trade_allowed = None

    if account is None:
        errors.append(
            "MT5 account_info() вернул None."
        )
    else:
        account_trade_mode = getattr(
            account,
            "trade_mode",
            None,
        )

        account_trade_mode_name = (
            get_account_trade_mode_name(
                account_trade_mode
            )
        )

        account_login = getattr(
            account,
            "login",
            None,
        )

        account_server = getattr(
            account,
            "server",
            None,
        )

        account_trade_allowed = getattr(
            account,
            "trade_allowed",
            None,
        )

    if terminal is None:
        errors.append(
            "MT5 terminal_info() вернул None."
        )
    else:
        terminal_connected = bool(
            getattr(
                terminal,
                "connected",
                False,
            )
        )

        terminal_trade_allowed = bool(
            getattr(
                terminal,
                "trade_allowed",
                False,
            )
        )

    configuration_valid = (
        len(errors) == 0
        and
        mode in ALLOWED_EXECUTION_MODES
    )

    order_send_allowed = False

    if configuration_valid:

        if mode == EXECUTION_MODE_DISABLED:
            warnings.append(
                "Торговля выключена: mt5.order_send() запрещён."
            )

        elif mode == EXECUTION_MODE_LIVE:

            if account_trade_mode_name not in allowed_account_modes:
                errors.append(
                    "Режим текущего MT5-счёта не разрешён: "
                    f"{account_trade_mode_name}; разрешены "
                    f"{', '.join(allowed_account_modes)}."
                )

            if terminal_connected is not True:
                errors.append(
                    "MT5 terminal.connected=False."
                )

            # terminal.trade_allowed НЕ является единственной защитой.
            if terminal_trade_allowed is not True:
                errors.append(
                    "MT5 terminal.trade_allowed=False."
                )

            if (
                account_trade_allowed is not None
                and
                bool(account_trade_allowed) is not True
            ):
                errors.append(
                    "MT5 account.trade_allowed=False."
                )

            if len(errors) == 0:
                order_send_allowed = True

    return {
        "mode": mode,
        "demo_live_armed": trading_enabled,
        "trading_enabled": trading_enabled,
        "allowed_account_modes": allowed_account_modes,
        "config_path": str(EXECUTION_CONFIG_PATH),
        "configuration_valid": (
            len(errors) == 0
            if mode == EXECUTION_MODE_LIVE
            else configuration_valid
        ),
        "order_send_allowed": (
            order_send_allowed
        ),
        "account": {
            "login": account_login,
            "server": account_server,
            "trade_mode": account_trade_mode,
            "trade_mode_name": account_trade_mode_name,
            "trade_allowed": account_trade_allowed,
        },
        "terminal": {
            "connected": terminal_connected,
            "trade_allowed": terminal_trade_allowed,
        },
        "errors": errors,
        "warnings": warnings,
    }


# ============================================================
# PRE-CLAUDE GATE
# ============================================================

@observe("execution")
def inspect_pre_claude_gate(
    symbol: str,
) -> dict:
    """
    Fail-closed gate перед НОВЫМ Claude-анализом.

    Claude нельзя вызывать для новой H1, если существует
    незавершённое торговое состояние, которое сначала нужно
    обработать/сверить.

    Блокирующие состояния:

        - pending_actions в Trade State;
        - managed_positions в Trade State;
        - active_plan в Trade State;
        - реальные MT5 positions по symbol;
        - реальные MT5 orders по symbol;
        - ошибка чтения MT5 positions/orders.

    ВАЖНО:
    gate вызывается только после того, как H1 Analysis Gate
    уже подтвердил NEW_H1. Поэтому active_plan здесь означает
    старое незавершённое состояние, а не план текущего анализа.
    """

    blockers = []
    warnings = []

    pending_actions = (
        get_pending_actions()
    )

    managed_positions = (
        get_managed_positions()
    )

    active_plan = (
        get_active_plan()
    )

    if pending_actions:
        blockers.append(
            "В Trade State есть незавершённые pending_actions."
        )

    if managed_positions:
        blockers.append(
            "В Trade State есть managed_positions. "
            "Перед новым Claude-анализом нужна reconciliation."
        )

    if active_plan is not None:
        blockers.append(
            "В Trade State существует active_plan от предыдущего цикла. "
            "Сначала должен отработать Executor/reconciliation."
        )

    positions = mt5.positions_get(
        symbol=str(symbol)
    )

    if positions is None:
        blockers.append(
            "Не удалось прочитать MT5 positions_get(). "
            f"MT5 error: {mt5.last_error()}"
        )
        positions_count = None
    else:
        positions_count = len(
            positions
        )

        if positions_count > 0:
            blockers.append(
                f"В MT5 уже существует {positions_count} "
                f"позиция(и) по {symbol}."
            )

    orders = mt5.orders_get(
        symbol=str(symbol)
    )

    if orders is None:
        blockers.append(
            "Не удалось прочитать MT5 orders_get(). "
            f"MT5 error: {mt5.last_error()}"
        )
        orders_count = None
    else:
        orders_count = len(
            orders
        )

        if orders_count > 0:
            blockers.append(
                f"В MT5 уже существует {orders_count} "
                f"активный ордер(а) по {symbol}."
            )

    return {
        "allowed": (
            len(blockers) == 0
        ),
        "symbol": str(symbol),
        "blockers": blockers,
        "warnings": warnings,
        "trade_state": {
            "pending_actions_count": len(
                pending_actions
            ),
            "managed_positions_count": len(
                managed_positions
            ),
            "active_plan_present": (
                active_plan is not None
            ),
            "active_plan_id": (
                active_plan.get(
                    "plan_id"
                )
                if active_plan is not None
                else None
            ),
        },
        "mt5": {
            "positions_count": positions_count,
            "orders_count": orders_count,
        },
    }


# ============================================================
# PRINT EXECUTION SAFETY GATE
# ============================================================

def print_execution_safety_gate(
    report: dict,
):
    """
    Печатает глобальный execution gate.
    """

    print()
    print("=" * 80)
    print("EXECUTION SAFETY GATE")
    print("=" * 80)

    print(
        f"Mode:                 "
        f"{report['mode']}"
    )

    print(
        f"Trading enabled:      "
        f"{report['trading_enabled']}"
    )

    print(
        f"Allowed account modes:"
        f" {', '.join(report['allowed_account_modes'])}"
    )

    print(
        f"Configuration valid:  "
        f"{report['configuration_valid']}"
    )

    print(
        f"order_send allowed:   "
        f"{report['order_send_allowed']}"
    )

    account = report[
        "account"
    ]

    print()
    print("ACCOUNT")
    print("-" * 80)

    print(
        f"Login:                "
        f"{account['login']}"
    )

    print(
        f"Server:               "
        f"{account['server']}"
    )

    print(
        f"Trade mode:           "
        f"{account['trade_mode_name']}"
    )

    print(
        f"Account trade allowed:"
        f" {account['trade_allowed']}"
    )

    terminal = report[
        "terminal"
    ]

    print()
    print("TERMINAL")
    print("-" * 80)

    print(
        f"Connected:            "
        f"{terminal['connected']}"
    )

    print(
        f"Trade allowed:        "
        f"{terminal['trade_allowed']}"
    )

    if report[
        "warnings"
    ]:
        print()
        print("WARNINGS")
        print("-" * 80)

        for warning in report[
            "warnings"
        ]:
            print(
                f"- {warning}"
            )

    if report[
        "errors"
    ]:
        print()
        print("ERRORS")
        print("-" * 80)

        for error in report[
            "errors"
        ]:
            print(
                f"- {error}"
            )

    print("=" * 80)


# ============================================================
# PRINT PRE-CLAUDE GATE
# ============================================================

def print_pre_claude_gate(
    report: dict,
):
    """
    Печатает gate перед новым Claude-анализом.
    """

    print()
    print("=" * 80)
    print("PRE-CLAUDE EXECUTION GATE")
    print("=" * 80)

    print(
        f"Symbol:               "
        f"{report['symbol']}"
    )

    print(
        f"Claude allowed:       "
        f"{report['allowed']}"
    )

    state = report[
        "trade_state"
    ]

    print()
    print("TRADE STATE")
    print("-" * 80)

    print(
        f"Pending actions:      "
        f"{state['pending_actions_count']}"
    )

    print(
        f"Managed positions:    "
        f"{state['managed_positions_count']}"
    )

    print(
        f"Active plan:          "
        f"{state['active_plan_present']}"
    )

    if state[
        "active_plan_id"
    ]:
        print(
            f"Active plan ID:       "
            f"{state['active_plan_id']}"
        )

    mt5_state = report[
        "mt5"
    ]

    print()
    print("MT5")
    print("-" * 80)

    print(
        f"Positions:            "
        f"{mt5_state['positions_count']}"
    )

    print(
        f"Orders:               "
        f"{mt5_state['orders_count']}"
    )

    if report[
        "blockers"
    ]:
        print()
        print("BLOCKERS")
        print("-" * 80)

        for blocker in report[
            "blockers"
        ]:
            print(
                f"- {blocker}"
            )

    if report[
        "allowed"
    ]:
        print()
        print(
            "[OK] Незавершённых торговых состояний нет. "
            "Новый Claude-анализ может быть выполнен."
        )
    else:
        print()
        print(
            "[BLOCKED] Claude НЕ должен вызываться, "
            "пока блокирующее состояние не устранено."
        )

    print("=" * 80)
