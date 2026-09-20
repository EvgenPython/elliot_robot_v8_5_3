from observability import observe, emit
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path

import MetaTrader5 as mt5


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument, symbol_state_path

SYMBOL = active_instrument()


# ============================================================
# FUNDINGPIPS TIME
# ============================================================

FUNDINGPIPS_TZ = timezone(
    timedelta(hours=3),
    name="FundingPips UTC+3",
)


# ============================================================
# FUNDINGPIPS 2 STEP STANDARD
# ============================================================

STARTING_ACCOUNT_SIZE = 10_000.0

DAILY_LOSS_LIMIT_PERCENT = 5.0

MAX_LOSS_LIMIT_PERCENT = 10.0


# ============================================================
# НАШ ВНУТРЕННИЙ РИСК-МЕНЕДЖМЕНТ
# ============================================================

# Максимальный риск на одну новую торговую идею.
TARGET_RISK_PER_TRADE_PERCENT = 1.0

# Минимальный confidence Claude.
MIN_CONFIDENCE = "medium"

# Дополнительный буфер до жёсткого лимита.
BREACH_SAFETY_BUFFER_PERCENT = 0.25

# Максимально допустимое отклонение market-entry
# от цены, которую анализировал Claude.
MAX_MARKET_ENTRY_DEVIATION_PERCENT = 0.10

# Если baseline нового дня зафиксирован в первые N минут
# после 00:00 FundingPips Time, считаем его надёжным.
DAILY_BASELINE_CAPTURE_WINDOW_MINUTES = 5


# ============================================================
# STATE
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

STATE_DIR = (
    BASE_DIR
    / "state"
)

RISK_STATE_PATH = symbol_state_path("fundingpips_risk_state.json")


# ============================================================
# ВРЕМЯ
# ============================================================

def now_fp() -> datetime:
    """
    Текущее FundingPips Platform Time.
    """

    return datetime.now(
        timezone.utc
    ).astimezone(
        FUNDINGPIPS_TZ
    )


def fp_day_start(
    dt: datetime | None = None,
) -> datetime:
    """
    Начало текущего FundingPips trading day.
    """

    if dt is None:
        dt = now_fp()

    return dt.astimezone(
        FUNDINGPIPS_TZ
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


# ============================================================
# STATE FILE
# ============================================================

def load_risk_state() -> dict | None:
    """
    Загружает сохранённое состояние FundingPips day.
    """

    if not RISK_STATE_PATH.exists():
        return None

    try:

        with open(
            RISK_STATE_PATH,
            "r",
            encoding="utf-8",
        ) as file:

            return json.load(
                file
            )

    except (
        json.JSONDecodeError,
        OSError,
    ):

        return None


def save_risk_state(
    state: dict,
):
    """
    Сохраняет состояние FundingPips day.
    """

    STATE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        RISK_STATE_PATH,
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
# MT5 ACCOUNT
# ============================================================

def get_account_info():
    """
    Получает состояние торгового счёта.
    """

    account = mt5.account_info()

    if account is None:

        raise RuntimeError(
            "Не удалось получить account_info(). "
            f"MT5 error: {mt5.last_error()}"
        )

    return account


def get_positions() -> tuple:
    """
    Получает все текущие позиции.
    """

    positions = mt5.positions_get()

    if positions is None:

        raise RuntimeError(
            "Не удалось получить positions_get(). "
            f"MT5 error: {mt5.last_error()}"
        )

    return positions


# ============================================================
# DAILY BASELINE
# ============================================================

def account_looks_pristine(
    account,
    positions: tuple,
) -> bool:
    """
    Проверяет, выглядит ли счёт как новый и нетронутый.
    """

    tolerance = 0.01

    balance_ok = (
        abs(
            float(account.balance)
            - STARTING_ACCOUNT_SIZE
        )
        <= tolerance
    )

    equity_ok = (
        abs(
            float(account.equity)
            - STARTING_ACCOUNT_SIZE
        )
        <= tolerance
    )

    positions_ok = (
        len(positions) == 0
    )

    return (
        balance_ok
        and equity_ok
        and positions_ok
    )


def _deal_net_balance_change(deal) -> float:
    """Return the booked balance change represented by one MT5 deal."""

    return sum(
        float(getattr(deal, name, 0.0) or 0.0)
        for name in ("profit", "commission", "swap", "fee")
    )


def reconstruct_daily_state_from_history(
    account,
    positions: tuple,
    current: datetime,
    start: datetime,
) -> dict:
    """Safely reconstruct a missed midnight baseline from MT5 history.

    Reconstruction is exact only when no position crossed the FP-day reset:
    in that case opening equity equals opening balance, and opening balance is
    current balance minus every booked deal change since midnight.  If a
    position crossed midnight, the historical midnight quote would be needed;
    MT5 account/deal history alone cannot prove it, so the result stays
    untrusted instead of inventing a number.
    """

    start_utc = start.astimezone(timezone.utc)
    end_utc = current.astimezone(timezone.utc) + timedelta(minutes=5)
    start_epoch = start_utc.timestamp()
    reasons = []

    for position in positions:
        opened_at = getattr(position, "time", None)
        if opened_at is None:
            reasons.append(
                f"Позиция #{getattr(position, 'ticket', '?')} не содержит time."
            )
            continue
        if float(opened_at) < start_epoch:
            reasons.append(
                f"Позиция #{getattr(position, 'ticket', '?')} перенесена через 00:00 FP."
            )

    if reasons:
        return {"trusted": False, "reasons": reasons, "deals_count": None}

    deals = mt5.history_deals_get(start_utc, end_utc)
    if deals is None:
        return {
            "trusted": False,
            "reasons": [
                "history_deals_get() не позволил восстановить начало дня: "
                f"{mt5.last_error()}"
            ],
            "deals_count": None,
        }

    deals = sorted(
        tuple(deals),
        key=lambda item: (
            int(getattr(item, "time_msc", 0) or 0),
            int(getattr(item, "time", 0) or 0),
            int(getattr(item, "ticket", 0) or 0),
        ),
    )
    entry_in = int(getattr(mt5, "DEAL_ENTRY_IN", 0))
    entry_out = int(getattr(mt5, "DEAL_ENTRY_OUT", 1))
    entry_inout = int(getattr(mt5, "DEAL_ENTRY_INOUT", 2))
    entry_out_by = int(getattr(mt5, "DEAL_ENTRY_OUT_BY", 3))
    buy_type = int(getattr(mt5, "DEAL_TYPE_BUY", 0))
    sell_type = int(getattr(mt5, "DEAL_TYPE_SELL", 1))
    seen_entries = set()

    for deal in deals:
        deal_type = int(getattr(deal, "type", -1))
        if deal_type not in {buy_type, sell_type}:
            continue
        position_id = int(getattr(deal, "position_id", 0) or 0)
        entry_kind = int(getattr(deal, "entry", -1))
        if entry_kind == entry_in:
            if position_id:
                seen_entries.add(position_id)
            continue
        if entry_kind in {entry_out, entry_out_by, entry_inout}:
            if not position_id or position_id not in seen_entries:
                reasons.append(
                    "Найдена закрывающая сделка по позиции, открытой до "
                    f"00:00 FP (position_id={position_id or '?'})."
                )

    current_position_ids = {
        int(getattr(position, "identifier", 0) or getattr(position, "ticket", 0) or 0)
        for position in positions
    }
    missing_entries = sorted(
        position_id
        for position_id in current_position_ids
        if position_id and position_id not in seen_entries
    )
    if missing_entries:
        reasons.append(
            "Для открытых позиций не найдены сегодняшние entry-deals: "
            + ", ".join(str(item) for item in missing_entries)
            + "."
        )

    if reasons:
        return {
            "trusted": False,
            "reasons": reasons,
            "deals_count": len(deals),
        }

    booked_change = sum(_deal_net_balance_change(deal) for deal in deals)
    opening_balance = float(account.balance) - booked_change
    return {
        "trusted": True,
        "reasons": [],
        "deals_count": len(deals),
        "booked_change_since_reset": booked_change,
        "opening_balance": opening_balance,
        "opening_equity": opening_balance,
        "daily_baseline": opening_balance,
    }


def create_daily_state(
    account,
    positions: tuple,
) -> dict:
    """
    Создаёт состояние нового FundingPips trading day.
    """

    current = now_fp()

    start = fp_day_start(
        current
    )

    minutes_from_reset = (
        current - start
    ).total_seconds() / 60.0

    opening_balance = float(
        account.balance
    )

    opening_equity = float(
        account.equity
    )

    baseline = max(
        opening_balance,
        opening_equity,
    )

    trusted = False

    capture_method = (
        "runtime_snapshot"
    )

    if (
        minutes_from_reset
        <= DAILY_BASELINE_CAPTURE_WINDOW_MINUTES
    ):

        trusted = True

        capture_method = (
            "reset_window"
        )

    elif account_looks_pristine(
        account,
        positions,
    ):

        trusted = True

        capture_method = (
            "pristine_account_bootstrap"
        )

    reconstruction = None

    if not trusted:
        reconstruction = reconstruct_daily_state_from_history(
            account=account,
            positions=positions,
            current=current,
            start=start,
        )
        if reconstruction.get("trusted"):
            trusted = True
            capture_method = "history_reconstruction"
            opening_balance = float(reconstruction["opening_balance"])
            opening_equity = float(reconstruction["opening_equity"])
            baseline = float(reconstruction["daily_baseline"])

    state = {

        "fp_day": str(
            current.date()
        ),

        "captured_at_fp": (
            current.isoformat()
        ),

        "capture_method": (
            capture_method
        ),

        "trusted": (
            trusted
        ),

        "starting_account_size": (
            STARTING_ACCOUNT_SIZE
        ),

        "opening_balance": (
            opening_balance
        ),

        "opening_equity": (
            opening_equity
        ),

        "daily_baseline": (
            baseline
        ),

        "reconstruction": reconstruction,
    }

    save_risk_state(
        state
    )

    return state


def get_daily_state(
    account,
    positions: tuple,
) -> dict:
    """
    Получает состояние текущего FundingPips day.
    """

    current_day = str(
        now_fp().date()
    )

    state = load_risk_state()

    if state is None:

        return create_daily_state(
            account,
            positions,
        )

    saved_day = str(
        state.get(
            "fp_day",
            ""
        )
    )

    if saved_day != current_day:

        return create_daily_state(
            account,
            positions,
        )

    return state


# ============================================================
# FUNDINGPIPS LIMITS
# ============================================================

def calculate_fundingpips_limits(
    daily_state: dict,
) -> dict:
    """
    Рассчитывает базовые лимиты FundingPips.
    """

    daily_baseline = float(
        daily_state[
            "daily_baseline"
        ]
    )

    daily_loss_money = (
        daily_baseline
        * DAILY_LOSS_LIMIT_PERCENT
        / 100.0
    )

    daily_floor = (
        daily_baseline
        - daily_loss_money
    )

    max_loss_money = (
        STARTING_ACCOUNT_SIZE
        * MAX_LOSS_LIMIT_PERCENT
        / 100.0
    )

    max_loss_floor = (
        STARTING_ACCOUNT_SIZE
        - max_loss_money
    )

    safety_buffer_money = (
        STARTING_ACCOUNT_SIZE
        * BREACH_SAFETY_BUFFER_PERCENT
        / 100.0
    )

    return {

        "daily_baseline": (
            daily_baseline
        ),

        "daily_loss_percent": (
            DAILY_LOSS_LIMIT_PERCENT
        ),

        "daily_loss_money": (
            daily_loss_money
        ),

        "daily_floor": (
            daily_floor
        ),

        "max_loss_percent": (
            MAX_LOSS_LIMIT_PERCENT
        ),

        "max_loss_money": (
            max_loss_money
        ),

        "max_loss_floor": (
            max_loss_floor
        ),

        "safety_buffer_money": (
            safety_buffer_money
        ),
    }


def enrich_limits(
    limits: dict,
    balance: float,
    equity: float,
    existing_positions_risk: float,
) -> dict:
    """
    Добавляет к базовым FundingPips limits
    текущее фактическое состояние счёта.

    Эта функция вызывается для ВСЕХ решений:

        APPROVED
        REJECTED
        NO_TRADE

    Благодаря этому структура отчёта всегда одинаковая.
    """

    conservative_account_value = min(
        balance,
        equity,
    )

    daily_room = (
        conservative_account_value
        - limits["daily_floor"]
    )

    max_room = (
        conservative_account_value
        - limits["max_loss_floor"]
    )

    safety_buffer = (
        limits["safety_buffer_money"]
    )

    daily_available_for_new_trade = (
        daily_room
        - safety_buffer
        - existing_positions_risk
    )

    max_available_for_new_trade = (
        max_room
        - safety_buffer
        - existing_positions_risk
    )

    return {

        **limits,

        "current_account_value": (
            conservative_account_value
        ),

        "current_daily_room": (
            daily_room
        ),

        "current_max_room": (
            max_room
        ),

        "daily_available_for_new_trade": (
            max(
                0.0,
                daily_available_for_new_trade,
            )
        ),

        "max_available_for_new_trade": (
            max(
                0.0,
                max_available_for_new_trade,
            )
        ),
    }


# ============================================================
# CONFIDENCE
# ============================================================

def confidence_rank(
    confidence: str,
) -> int:
    """
    Преобразует confidence в числовой уровень.
    """

    mapping = {
        "low": 1,
        "medium": 2,
        "high": 3,
    }

    return mapping.get(
        str(
            confidence
        ).lower(),
        0,
    )


def confidence_is_allowed(
    confidence: str,
) -> bool:
    """
    Проверяет минимальный confidence.
    """

    return (
        confidence_rank(
            confidence
        )
        >=
        confidence_rank(
            MIN_CONFIDENCE
        )
    )


# ============================================================
# ORDER CALCULATION
# ============================================================

def get_mt5_order_type(
    action: str,
):
    """
    Возвращает BUY или SELL для расчётов MT5.
    """

    if action == "enter_long":

        return mt5.ORDER_TYPE_BUY

    if action == "enter_short":

        return mt5.ORDER_TYPE_SELL

    raise ValueError(
        f"Неизвестный action: {action}"
    )


def calculate_trade_loss(
    symbol: str,
    action: str,
    volume: float,
    entry_price: float,
    stop_loss: float,
) -> float:
    """
    Рассчитывает денежный убыток Entry -> SL.
    """

    order_type = get_mt5_order_type(
        action
    )

    pnl = mt5.order_calc_profit(
        order_type,
        symbol,
        float(volume),
        float(entry_price),
        float(stop_loss),
    )

    if pnl is None:

        raise RuntimeError(
            "MT5 order_calc_profit() не смог "
            "рассчитать Stop Loss. "
            f"Ошибка: {mt5.last_error()}"
        )

    pnl = float(
        pnl
    )

    if pnl >= 0:

        raise ValueError(
            "Stop Loss даёт не отрицательный P/L. "
            "Проверь расположение SL."
        )

    return abs(
        pnl
    )


def calculate_trade_profit(
    symbol: str,
    action: str,
    volume: float,
    entry_price: float,
    take_profit: float,
) -> float:
    """
    Рассчитывает потенциальную прибыль Entry -> TP.
    """

    order_type = get_mt5_order_type(
        action
    )

    pnl = mt5.order_calc_profit(
        order_type,
        symbol,
        float(volume),
        float(entry_price),
        float(take_profit),
    )

    if pnl is None:

        raise RuntimeError(
            "MT5 order_calc_profit() не смог "
            "рассчитать Take Profit. "
            f"Ошибка: {mt5.last_error()}"
        )

    return float(
        pnl
    )


# ============================================================
# LOT NORMALIZATION
# ============================================================

def floor_to_volume_step(
    volume: float,
    step: float,
) -> float:
    """
    Округляет lot только вниз.
    """

    volume_decimal = Decimal(
        str(volume)
    )

    step_decimal = Decimal(
        str(step)
    )

    steps = (
        volume_decimal
        / step_decimal
    ).quantize(
        Decimal("1"),
        rounding=ROUND_FLOOR,
    )

    result = (
        steps
        * step_decimal
    )

    return float(
        result
    )


def calculate_position_size(
    symbol: str,
    action: str,
    entry_price: float,
    stop_loss: float,
    risk_money: float,
) -> dict:
    """
    Рассчитывает максимально допустимый объём позиции.
    """

    info = mt5.symbol_info(
        symbol
    )

    if info is None:

        raise RuntimeError(
            f"Не удалось получить symbol_info({symbol})."
        )

    volume_min = float(
        info.volume_min
    )

    volume_max = float(
        info.volume_max
    )

    volume_step = float(
        info.volume_step
    )

    loss_one_lot = calculate_trade_loss(
        symbol=symbol,
        action=action,
        volume=1.0,
        entry_price=entry_price,
        stop_loss=stop_loss,
    )

    if loss_one_lot <= 0:

        raise ValueError(
            "Некорректный риск на 1 lot."
        )

    raw_volume = (
        risk_money
        / loss_one_lot
    )

    normalized_volume = (
        floor_to_volume_step(
            raw_volume,
            volume_step,
        )
    )

    normalized_volume = min(
        normalized_volume,
        volume_max,
    )

    min_lot_loss = calculate_trade_loss(
        symbol=symbol,
        action=action,
        volume=volume_min,
        entry_price=entry_price,
        stop_loss=stop_loss,
    )

    if min_lot_loss > risk_money:

        return {

            "possible": False,

            "reason": (
                "Даже минимальный lot превышает "
                "допустимый денежный риск."
            ),

            "raw_volume": (
                raw_volume
            ),

            "volume": 0.0,

            "loss_one_lot": (
                loss_one_lot
            ),

            "min_lot_loss": (
                min_lot_loss
            ),
        }

    if normalized_volume < volume_min:

        return {

            "possible": False,

            "reason": (
                "Расчётный lot меньше минимально "
                "разрешённого брокером."
            ),

            "raw_volume": (
                raw_volume
            ),

            "volume": 0.0,

            "loss_one_lot": (
                loss_one_lot
            ),

            "min_lot_loss": (
                min_lot_loss
            ),
        }

    expected_loss = calculate_trade_loss(
        symbol=symbol,
        action=action,
        volume=normalized_volume,
        entry_price=entry_price,
        stop_loss=stop_loss,
    )

    return {

        "possible": True,

        "reason": None,

        "raw_volume": (
            raw_volume
        ),

        "volume": (
            normalized_volume
        ),

        "loss_one_lot": (
            loss_one_lot
        ),

        "min_lot_loss": (
            min_lot_loss
        ),

        "expected_loss": (
            expected_loss
        ),
    }


# ============================================================
# MARGIN
# ============================================================

def calculate_required_margin(
    symbol: str,
    action: str,
    volume: float,
    entry_price: float,
) -> float:
    """
    Рассчитывает необходимую маржу.
    """

    order_type = get_mt5_order_type(
        action
    )

    margin = mt5.order_calc_margin(
        order_type,
        symbol,
        float(volume),
        float(entry_price),
    )

    if margin is None:

        raise RuntimeError(
            "MT5 order_calc_margin() "
            "не смог рассчитать маржу. "
            f"Ошибка: {mt5.last_error()}"
        )

    return float(
        margin
    )


# ============================================================
# ОТКРЫТЫЕ ПОЗИЦИИ
# ============================================================

def calculate_open_positions_risk() -> dict:
    """
    Рассчитывает оставшийся downside-risk
    открытых позиций до их Stop Loss.
    """

    positions = get_positions()

    total_remaining_risk = 0.0

    details = []

    risk_known = True

    problems = []

    for position in positions:

        symbol = position.symbol

        tick = mt5.symbol_info_tick(
            symbol
        )

        if tick is None:

            risk_known = False

            problems.append(
                f"Нет текущей цены для {symbol}."
            )

            continue

        stop_loss = float(
            position.sl
        )

        if stop_loss <= 0:

            risk_known = False

            problems.append(
                f"Позиция #{position.ticket} "
                f"{symbol} не имеет Stop Loss."
            )

            details.append({

                "ticket": int(
                    position.ticket
                ),

                "symbol": (
                    symbol
                ),

                "volume": float(
                    position.volume
                ),

                "remaining_risk": None,

                "has_stop_loss": False,
            })

            continue

        if (
            position.type
            == mt5.POSITION_TYPE_BUY
        ):

            current_price = float(
                tick.bid
            )

            order_type = (
                mt5.ORDER_TYPE_BUY
            )

            direction = "long"

        elif (
            position.type
            == mt5.POSITION_TYPE_SELL
        ):

            current_price = float(
                tick.ask
            )

            order_type = (
                mt5.ORDER_TYPE_SELL
            )

            direction = "short"

        else:

            risk_known = False

            problems.append(
                f"Неизвестный тип позиции "
                f"#{position.ticket}."
            )

            continue

        pnl_to_stop = mt5.order_calc_profit(
            order_type,
            symbol,
            float(position.volume),
            current_price,
            stop_loss,
        )

        if pnl_to_stop is None:

            risk_known = False

            problems.append(
                f"Не удалось рассчитать риск "
                f"позиции #{position.ticket}."
            )

            continue

        pnl_to_stop = float(
            pnl_to_stop
        )

        remaining_risk = max(
            0.0,
            -pnl_to_stop,
        )

        total_remaining_risk += (
            remaining_risk
        )

        details.append({

            "ticket": int(
                position.ticket
            ),

            "symbol": (
                symbol
            ),

            "direction": (
                direction
            ),

            "volume": float(
                position.volume
            ),

            "current_price": (
                current_price
            ),

            "stop_loss": (
                stop_loss
            ),

            "remaining_risk": (
                remaining_risk
            ),

            "has_stop_loss": True,
        })

    return {

        "positions_count": (
            len(positions)
        ),

        "risk_known": (
            risk_known
        ),

        "total_remaining_risk": (
            total_remaining_risk
        ),

        "problems": (
            problems
        ),

        "details": (
            details
        ),
    }


# ============================================================
# ПРОВЕРКА ЦЕН
# ============================================================

def validate_order_prices(
    symbol: str,
    action: str,
    order_type: str,
    entry: float,
    stop_loss: float,
    take_profit: float,
) -> list[str]:
    """
    Проверяет структуру торговых уровней.
    """

    errors = []

    info = mt5.symbol_info(
        symbol
    )

    tick = mt5.symbol_info_tick(
        symbol
    )

    if info is None:

        return [
            f"Нет symbol_info({symbol})."
        ]

    if tick is None:

        return [
            f"Нет текущей котировки {symbol}."
        ]

    bid = float(
        tick.bid
    )

    ask = float(
        tick.ask
    )

    if action == "enter_long":

        if not (
            stop_loss
            < entry
            < take_profit
        ):

            errors.append(
                "Для LONG должно выполняться "
                "SL < Entry < TP."
            )

        if order_type == "limit":

            if not (
                entry < ask
            ):

                errors.append(
                    "BUY LIMIT должен находиться "
                    "ниже текущего Ask."
                )

        elif order_type == "stop":

            if not (
                entry > ask
            ):

                errors.append(
                    "BUY STOP должен находиться "
                    "выше текущего Ask."
                )

        elif order_type == "market":

            deviation = (
                abs(
                    ask - entry
                )
                / ask
                * 100.0
            )

            if (
                deviation
                > MAX_MARKET_ENTRY_DEVIATION_PERCENT
            ):

                errors.append(
                    "Текущий Ask слишком далеко ушёл "
                    "от market-entry Claude."
                )

    elif action == "enter_short":

        if not (
            take_profit
            < entry
            < stop_loss
        ):

            errors.append(
                "Для SHORT должно выполняться "
                "TP < Entry < SL."
            )

        if order_type == "limit":

            if not (
                entry > bid
            ):

                errors.append(
                    "SELL LIMIT должен находиться "
                    "выше текущего Bid."
                )

        elif order_type == "stop":

            if not (
                entry < bid
            ):

                errors.append(
                    "SELL STOP должен находиться "
                    "ниже текущего Bid."
                )

        elif order_type == "market":

            deviation = (
                abs(
                    bid - entry
                )
                / bid
                * 100.0
            )

            if (
                deviation
                > MAX_MARKET_ENTRY_DEVIATION_PERCENT
            ):

                errors.append(
                    "Текущий Bid слишком далеко ушёл "
                    "от market-entry Claude."
                )

    else:

        errors.append(
            f"Неизвестный action: {action}."
        )

    if order_type not in (
        "market",
        "limit",
        "stop",
    ):

        errors.append(
            f"Недопустимый order_type: "
            f"{order_type}."
        )

    point = float(
        info.point
    )

    stops_level = int(
        info.trade_stops_level
    )

    minimum_distance = (
        stops_level
        * point
    )

    if minimum_distance > 0:

        sl_distance = abs(
            entry - stop_loss
        )

        tp_distance = abs(
            take_profit - entry
        )

        if (
            sl_distance
            < minimum_distance
        ):

            errors.append(
                "Stop Loss находится ближе "
                "минимально разрешённой брокером дистанции."
            )

        if (
            tp_distance
            < minimum_distance
        ):

            errors.append(
                "Take Profit находится ближе "
                "минимально разрешённой брокером дистанции."
            )

    return errors


# ============================================================
# R:R
# ============================================================

def calculate_rr(
    entry: float,
    stop_loss: float,
    take_profit: float,
) -> float:
    """
    Рассчитывает Reward / Risk.
    """

    risk_distance = abs(
        entry - stop_loss
    )

    reward_distance = abs(
        take_profit - entry
    )

    if risk_distance <= 0:

        return 0.0

    return (
        reward_distance
        / risk_distance
    )


# ============================================================
# БАЗОВАЯ СТРУКТУРА ОТЧЁТА
# ============================================================

def build_base_report(
    current_time: datetime,
    account,
    daily_state: dict,
    limits: dict,
    open_risk: dict,
) -> dict:
    """
    Формирует единую основу отчёта.

    Благодаря этому NO_TRADE, REJECTED и APPROVED
    всегда имеют одинаковую структуру.
    """

    balance = float(
        account.balance
    )

    equity = float(
        account.equity
    )

    free_margin = float(
        account.margin_free
    )

    enriched_limits = enrich_limits(
        limits=limits,
        balance=balance,
        equity=equity,
        existing_positions_risk=float(
            open_risk[
                "total_remaining_risk"
            ]
        ),
    )

    return {

        "decision": None,

        "approved": False,

        "fp_time": (
            current_time.isoformat()
        ),

        "reasons": [],

        "settings": {

            "starting_account_size": (
                STARTING_ACCOUNT_SIZE
            ),

            "target_risk_per_trade_percent": (
                TARGET_RISK_PER_TRADE_PERCENT
            ),

            "minimum_confidence": (
                MIN_CONFIDENCE
            ),

            "breach_safety_buffer_percent": (
                BREACH_SAFETY_BUFFER_PERCENT
            ),
        },

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
            enriched_limits
        ),

        "open_positions_risk": (
            open_risk
        ),

        "trade": {},
    }


# ============================================================
# ГЛАВНАЯ ПРОВЕРКА
# ============================================================

@observe("analysis")
def evaluate_trade(
    analysis: dict,
    symbol: str = SYMBOL,
) -> dict:
    """
    Главная функция Risk Manager.

    Никаких ордеров не отправляет.
    """

    current_time = now_fp()

    account = get_account_info()

    positions = get_positions()

    daily_state = get_daily_state(
        account,
        positions,
    )

    base_limits = (
        calculate_fundingpips_limits(
            daily_state
        )
    )

    open_risk = (
        calculate_open_positions_risk()
    )

    report = build_base_report(
        current_time=current_time,
        account=account,
        daily_state=daily_state,
        limits=base_limits,
        open_risk=open_risk,
    )

    limits = report[
        "limits"
    ]

    balance = report[
        "account"
    ][
        "balance"
    ]

    equity = report[
        "account"
    ][
        "equity"
    ]

    free_margin = report[
        "account"
    ][
        "free_margin"
    ]

    reasons = report[
        "reasons"
    ]

    recommendation = analysis.get(
        "recommendation",
        {}
    )

    action = recommendation.get(
        "action"
    )

    confidence = recommendation.get(
        "confidence",
        ""
    )

    order_type = recommendation.get(
        "order_type"
    )

    # ========================================================
    # БАЗОВАЯ TRADE INFO
    # ========================================================

    report[
        "trade"
    ] = {

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
            confidence
        ),
    }

    # ========================================================
    # STAY OUT
    # ========================================================

    if action == "stay_out":

        report[
            "decision"
        ] = "NO_TRADE"

        report[
            "approved"
        ] = False

        reasons.append(
            "Claude рекомендовал stay_out."
        )

        return report

    # ========================================================
    # DATA QUALITY
    # ========================================================

    data_quality = analysis.get(
        "data_quality",
        {}
    )

    if not bool(
        data_quality.get(
            "sufficient",
            False,
        )
    ):

        reasons.append(
            "Claude сообщил, что данных "
            "недостаточно для анализа."
        )

    # ========================================================
    # CONFIDENCE
    # ========================================================

    if not confidence_is_allowed(
        confidence
    ):

        reasons.append(
            f"Confidence '{confidence}' ниже "
            f"минимального '{MIN_CONFIDENCE}'."
        )

    # ========================================================
    # DAILY BASELINE
    # ========================================================

    if not bool(
        daily_state.get(
            "trusted",
            False,
        )
    ):

        reasons.append(
            "Daily baseline FundingPips "
            "не считается надёжно зафиксированным."
        )

    # ========================================================
    # EXISTING POSITIONS
    # ========================================================

    if not open_risk[
        "risk_known"
    ]:

        reasons.append(
            "Невозможно точно определить риск "
            "существующих позиций."
        )

    # ========================================================
    # CURRENT PROP STATUS
    # ========================================================

    safety_buffer = limits[
        "safety_buffer_money"
    ]

    daily_room = limits[
        "current_daily_room"
    ]

    max_room = limits[
        "current_max_room"
    ]

    if (
        daily_room
        <= safety_buffer
    ):

        reasons.append(
            "Счёт слишком близко к "
            "FundingPips Daily Loss floor."
        )

    if (
        max_room
        <= safety_buffer
    ):

        reasons.append(
            "Счёт слишком близко к "
            "FundingPips Max Loss floor."
        )

    # ========================================================
    # TRADE LEVELS
    # ========================================================

    entry = recommendation.get(
        "entry_price"
    )

    stop_loss = recommendation.get(
        "stop_loss"
    )

    take_profit = recommendation.get(
        "take_profit"
    )

    if (
        entry is None
        or stop_loss is None
        or take_profit is None
    ):

        reasons.append(
            "Claude не передал полный набор "
            "Entry / SL / TP."
        )

        report[
            "decision"
        ] = "REJECTED"

        return report

    entry = float(
        entry
    )

    stop_loss = float(
        stop_loss
    )

    take_profit = float(
        take_profit
    )

    # ========================================================
    # PRICE VALIDATION
    # ========================================================

    price_errors = validate_order_prices(
        symbol=symbol,
        action=action,
        order_type=order_type,
        entry=entry,
        stop_loss=stop_loss,
        take_profit=take_profit,
    )

    reasons.extend(
        price_errors
    )

    # ========================================================
    # INTERNAL RISK
    # ========================================================

    internal_risk_money = (
        STARTING_ACCOUNT_SIZE
        * TARGET_RISK_PER_TRADE_PERCENT
        / 100.0
    )

    existing_positions_risk = float(
        open_risk[
            "total_remaining_risk"
        ]
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
        internal_risk_money,
        available_prop_risk,
    )

    if risk_budget <= 0:

        reasons.append(
            "Нет доступного risk budget "
            "для новой сделки."
        )

    # ========================================================
    # POSITION SIZE
    # ========================================================

    lot_result = None

    if risk_budget > 0:

        try:

            lot_result = (
                calculate_position_size(
                    symbol=symbol,
                    action=action,
                    entry_price=entry,
                    stop_loss=stop_loss,
                    risk_money=risk_budget,
                )
            )

        except Exception as error:

            reasons.append(
                f"Ошибка расчёта lot: {error}"
            )

    if (
        lot_result is not None
        and not lot_result[
            "possible"
        ]
    ):

        reasons.append(
            lot_result[
                "reason"
            ]
        )

    volume = 0.0

    expected_loss = None

    expected_profit = None

    required_margin = None

    projected_equity_all_stops = None

    projected_balance_all_stops = None

    # ========================================================
    # ЕСЛИ LOT РАССЧИТАН
    # ========================================================

    if (
        lot_result is not None
        and lot_result[
            "possible"
        ]
    ):

        volume = float(
            lot_result[
                "volume"
            ]
        )

        expected_loss = float(
            lot_result[
                "expected_loss"
            ]
        )

        expected_profit = (
            calculate_trade_profit(
                symbol=symbol,
                action=action,
                volume=volume,
                entry_price=entry,
                take_profit=take_profit,
            )
        )

        required_margin = (
            calculate_required_margin(
                symbol=symbol,
                action=action,
                volume=volume,
                entry_price=entry,
            )
        )

        if (
            required_margin
            > free_margin
        ):

            reasons.append(
                "Недостаточно Free Margin "
                "для рассчитанного объёма."
            )

        projected_equity_all_stops = (
            equity
            - existing_positions_risk
            - expected_loss
        )

        projected_balance_all_stops = (
            balance
            - existing_positions_risk
            - expected_loss
        )

        projected_conservative_value = min(
            projected_equity_all_stops,
            projected_balance_all_stops,
        )

        if (
            projected_conservative_value
            <= (
                limits[
                    "daily_floor"
                ]
                + safety_buffer
            )
        ):

            reasons.append(
                "После Stop Loss счёт оказался бы "
                "слишком близко или ниже "
                "Daily Loss safety level."
            )

        if (
            projected_conservative_value
            <= (
                limits[
                    "max_loss_floor"
                ]
                + safety_buffer
            )
        ):

            reasons.append(
                "После Stop Loss счёт оказался бы "
                "слишком близко или ниже "
                "Max Loss safety level."
            )

    # ========================================================
    # R:R
    # ========================================================

    rr = calculate_rr(
        entry,
        stop_loss,
        take_profit,
    )

    # ========================================================
    # TRADE REPORT
    # ========================================================

    report[
        "trade"
    ].update({

        "entry_price": (
            entry
        ),

        "stop_loss": (
            stop_loss
        ),

        "take_profit": (
            take_profit
        ),

        "risk_reward": (
            rr
        ),

        "internal_risk_limit_money": (
            internal_risk_money
        ),

        "available_prop_risk": (
            available_prop_risk
        ),

        "risk_budget": (
            risk_budget
        ),

        "raw_volume": (
            lot_result.get(
                "raw_volume"
            )
            if lot_result
            else None
        ),

        "volume": (
            volume
        ),

        "expected_loss": (
            expected_loss
        ),

        "expected_loss_percent_start": (
            (
                expected_loss
                / STARTING_ACCOUNT_SIZE
                * 100.0
            )
            if expected_loss is not None
            else None
        ),

        "expected_profit": (
            expected_profit
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
    })

    # ========================================================
    # FINAL DECISION
    # ========================================================

    if reasons:

        report[
            "decision"
        ] = "REJECTED"

        report[
            "approved"
        ] = False

    else:

        report[
            "decision"
        ] = "APPROVED"

        report[
            "approved"
        ] = True

    return report


# ============================================================
# ВЫВОД ОТЧЁТА
# ============================================================

def print_risk_report(
    report: dict,
):
    """
    Выводит результат Risk Manager.
    """

    print()
    print("=" * 80)
    print("RISK MANAGER")
    print("=" * 80)

    print(
        f"FP Time:       "
        f"{report['fp_time']}"
    )

    print(
        f"Решение:       "
        f"{report['decision']}"
    )

    # ========================================================
    # REASONS
    # ========================================================

    print()
    print("ПРИЧИНЫ")
    print("-" * 80)

    if report[
        "reasons"
    ]:

        for reason in report[
            "reasons"
        ]:

            print(
                f"- {reason}"
            )

    else:

        print(
            "Нарушений не обнаружено."
        )

    # ========================================================
    # ACCOUNT
    # ========================================================

    account = report[
        "account"
    ]

    print()
    print("СЧЁТ")
    print("-" * 80)

    print(
        f"Balance:       "
        f"{account['balance']:.2f}"
    )

    print(
        f"Equity:        "
        f"{account['equity']:.2f}"
    )

    print(
        f"Free Margin:   "
        f"{account['free_margin']:.2f}"
    )

    # ========================================================
    # DAILY STATE
    # ========================================================

    daily_state = report[
        "daily_state"
    ]

    print()
    print("FUNDINGPIPS DAY")
    print("-" * 80)

    print(
        f"Day:           "
        f"{daily_state['fp_day']}"
    )

    print(
        f"Opening Bal:   "
        f"{daily_state['opening_balance']:.2f}"
    )

    print(
        f"Opening Equity:"
        f" {daily_state['opening_equity']:.2f}"
    )

    print(
        f"Baseline:      "
        f"{daily_state['daily_baseline']:.2f}"
    )

    print(
        f"Trusted:       "
        f"{daily_state['trusted']}"
    )

    print(
        f"Method:        "
        f"{daily_state['capture_method']}"
    )

    # ========================================================
    # LIMITS
    # ========================================================

    limits = report[
        "limits"
    ]

    print()
    print("FUNDINGPIPS LIMITS")
    print("-" * 80)

    print(
        f"Daily Loss:    "
        f"{limits['daily_loss_money']:.2f}"
    )

    print(
        f"Daily Floor:   "
        f"{limits['daily_floor']:.2f}"
    )

    print(
        f"Daily Room:    "
        f"{limits['current_daily_room']:.2f}"
    )

    print(
        f"Daily free:    "
        f"{limits['daily_available_for_new_trade']:.2f}"
    )

    print()

    print(
        f"Max Loss:      "
        f"{limits['max_loss_money']:.2f}"
    )

    print(
        f"Max Floor:     "
        f"{limits['max_loss_floor']:.2f}"
    )

    print(
        f"Max Room:      "
        f"{limits['current_max_room']:.2f}"
    )

    print(
        f"Max free:      "
        f"{limits['max_available_for_new_trade']:.2f}"
    )

    print()

    print(
        f"Safety Buffer: "
        f"{limits['safety_buffer_money']:.2f}"
    )

    # ========================================================
    # EXISTING POSITIONS
    # ========================================================

    existing = report[
        "open_positions_risk"
    ]

    print()
    print("ОТКРЫТЫЕ ПОЗИЦИИ")
    print("-" * 80)

    print(
        f"Количество:    "
        f"{existing['positions_count']}"
    )

    print(
        f"Risk known:    "
        f"{existing['risk_known']}"
    )

    print(
        f"Open risk:     "
        f"{existing['total_remaining_risk']:.2f}"
    )

    if existing[
        "problems"
    ]:

        print()

        for problem in existing[
            "problems"
        ]:

            print(
                f"- {problem}"
            )

    # ========================================================
    # TRADE
    # ========================================================

    trade = report[
        "trade"
    ]

    print()
    print("НОВАЯ СДЕЛКА")
    print("-" * 80)

    print(
        f"Action:        "
        f"{trade.get('action')}"
    )

    print(
        f"Order type:    "
        f"{trade.get('order_type')}"
    )

    print(
        f"Confidence:    "
        f"{trade.get('confidence')}"
    )

    if (
        trade.get(
            "entry_price"
        )
        is not None
    ):

        print(
            f"Entry:         "
            f"{trade['entry_price']}"
        )

        print(
            f"Stop Loss:     "
            f"{trade['stop_loss']}"
        )

        print(
            f"Take Profit:   "
            f"{trade['take_profit']}"
        )

        print(
            f"R:R:           "
            f"{trade['risk_reward']:.2f}"
        )

        print()

        print(
            f"Risk budget:   "
            f"{trade['risk_budget']:.2f}"
        )

        print(
            f"Raw lot:       "
            f"{trade['raw_volume']}"
        )

        print(
            f"Final lot:     "
            f"{trade['volume']}"
        )

        if (
            trade[
                "expected_loss"
            ]
            is not None
        ):

            print(
                f"Loss at SL:    "
                f"{trade['expected_loss']:.2f}"
            )

            print(
                f"Risk %:        "
                f"{trade['expected_loss_percent_start']:.3f}%"
            )

        if (
            trade[
                "expected_profit"
            ]
            is not None
        ):

            print(
                f"Profit at TP:  "
                f"{trade['expected_profit']:.2f}"
            )

        if (
            trade[
                "required_margin"
            ]
            is not None
        ):

            print(
                f"Margin:        "
                f"{trade['required_margin']:.2f}"
            )

        if (
            trade[
                "projected_equity_all_stops"
            ]
            is not None
        ):

            print()

            print(
                f"Equity @ SL:   "
                f"{trade['projected_equity_all_stops']:.2f}"
            )

    else:

        print(
            "Entry/SL/TP:    отсутствуют"
        )

    # ========================================================
    # FINAL
    # ========================================================

    print()
    print("=" * 80)

    if report[
        "approved"
    ]:

        print(
            "[APPROVED] Сделка прошла Risk Manager."
        )

        print(
            "[SAFE MODE] Исполнение в MT5 "
            "пока НЕ производится."
        )

    elif report[
        "decision"
    ] == "NO_TRADE":

        print(
            "[NO TRADE] Торгового действия нет."
        )

    else:

        print(
            "[REJECTED] Сделка заблокирована Risk Manager."
        )

    print("=" * 80)
