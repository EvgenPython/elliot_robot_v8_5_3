from datetime import datetime, timezone

import os

import MetaTrader5 as mt5
import pandas as pd
from dateutil import tz

from prop_time import FUNDINGPIPS_TZ, now_fp


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument

SYMBOL = active_instrument()


# ============================================================
# ВРЕМЯ
# ============================================================

# Наш текущий источник котировок — AMarkets.
# Его серверное время использует EET/EEST.
#
# Это нужно ТОЛЬКО внутри функции преобразования времени.
# Наружу из market_data.py AMarkets-время не передаётся.
AMARKETS_TZ = tz.gettz("Europe/Kyiv")

if AMARKETS_TZ is None:
    raise RuntimeError(
        "Не удалось определить timezone AMarkets."
    )


# По фактическому тесту нашего AMarkets Demo
# timestamp из MetaTrader5 выглядит как server wall-clock time.
#
# Поэтому интерпретируем:
#
# raw 21:00
#     ↓
# 21:00 AMarkets server time
#     ↓
# переводим в FundingPips UTC+3
#
# Летом время совпадёт.
# Зимой при необходимости появится корректная разница.
MT5_TIME_MODE = "server_wall"


# ============================================================
# ТАЙМФРЕЙМЫ
# ============================================================

TIMEFRAMES = {
    # Старшие контекстные таймфреймы.
    "D1": mt5.TIMEFRAME_D1,
    "H4": mt5.TIMEFRAME_H4,

    # Основной структурный торговый таймфрейм.
    "H1": mt5.TIMEFRAME_H1,

    # Промежуточные закрытия M30 дают второй decision clock внутри каждого
    # часа. MT5 Python API поддерживает TIMEFRAME_M30 напрямую.
    "M30": mt5.TIMEFRAME_M30,

    # Младшие таймфреймы используются только для
    # уточнения структуры входа, локального импульса,
    # пробоя/ретеста и компактной структурной инвалидации.
    "M15": mt5.TIMEFRAME_M15,
    "M5": mt5.TIMEFRAME_M5,
}


# Мультирезолюционная история для одного полного анализа Claude.
#
# Идея не в том, чтобы бесконечно наращивать число свечей,
# а в том, чтобы дать достаточно истории на каждом масштабе:
#
# D1  — крупный режим и многомесячная структура;
# H4  — среднесрочные импульсы/коррекции;
# H1  — основной рабочий контекст;
# M15 — структура точки входа за последние ~2 суток;
# M5  — микро-контекст последних ~12 часов.
# Фактически загружаем чуть больше истории, чем отдаём в
# ежедневную cacheable-base Claude. Это нужно только для того,
# чтобы в любой момент дня у нас оставалось одинаковое количество
# закрытых баров ДО 00:00 FP Time плюс все новые бары текущих суток.
# Никакой рыночной интерпретации Python здесь не делает.
BAR_COUNTS = {
    "D1": 260,
    "H4": 320,
    "H1": 400,
    "M30": 320,
    "M15": 320,
    "M5": 480,
}


# ============================================================
# ПРЕОБРАЗОВАНИЕ ВРЕМЕНИ MT5 -> FUNDINGPIPS
# ============================================================

def mt5_timestamp_to_fp(timestamp: int) -> datetime:
    """
    Преобразует timestamp MetaTrader 5
    во время FundingPips Platform Time (UTC+3).

    Весь остальной проект получает только FP Time.
    """

    timestamp = int(timestamp)

    raw_utc = datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )

    if MT5_TIME_MODE == "utc":

        # Если когда-нибудь терминал будет реально
        # отдавать Unix timestamp в UTC.
        actual_utc = raw_utc

    elif MT5_TIME_MODE == "server_wall":

        # AMarkets в нашем тесте отдаёт в timestamp
        # серверные часы как wall-clock.
        #
        # Убираем ошибочную UTC-метку:
        raw_naive = raw_utc.replace(
            tzinfo=None
        )

        # Назначаем реальную timezone AMarkets.
        broker_time = raw_naive.replace(
            tzinfo=AMARKETS_TZ
        )

        # Получаем реальный UTC.
        actual_utc = broker_time.astimezone(
            timezone.utc
        )

    else:
        raise ValueError(
            f"Неизвестный MT5_TIME_MODE: "
            f"{MT5_TIME_MODE}"
        )

    # И уже из реального UTC переводим
    # в единое время всего проекта.
    fp_time = actual_utc.astimezone(
        FUNDINGPIPS_TZ
    )

    return fp_time


# ============================================================
# ПРОВЕРКА СИМВОЛА
# ============================================================

def ensure_symbol(
    symbol: str = SYMBOL,
):
    """
    Проверяет наличие инструмента у брокера.

    Если инструмент существует, но отсутствует
    в Market Watch — добавляет его.

    Если XAUUSD не найден — ищет возможные
    альтернативные названия золота.
    """

    symbol_info = mt5.symbol_info(
        symbol
    )

    if symbol_info is None:

        symbols = mt5.symbols_get()

        candidates = []

        if symbols:
            for item in symbols:

                name = item.name.upper()

                if (
                    "XAU" in name
                    or "GOLD" in name
                ):
                    candidates.append(
                        item.name
                    )

        message = (
            f"Символ {symbol} "
            f"не найден у брокера."
        )

        if candidates:

            message += (
                "\n\nВозможные символы золота:\n- "
                + "\n- ".join(candidates)
            )

        raise RuntimeError(
            message
        )

    # Если символ не отображается
    # в Market Watch.
    if not symbol_info.visible:

        selected = mt5.symbol_select(
            symbol,
            True,
        )

        if not selected:

            raise RuntimeError(
                f"Не удалось добавить "
                f"{symbol} в Market Watch.\n"
                f"MT5 error: {mt5.last_error()}"
            )

        symbol_info = mt5.symbol_info(
            symbol
        )

        if symbol_info is None:

            raise RuntimeError(
                f"После symbol_select() "
                f"не удалось получить "
                f"symbol_info({symbol})."
            )

    return symbol_info


# ============================================================
# ТЕКУЩАЯ КОТИРОВКА
# ============================================================

def get_current_tick(
    symbol: str = SYMBOL,
) -> dict:
    """
    Получает текущие Bid / Ask.

    Время возвращается исключительно
    в FundingPips Platform Time.
    """

    tick = mt5.symbol_info_tick(
        symbol
    )

    if tick is None:

        raise RuntimeError(
            f"Не удалось получить текущий "
            f"tick {symbol}.\n"
            f"MT5 error: {mt5.last_error()}"
        )

    symbol_info = mt5.symbol_info(
        symbol
    )

    if symbol_info is None:

        raise RuntimeError(
            f"Не удалось получить "
            f"symbol_info({symbol})."
        )

    fp_time = mt5_timestamp_to_fp(
        tick.time
    )

    bid = float(
        tick.bid
    )

    ask = float(
        tick.ask
    )

    digits = int(
        symbol_info.digits
    )

    point = float(
        symbol_info.point
    )

    spread_price = round(
        ask - bid,
        digits,
    )

    if point > 0:

        spread_points = int(
            round(
                spread_price / point
            )
        )

    else:
        spread_points = 0

    return {

        "time_fp": fp_time.isoformat(),

        "bid": bid,
        "ask": ask,

        "spread_price": spread_price,
        "spread_points": spread_points,
    }


# ============================================================
# ПРЕОБРАЗОВАНИЕ БАРОВ MT5 -> DATAFRAME
# ============================================================

def rates_to_dataframe(
    rates,
) -> pd.DataFrame:
    """
    Преобразует массив баров MT5
    в pandas DataFrame.

    Все времена переводятся сразу
    в FundingPips Platform Time.
    """

    df = pd.DataFrame(
        rates
    )

    if df.empty:
        return df

    fp_times = []

    for timestamp in df["time"]:

        fp_time = mt5_timestamp_to_fp(
            int(timestamp)
        )

        fp_times.append(
            fp_time
        )

    # Удаляем исходный MT5 timestamp.
    df.drop(
        columns=["time"],
        inplace=True,
    )

    # Добавляем единое время проекта.
    df.insert(
        0,
        "time_fp",
        pd.to_datetime(fp_times),
    )

    # На всякий случай всегда сортируем
    # историю от старой свечи к новой.
    df.sort_values(
        by="time_fp",
        inplace=True,
    )

    df.reset_index(
        drop=True,
        inplace=True,
    )

    return df


# ============================================================
# ЗАКРЫТЫЕ СВЕЧИ
# ============================================================

def get_closed_bars(
    symbol: str,
    timeframe: int,
    count: int,
) -> pd.DataFrame:
    """
    Получает только полностью закрытые свечи.

    MT5:
        position 0 = текущая незакрытая свеча
        position 1 = последняя закрытая свеча

    Поэтому начинаем с position=1.
    """

    rates = mt5.copy_rates_from_pos(
        symbol,
        timeframe,
        1,
        count,
    )

    if rates is None:

        raise RuntimeError(
            f"Не удалось получить "
            f"исторические бары {symbol}.\n"
            f"MT5 error: {mt5.last_error()}"
        )

    if len(rates) == 0:

        raise RuntimeError(
            f"MT5 вернул 0 исторических "
            f"баров для {symbol}."
        )

    df = rates_to_dataframe(
        rates
    )

    if len(df) != count:

        print(
            f"[WARNING] "
            f"Запрошено {count} баров, "
            f"получено {len(df)}."
        )

    return df


# ============================================================
# ТЕКУЩАЯ НЕЗАКРЫТАЯ СВЕЧА
# ============================================================

def get_current_bar(
    symbol: str,
    timeframe: int,
) -> dict:
    """
    Получает текущую формирующуюся свечу.

    Эта свеча хранится отдельно
    и не смешивается с закрытыми барами.
    """

    rates = mt5.copy_rates_from_pos(
        symbol,
        timeframe,
        0,
        1,
    )

    if (
        rates is None
        or len(rates) == 0
    ):

        raise RuntimeError(
            f"Не удалось получить "
            f"текущую свечу {symbol}.\n"
            f"MT5 error: {mt5.last_error()}"
        )

    df = rates_to_dataframe(
        rates
    )

    row = df.iloc[0]

    result = {

        "time_fp": row[
            "time_fp"
        ].isoformat(),

        "open": float(
            row["open"]
        ),

        "high": float(
            row["high"]
        ),

        "low": float(
            row["low"]
        ),

        "close": float(
            row["close"]
        ),

        "tick_volume": int(
            row["tick_volume"]
        ),

        "spread": int(
            row["spread"]
        ),
    }

    if "real_volume" in row.index:

        result[
            "real_volume"
        ] = int(
            row["real_volume"]
        )

    return result


# ============================================================
# ДАННЫЕ ОДНОГО ТАЙМФРЕЙМА
# ============================================================

def get_timeframe_data(
    symbol: str,
    timeframe_name: str,
) -> dict:
    """
    Получает:

    current_bar
        текущая формирующаяся свеча

    closed_bars
        история закрытых свечей
    """

    if timeframe_name not in TIMEFRAMES:

        raise ValueError(
            f"Неизвестный timeframe: "
            f"{timeframe_name}"
        )

    timeframe = TIMEFRAMES[
        timeframe_name
    ]

    count = BAR_COUNTS[
        timeframe_name
    ]

    closed_bars = get_closed_bars(
        symbol=symbol,
        timeframe=timeframe,
        count=count,
    )

    current_bar = get_current_bar(
        symbol=symbol,
        timeframe=timeframe,
    )

    return {

        "current_bar": current_bar,

        "closed_bars": closed_bars,
    }


# ============================================================
# MARKET SNAPSHOT
# ============================================================

def get_market_snapshot(
    symbol: str = SYMBOL,
) -> dict:
    """
    Собирает полный snapshot рынка.

    Это будет базовая структура,
    которую позже будем превращать
    в JSON для Claude.
    """

    symbol_info = ensure_symbol(
        symbol
    )

    tick = get_current_tick(
        symbol
    )

    snapshot = {

        "instrument": symbol,

        # Реальное текущее FundingPips Platform Time.
        # НЕ равно времени последнего тика на закрытом рынке.
        "generated_at_fp": now_fp().isoformat(),

        # Отдельно сохраняем время последнего рыночного тика.
        "last_tick_time_fp": tick[
            "time_fp"
        ],

        "symbol_info": {

            "digits": int(
                symbol_info.digits
            ),

            "point": float(
                symbol_info.point
            ),

            "trade_tick_size": float(
                symbol_info.trade_tick_size
            ),

            "trade_tick_value": float(
                symbol_info.trade_tick_value
            ),

            "contract_size": float(
                symbol_info.trade_contract_size
            ),

            "volume_min": float(
                symbol_info.volume_min
            ),

            "volume_max": float(
                symbol_info.volume_max
            ),

            "volume_step": float(
                symbol_info.volume_step
            ),

            # Минимальные торговые дистанции брокера.
            # Claude не рассчитывает lot, но эти поля помогают
            # не предлагать технически невозможный сверхузкий SL.
            "trade_stops_level": int(
                getattr(
                    symbol_info,
                    "trade_stops_level",
                    0,
                )
            ),

            "trade_freeze_level": int(
                getattr(
                    symbol_info,
                    "trade_freeze_level",
                    0,
                )
            ),

            "trade_mode": int(
                getattr(
                    symbol_info,
                    "trade_mode",
                    0,
                )
            ),

            "description": str(
                getattr(
                    symbol_info,
                    "description",
                    symbol,
                )
            ),

            "currency_base": str(
                getattr(
                    symbol_info,
                    "currency_base",
                    "",
                )
            ),

            "currency_profit": str(
                getattr(
                    symbol_info,
                    "currency_profit",
                    "",
                )
            ),
        },

        "tick": tick,

        "timeframes": {},
    }

    for timeframe_name in TIMEFRAMES:

        print(
            f"[INFO] Получаем "
            f"{symbol} "
            f"{timeframe_name}..."
        )

        snapshot[
            "timeframes"
        ][timeframe_name] = (
            get_timeframe_data(
                symbol=symbol,
                timeframe_name=timeframe_name,
            )
        )

    return snapshot


# ============================================================
# ВЫВОД MARKET SNAPSHOT
# ============================================================

def print_market_snapshot(
    snapshot: dict,
    detailed: bool | None = None,
):
    """
    Выводит snapshot в консоль.

    Все времена отображаются исключительно
    в FundingPips Platform Time.
    """

    if detailed is None:
        detailed = os.getenv("ROBOT_CONSOLE_DETAIL", "compact").strip().lower() in {
            "full", "detailed", "debug", "1", "true", "yes"
        }

    if not detailed:
        tick = snapshot["tick"]
        print()
        print("-" * 80)
        print("РЫНОЧНЫЙ СНИМОК — КРАТКО")
        print("-" * 80)
        print(
            f"{snapshot['instrument']} | FP {snapshot['generated_at_fp']} | "
            f"Bid/Ask {tick['bid']}/{tick['ask']} | "
            f"spread {tick['spread_points']} пт."
        )
        for timeframe_name, data in snapshot["timeframes"].items():
            bars = data.get("closed_bars")
            count = len(bars) if bars is not None else 0
            latest = bars.iloc[-1] if hasattr(bars, "iloc") and count else None
            if latest is not None:
                time_value = latest.get("time_fp", latest.get("time", ""))
                print(
                    f"{timeframe_name:>3}: закрытых={count:<3} "
                    f"последняя={time_value} "
                    f"O={latest.get('open')} H={latest.get('high')} "
                    f"L={latest.get('low')} C={latest.get('close')}"
                )
        print(
            "[DETAIL] Полные свечи сохранены в debug/claude_market_payload.json; "
            "для полного вывода: ROBOT_CONSOLE_DETAIL=full"
        )
        print("-" * 80)
        return

    print()
    print("=" * 80)
    print("MARKET SNAPSHOT")
    print("=" * 80)

    print(
        f"Instrument: "
        f"{snapshot['instrument']}"
    )

    print(
        f"Current FP time: "
        f"{snapshot['generated_at_fp']}"
    )

    print(
        f"Last tick FP:    "
        f"{snapshot.get('last_tick_time_fp')}"
    )

    # ========================================================
    # CURRENT PRICE
    # ========================================================

    tick = snapshot[
        "tick"
    ]

    print()
    print(
        "ТЕКУЩАЯ ЦЕНА"
    )

    print("-" * 80)

    print(
        f"Last tick FP: "
        f"{tick['time_fp']}"
    )

    print(
        f"Bid:      "
        f"{tick['bid']}"
    )

    print(
        f"Ask:      "
        f"{tick['ask']}"
    )

    print(
        f"Spread:   "
        f"{tick['spread_price']} "
        f"({tick['spread_points']} points)"
    )

    # ========================================================
    # SYMBOL INFO
    # ========================================================

    info = snapshot[
        "symbol_info"
    ]

    print()
    print(
        "СПЕЦИФИКАЦИЯ"
    )

    print("-" * 80)

    print(
        f"Digits:        "
        f"{info['digits']}"
    )

    print(
        f"Point:         "
        f"{info['point']}"
    )

    print(
        f"Tick size:     "
        f"{info['trade_tick_size']}"
    )

    print(
        f"Tick value:    "
        f"{info['trade_tick_value']}"
    )

    print(
        f"Contract size: "
        f"{info['contract_size']}"
    )

    print(
        f"Min lot:       "
        f"{info['volume_min']}"
    )

    print(
        f"Max lot:       "
        f"{info['volume_max']}"
    )

    print(
        f"Lot step:      "
        f"{info['volume_step']}"
    )

    print(
        f"Stops level:   "
        f"{info.get('trade_stops_level', 0)} points"
    )

    print(
        f"Freeze level:  "
        f"{info.get('trade_freeze_level', 0)} points"
    )

    # ========================================================
    # TIMEFRAMES
    # ========================================================

    for (
        timeframe_name,
        data,
    ) in snapshot[
        "timeframes"
    ].items():

        print()
        print("=" * 80)

        print(
            f"{snapshot['instrument']} "
            f"{timeframe_name}"
        )

        print("=" * 80)

        current = data[
            "current_bar"
        ]

        print()
        print(
            "ТЕКУЩАЯ НЕЗАКРЫТАЯ СВЕЧА"
        )

        print("-" * 80)

        print(
            f"FP Time: "
            f"{current['time_fp']}"
        )

        print(
            f"O={current['open']}  "
            f"H={current['high']}  "
            f"L={current['low']}  "
            f"C={current['close']}"
        )

        closed = data[
            "closed_bars"
        ]

        print()

        print(
            f"Закрытых свечей получено: "
            f"{len(closed)}"
        )

        print()

        print(
            "ПОСЛЕДНИЕ 5 "
            "ЗАКРЫТЫХ СВЕЧЕЙ"
        )

        print("-" * 80)

        columns = [
            "time_fp",
            "open",
            "high",
            "low",
            "close",
            "tick_volume",
        ]

        print(
            closed[
                columns
            ]
            .tail(5)
            .to_string(
                index=False
            )
        )

    print()
    print("=" * 80)
