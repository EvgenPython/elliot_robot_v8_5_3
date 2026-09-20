from datetime import datetime, timedelta

from market_data import (
    SYMBOL,
    TIMEFRAMES,
    get_closed_bars,
    get_current_tick,
)
from prop_time import now_fp


# ============================================================
# RUNTIME / MARKET POLICY
# ============================================================

# Новые Claude-анализы разрешены только по будням.
#
# Рабочее окно проекта исторически было 08:00-24:00 FP Time.
# Для автономного режима вводим отдельный cutoff НОВЫХ ИДЕЙ в 23:00:
# последняя новая идея может появиться до 23:00, а последний час
# остаётся для технического контроля и безопасной отмены pending.
CLAUDE_ANALYSIS_START_HOUR = 8
CLAUDE_NEW_IDEA_CUTOFF_HOUR = 23

# Последний MT5 tick должен быть свежим. XAUUSD в активном рынке
# тикает значительно чаще, поэтому 3 минуты — большой safety margin.
MAX_LAST_TICK_AGE_SECONDS = 180

# Новая H1 должна быть действительно свежей после закрытия. При этом
# перезапуск Windows/MT5 или краткий сетевой сбой не должен молча съедать
# почти весь час торговых решений. До следующего закрытия H1 разрешаем
# догнать текущий час; последние пять минут оставляем как защиту от запуска
# уже устаревающей идеи непосредственно перед новой свечой.
MAX_CLOSED_H1_AGE_SECONDS = 55 * 60

# Минимальная задержка после формального закрытия H1, чтобы MT5
# успел окончательно перевести бар из position=0 в position=1.
MIN_SECONDS_AFTER_H1_CLOSE = 2


def _as_fp_datetime(value) -> datetime:
    """Нормализует datetime в aware datetime из уже FP-времени."""

    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("datetime должен содержать timezone")
        return value

    parsed = datetime.fromisoformat(str(value))

    if parsed.tzinfo is None:
        raise ValueError("ISO datetime должен содержать timezone")

    return parsed


def is_claude_analysis_window(
    current_fp: datetime | None = None,
) -> bool:
    """
    Разрешено ли создавать НОВУЮ торговую идею Claude.

    Технический мониторинг runner работает 24/7 и этим окном
    не ограничивается.
    """

    if current_fp is None:
        current_fp = now_fp()

    if current_fp.tzinfo is None:
        raise ValueError("current_fp должен содержать timezone")

    if current_fp.weekday() >= 5:
        return False

    return (
        CLAUDE_ANALYSIS_START_HOUR
        <= current_fp.hour
        < CLAUDE_NEW_IDEA_CUTOFF_HOUR
    )


def get_latest_closed_h1_time(
    symbol: str = SYMBOL,
) -> datetime:
    """Возвращает open-time последней полностью закрытой H1."""

    bars = get_closed_bars(
        symbol=symbol,
        timeframe=TIMEFRAMES["H1"],
        count=1,
    )

    value = bars.iloc[-1]["time_fp"]

    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()

    return _as_fp_datetime(value)


def inspect_market_runtime_gate(
    symbol: str = SYMBOL,
) -> dict:
    """
    Проверяет, можно ли запускать НОВЫЙ Claude-анализ.

    ВАЖНО:

    current_fp_time
        реальное текущее FundingPips Platform Time;

    last_tick_fp_time
        время последнего рыночного тика MT5.

    Эти два времени больше никогда не смешиваются.
    """

    current_fp = now_fp()
    tick = get_current_tick(symbol)

    last_tick_fp = _as_fp_datetime(
        tick["time_fp"]
    )

    latest_h1_open = get_latest_closed_h1_time(
        symbol=symbol
    )

    latest_h1_close = (
        latest_h1_open
        + timedelta(hours=1)
    )

    tick_age_seconds = (
        current_fp - last_tick_fp
    ).total_seconds()

    h1_age_seconds = (
        current_fp - latest_h1_close
    ).total_seconds()

    analysis_window_allowed = (
        is_claude_analysis_window(
            current_fp
        )
    )

    tick_fresh = (
        0
        <= tick_age_seconds
        <= MAX_LAST_TICK_AGE_SECONDS
    )

    h1_fresh = (
        MIN_SECONDS_AFTER_H1_CLOSE
        <= h1_age_seconds
        <= MAX_CLOSED_H1_AGE_SECONDS
    )

    reasons = []

    if current_fp.weekday() >= 5:
        reasons.append(
            "Сегодня выходной по FundingPips Platform Time."
        )
    elif current_fp.hour < CLAUDE_ANALYSIS_START_HOUR:
        reasons.append(
            "Рабочее окно новых торговых идей ещё не началось."
        )
    elif current_fp.hour >= CLAUDE_NEW_IDEA_CUTOFF_HOUR:
        reasons.append(
            "Cutoff новых торговых идей уже наступил."
        )

    if not tick_fresh:
        reasons.append(
            "Последний MT5 tick устарел: "
            f"age={tick_age_seconds:.1f}s, "
            f"limit={MAX_LAST_TICK_AGE_SECONDS}s."
        )

    if not h1_fresh:
        reasons.append(
            "Последняя закрытая H1 не подходит по свежести: "
            f"age_from_close={h1_age_seconds:.1f}s, "
            f"allowed={MIN_SECONDS_AFTER_H1_CLOSE}.."
            f"{MAX_CLOSED_H1_AGE_SECONDS}s."
        )

    allowed = (
        analysis_window_allowed
        and tick_fresh
        and h1_fresh
    )

    return {
        "allowed": allowed,
        "analysis_window_allowed": analysis_window_allowed,
        "tick_fresh": tick_fresh,
        "h1_fresh": h1_fresh,
        "current_fp_time": current_fp.isoformat(),
        "last_tick_fp_time": last_tick_fp.isoformat(),
        "last_tick_age_seconds": tick_age_seconds,
        "latest_closed_h1_time": latest_h1_open.isoformat(),
        "latest_closed_h1_close_time": latest_h1_close.isoformat(),
        "h1_age_from_close_seconds": h1_age_seconds,
        "max_last_tick_age_seconds": MAX_LAST_TICK_AGE_SECONDS,
        "max_closed_h1_age_seconds": MAX_CLOSED_H1_AGE_SECONDS,
        "analysis_start_hour": CLAUDE_ANALYSIS_START_HOUR,
        "new_idea_cutoff_hour": CLAUDE_NEW_IDEA_CUTOFF_HOUR,
        "reasons": reasons,
    }


def print_market_runtime_gate(
    gate: dict,
):
    """Печатает runtime/market freshness gate."""

    print()
    print("=" * 80)
    print("MARKET RUNTIME / FRESHNESS GATE")
    print("=" * 80)

    print(
        f"Current FP time:       "
        f"{gate.get('current_fp_time')}"
    )

    print(
        f"Last market tick FP:   "
        f"{gate.get('last_tick_fp_time')}"
    )

    print(
        f"Last tick age:         "
        f"{gate.get('last_tick_age_seconds', 0.0):.1f} sec"
    )

    print(
        f"Latest closed H1:      "
        f"{gate.get('latest_closed_h1_time')}"
    )

    print(
        f"H1 closed at:          "
        f"{gate.get('latest_closed_h1_close_time')}"
    )

    print(
        f"H1 age from close:     "
        f"{gate.get('h1_age_from_close_seconds', 0.0):.1f} sec"
    )

    print()
    print("POLICY")
    print("-" * 80)

    print(
        f"Claude window:         "
        f"Mon-Fri {gate.get('analysis_start_hour'):02d}:00-"
        f"{gate.get('new_idea_cutoff_hour'):02d}:00 FP"
    )

    print(
        f"Window allowed:        "
        f"{gate.get('analysis_window_allowed')}"
    )

    print(
        f"Tick fresh:            "
        f"{gate.get('tick_fresh')}"
    )

    print(
        f"Closed H1 fresh:       "
        f"{gate.get('h1_fresh')}"
    )

    print(
        f"Claude allowed:        "
        f"{gate.get('allowed')}"
    )

    reasons = gate.get(
        "reasons",
        [],
    )

    if reasons:
        print()
        print("BLOCKERS")
        print("-" * 80)
        for reason in reasons:
            print(f"- {reason}")

    if gate.get("allowed"):
        print()
        print(
            "[OK] Рынок свежий, последняя закрытая H1 актуальна, "
            "рабочее окно разрешает новый Claude-анализ."
        )
    else:
        print()
        print(
            "[BLOCKED] Новая торговая идея Claude сейчас запрещена."
        )

    print("=" * 80)
