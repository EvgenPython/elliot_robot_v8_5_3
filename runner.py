import time
from observability import emit, observe, context, start_session
import os
from datetime import datetime
from pathlib import Path

import MetaTrader5 as mt5

from analysis_schedule import DAILY_BASELINE_FULL_CLOSE_HOUR
from analysis_state import load_analysis_state
from execution_control import inspect_execution_safety_gate
from live_executor import print_live_execution_report
from main import main as run_main_cycle, run_m30_decision_cycle
from mt5_client import connect_mt5, disconnect_mt5
from pending_executor import (
    PENDING_MAX_H1_BARS,
    PENDING_ORDER_TYPES,
    execute_pending_cancel,
    inspect_pending_h1_validity,
    reconcile_active_pending_execution,
)
from prop_time import now_fp
from risk_manager import (
    get_account_info,
    get_daily_state,
    get_positions,
)
from runtime_policy import inspect_market_runtime_gate
from single_instance import (
    SingleInstanceError,
    SingleInstanceLock,
)
from trade_state import (
    get_active_plan,
    get_managed_positions,
)
from web_runtime_state import write_runner_status
from entry_watch import inspect_entry_trigger
from entry_check_cycle import run_entry_check
from decision_clock import (
    inspect_m30_decision_due,
    mark_m30_decision_attempt,
)
from console_log import install_console_log
from position_monitor import (
    inspect_position_monitor_due,
    run_position_monitor,
)


# ============================================================
# RUNNER SETTINGS
# ============================================================

from instruments import active_instrument

SYMBOL = active_instrument()

BASE_DIR = Path(__file__).resolve().parent
_COMMON_LOCK_ROOT = Path(os.environ.get("PROGRAMDATA", str(BASE_DIR)))
_LOCK_SYMBOL = "".join(
    character if character.isalnum() else "_" for character in SYMBOL
)
LOCK_PATH = _COMMON_LOCK_ROOT / "WaveFrame" / f"runner_{_LOCK_SYMBOL}.lock"

# Лёгкий polling MT5.
POLL_INTERVAL_SECONDS = 10

# Полный POSITION HOLD audit через существующий main.py.
POSITION_AUDIT_INTERVAL_SECONDS = 60

# Для market/not_sent/send_intent entry lifecycle.
ENTRY_RETRY_INTERVAL_SECONDS = 15

# Если Claude/API упал ДО регистрации H1, не долбим API каждые 10 сек.
SAME_H1_ANALYSIS_RETRY_SECONDS = 300

# Повтор отмены pending, если предыдущая попытка не смогла
# однозначно завершиться.
PENDING_CANCEL_RETRY_SECONDS = 60

# Короткий heartbeat, когда ничего не происходит.
HEARTBEAT_INTERVAL_SECONDS = 300

# На закрытом рынке одинаковый heartbeat раз в 5 минут создаёт сотни строк,
# не добавляя диагностической ценности.
MARKET_CLOSED_HEARTBEAT_INTERVAL_SECONDS = 3600

# При потере MT5 соединения.
RECONNECT_INTERVAL_SECONDS = 30


# ============================================================
# HELPERS
# ============================================================

def _parse_iso(value) -> datetime | None:
    if not value:
        return None

    try:
        result = datetime.fromisoformat(
            str(value)
        )
    except ValueError:
        return None

    if result.tzinfo is None:
        return None

    return result


def _seconds_since(
    timestamp: datetime | None,
) -> float:
    if timestamp is None:
        return float("inf")

    return max(
        0.0,
        (
            now_fp()
            - timestamp
        ).total_seconds(),
    )


def _mt5_connection_alive() -> bool:
    terminal = mt5.terminal_info()
    account = mt5.account_info()

    return bool(
        terminal is not None
        and account is not None
        and getattr(
            terminal,
            "connected",
            False,
        )
    )


def _print_runner_header():
    print()
    print("=" * 80)
    print("CLAUDE ROBOT — CONTINUOUS RUNNER")
    print("=" * 80)
    print(f"Symbol:                    {SYMBOL}")
    print(f"Polling:                   {POLL_INTERVAL_SECONDS} sec")
    print(
        "Position audit:            "
        f"{POSITION_AUDIT_INTERVAL_SECONDS} sec"
    )
    print(
        "Same H1 retry after error: "
        f"{SAME_H1_ANALYSIS_RETRY_SECONDS} sec"
    )
    print(f"Lock file:                 {LOCK_PATH}")
    print("[INFO] Технический мониторинг работает 24/7.")
    print("[INFO] Claude запускается только через все runtime gates.")
    print(
        "[POLICY] Daily baseline FULL: "
        f"{DAILY_BASELINE_FULL_CLOSE_HOUR:02d}:00 FP."
    )
    print(
        "[POLICY] Остальные новые H1: Scout -> свежий H1 decision; "
        "при событии -> глубокий FULL."
    )
    print(
        "[POLICY] Между H1: одна дополнительная M30 decision в xx:30; "
        "M15 подтверждает, M5 уточняет trigger."
    )
    print(
        "[POLICY] Managed position: M15 Scout + H1 deep Elliott review."
    )
    print(
        "[POLICY] SL: за экстремум предыдущей завершённой волны; "
        "TP: подтверждённая Fibonacci-проекция."
    )
    print(
        "[POSITION POLICY] При открытой позиции стоп только подтягивается; второй вход, доливка, "
        "разворот и автозакрытие запрещены."
    )
    print("=" * 80)


def _heartbeat_interval(market_gate: dict | None) -> int:
    if isinstance(market_gate, dict) and not market_gate.get("tick_fresh", True):
        return MARKET_CLOSED_HEARTBEAT_INTERVAL_SECONDS
    return HEARTBEAT_INTERVAL_SECONDS


def _print_heartbeat(
    daily_state: dict | None,
    market_gate: dict | None,
):
    print()
    print("-" * 80)
    print("RUNNER HEARTBEAT")
    print("-" * 80)
    print(f"FP Time:          {now_fp().isoformat()}")

    if daily_state:
        print(
            f"FP Day:           "
            f"{daily_state.get('fp_day')}"
        )
        print(
            f"Daily trusted:    "
            f"{daily_state.get('trusted')}"
        )
        print(
            f"Daily method:     "
            f"{daily_state.get('capture_method')}"
        )

    if market_gate:
        print(
            f"Market gate:      "
            f"{market_gate.get('allowed')}"
        )
        print(
            f"Window allowed:   "
            f"{market_gate.get('analysis_window_allowed')}"
        )
        print(
            f"Tick fresh:       "
            f"{market_gate.get('tick_fresh')}"
        )
        print(
            f"Latest H1:        "
            f"{market_gate.get('latest_closed_h1_time')}"
        )

    print("-" * 80)


@observe("scheduler", inputs=True)
def _run_full_cycle(
    reason: str,
    analysis_only: bool = False,
):
    print()
    print("=" * 80)
    print("RUNNER -> MAIN CYCLE")
    print("=" * 80)
    print(f"Reason: {reason}")
    print(f"Analysis only: {analysis_only}")
    print(f"FP Time: {now_fp().isoformat()}")
    print("=" * 80)

    # MT5 уже подключён runner-ом.
    run_main_cycle(
        manage_connection=False,
        analysis_only=analysis_only,
    )


def _refresh_daily_state() -> dict:
    account = get_account_info()
    positions = get_positions()

    return get_daily_state(
        account,
        positions,
    )


def _print_daily_rollover(
    previous_day: str | None,
    state: dict,
):
    current_day = str(
        state.get(
            "fp_day"
        )
    )

    if previous_day == current_day:
        return

    print()
    print("=" * 80)
    print("FUNDINGPIPS DAY ROLLOVER")
    print("=" * 80)
    print(f"Previous day:      {previous_day}")
    print(f"Current day:       {current_day}")
    print(
        f"Captured at FP:    "
        f"{state.get('captured_at_fp')}"
    )
    print(
        f"Opening Balance:   "
        f"{float(state.get('opening_balance', 0.0)):.2f}"
    )
    print(
        f"Opening Equity:    "
        f"{float(state.get('opening_equity', 0.0)):.2f}"
    )
    print(
        f"Daily baseline:    "
        f"{float(state.get('daily_baseline', 0.0)):.2f}"
    )
    print(
        f"Trusted:           "
        f"{state.get('trusted')}"
    )
    print(
        f"Capture method:    "
        f"{state.get('capture_method')}"
    )
    print("=" * 80)


def _pending_needs_session_cancel(
    plan: dict,
    market_gate: dict,
) -> bool:
    order_type = str(
        plan.get(
            "order_type",
            "",
        )
    )

    if order_type not in PENDING_ORDER_TYPES:
        return False

    # Вне окна НОВЫХ идей pending от старой идеи не переносим дальше.
    return not bool(
        market_gate.get(
            "analysis_window_allowed",
            False,
        )
    )


def _fresh_h1_analysis_due(
    market_gate: dict,
    *,
    last_attempt_h1: str | None,
    last_attempt_at: datetime | None,
) -> tuple[bool, str]:
    """Return whether the newest closed H1 still needs one analysis cycle.

    The same decision is used in flat mode and in both pending-order
    quarantine paths. This prevents an execution problem from starving the
    analysis clock without repeatedly rebuilding the already processed H1
    snapshot on every 10-second runner poll.
    """
    latest_h1 = str(
        market_gate.get("latest_closed_h1_time", "")
        or ""
    )
    if not market_gate.get("allowed", False) or not latest_h1:
        emit("scheduler", "h1_skipped", data={"h1": latest_h1, "reason": "market_gate", "gate": market_gate},
             repeat_key="h1", repeat_seconds=300)
        return False, latest_h1

    last_analyzed_h1 = str(
        load_analysis_state().get("last_analyzed_h1", "")
        or ""
    )
    if latest_h1 == last_analyzed_h1:
        emit("scheduler", "h1_skipped", data={"h1": latest_h1, "reason": "already_analyzed"},
             repeat_key="h1", repeat_seconds=300)
        return False, latest_h1

    same_retry = latest_h1 == str(last_attempt_h1 or "")
    retry_ready = (
        not same_retry
        or _seconds_since(last_attempt_at)
        >= SAME_H1_ANALYSIS_RETRY_SECONDS
    )
    emit("scheduler", "h1_decision", data={"h1": latest_h1, "due": retry_ready,
         "reason": "due" if retry_ready else "retry_cooldown", "last_analyzed_h1": last_analyzed_h1},
         repeat_key="h1", repeat_seconds=300)
    return retry_ready, latest_h1


def _pending_cancel_reason(plan: dict, market_gate: dict) -> str | None:
    """Cancel only at explicit invalidation/session end or bounded TTL."""
    if _pending_needs_session_cancel(plan, market_gate):
        return "Рабочее окно новых торговых идей завершено. Pending не переносится дальше."
    if str(plan.get("execution_status")).lower() == "cancel_requested":
        return str(plan.get("cancel_reason") or "Повтор ранее запрошенной отмены pending.")
    current_h1 = market_gate.get("latest_closed_h1_time")
    validity = inspect_pending_h1_validity(plan, current_h1)
    if current_h1 and not validity.get("valid", False):
        return (
            "Срок pending истёк: "
            f"source H1={validity.get('source_h1')}; current H1={current_h1}; "
            f"age={validity.get('age_h1_bars')} bars; "
            f"max={PENDING_MAX_H1_BARS}."
        )
    return None


# ============================================================
# RUNNER
# ============================================================

def run_forever():
    start_session("runner")
    _print_runner_header()

    connected = False
    previous_fp_day = None

    last_position_audit_at = None
    last_entry_retry_at = None
    last_heartbeat_at = None
    last_pending_cancel_attempt_at = None
    last_pending_block_log_at = None
    last_pending_block_fingerprint = None

    last_analysis_attempt_h1 = None
    last_analysis_attempt_at = None

    while True:
        try:
            # =================================================
            # CONNECTION
            # =================================================

            if not connected or not _mt5_connection_alive():
                if connected:
                    try:
                        mt5.shutdown()
                    except Exception:
                        pass

                connected = connect_mt5()

                if not connected:
                    write_runner_status(
                        connected=False,
                        daily_state=None,
                        status="reconnecting",
                        last_error=mt5.last_error(),
                    )

                    print()
                    print(
                        "[RUNNER] MT5 недоступен. "
                        f"Повтор через {RECONNECT_INTERVAL_SECONDS} сек."
                    )
                    time.sleep(
                        RECONNECT_INTERVAL_SECONDS
                    )
                    continue

                print()
                print(
                    "[RUNNER] Постоянное MT5 соединение установлено."
                )

            # =================================================
            # DAILY ROLLOVER — 24/7
            # =================================================

            daily_state = _refresh_daily_state()

            current_day = str(
                daily_state.get(
                    "fp_day"
                )
            )

            _print_daily_rollover(
                previous_fp_day,
                daily_state,
            )

            previous_fp_day = current_day

            write_runner_status(
                connected=True,
                daily_state=daily_state,
                status="running",
            )

            # =================================================
            # POSITION HOLD — 24/7
            # =================================================

            managed_positions = get_managed_positions()

            if managed_positions:
                if (
                    _seconds_since(
                        last_position_audit_at
                    )
                    >= POSITION_AUDIT_INTERVAL_SECONDS
                ):
                    try:
                        _run_full_cycle("POSITION_HOLD_AUDIT")
                    except Exception as audit_error:
                        emit("position", "technical_audit_failed", level="ERROR",
                             data={"error": str(audit_error)})
                        print(f"[POSITION AUDIT ERROR] {audit_error}; scheduled analysis continues.")
                    last_position_audit_at = now_fp()

                # Технический audit выше только сверяет MT5 и риск. Отдельный
                # Position monitor продолжает волновой/структурный анализ:
                # дешёвый Scout на каждой закрытой M15 и глубокий review на
                # каждой закрытой H1 либо при смысловом M15-событии.
                position_market_gate = inspect_market_runtime_gate(
                    symbol=SYMBOL
                )
                monitor_due = inspect_position_monitor_due(
                    symbol=SYMBOL,
                    managed_positions=managed_positions,
                    tick_fresh=bool(
                        position_market_gate.get("tick_fresh", False)
                    ),
                )
                emit("scheduler", "position_due", data=monitor_due,
                     repeat_key="position_due", repeat_seconds=300)
                if monitor_due.get("due"):
                    run_position_monitor(
                        symbol=SYMBOL,
                        due=monitor_due,
                    )

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            # =================================================
            # ACTIVE PLAN / PENDING — 24/7
            # =================================================

            active_plan = get_active_plan()

            if active_plan is not None:
                order_type = str(
                    active_plan.get(
                        "order_type",
                        "",
                    )
                )

                # Pending reconciliation лёгкий и не вызывает Claude.
                if order_type in PENDING_ORDER_TYPES:
                    pending_report = (
                        reconcile_active_pending_execution(
                            symbol=SYMBOL
                        )
                    )

                    emit("scheduler", "pending_reconciliation", data=pending_report,
                         repeat_key="pending", repeat_seconds=300)
                    if pending_report.get(
                        "blocked",
                        False,
                    ):
                        fingerprint = (
                            str(pending_report.get("decision")),
                            tuple(
                                str(item)
                                for item in pending_report.get("errors", [])
                            ),
                        )
                        if (
                            fingerprint != last_pending_block_fingerprint
                            or _seconds_since(last_pending_block_log_at)
                            >= HEARTBEAT_INTERVAL_SECONDS
                        ):
                            print()
                            print("=" * 80)
                            print(
                                "[EXECUTION QUARANTINE] "
                                "PENDING RECONCILIATION BLOCKED"
                            )
                            print("=" * 80)
                            print(
                                f"Decision: {pending_report.get('decision')}"
                            )
                            for error in pending_report.get("errors", []):
                                print(f"- {error}")
                            print(
                                "[SAFETY] Новые ордера заблокированы; "
                                "наблюдение рынка остаётся активным."
                            )
                            print("=" * 80)
                            last_pending_block_log_at = now_fp()
                            last_pending_block_fingerprint = fingerprint

                        # Execution failure must never starve the H1 clock.
                        # The analysis-only cycle may call Claude and publish
                        # the result, but cannot create a new trade plan or
                        # send an entry order.
                        market_gate = inspect_market_runtime_gate(
                            symbol=SYMBOL
                        )
                        analysis_due, latest_h1 = _fresh_h1_analysis_due(
                            market_gate,
                            last_attempt_h1=last_analysis_attempt_h1,
                            last_attempt_at=last_analysis_attempt_at,
                        )
                        if analysis_due:
                            last_analysis_attempt_h1 = latest_h1
                            last_analysis_attempt_at = now_fp()
                            _run_full_cycle(
                                "FRESH_NEW_H1_EXECUTION_QUARANTINE",
                                analysis_only=True,
                            )

                        time.sleep(
                            POLL_INTERVAL_SECONDS
                        )
                        continue

                    last_pending_block_log_at = None
                    last_pending_block_fingerprint = None

                    # Reconciliation мог превратить pending в managed position.
                    if get_managed_positions():
                        _run_full_cycle(
                            "PENDING_FILLED_TO_POSITION"
                        )
                        last_position_audit_at = now_fp()
                        time.sleep(
                            POLL_INTERVAL_SECONDS
                        )
                        continue

                    active_plan = get_active_plan()

                    if active_plan is None:
                        time.sleep(
                            POLL_INTERVAL_SECONDS
                        )
                        continue

                    market_gate = inspect_market_runtime_gate(
                        symbol=SYMBOL
                    )

                    # Pending живёт ограниченное число H1 и отменяется на
                    # границе рабочего окна. Ошибка отмены не глушит анализ.
                    cancel_reason = _pending_cancel_reason(active_plan, market_gate)
                    if cancel_reason:
                        if (
                            _seconds_since(
                                last_pending_cancel_attempt_at
                            )
                            >= PENDING_CANCEL_RETRY_SECONDS
                        ):
                            print()
                            print(
                                "[RUNNER] Срок pending завершён. "
                                "Пытаемся безопасно отменить pending."
                            )

                            cancel_report = execute_pending_cancel(
                                plan=active_plan,
                                reason=cancel_reason,
                            )

                            print_live_execution_report(
                                cancel_report
                            )

                            last_pending_cancel_attempt_at = now_fp()

                    # Новый H1-анализ не создаёт второй торговый план, пока
                    # pending жив: он выполняется в observation-only и ровно
                    # один раз регистрирует текущую H1.
                    analysis_due, current_h1 = _fresh_h1_analysis_due(
                        market_gate,
                        last_attempt_h1=last_analysis_attempt_h1,
                        last_attempt_at=last_analysis_attempt_at,
                    )
                    if analysis_due:
                        last_analysis_attempt_h1 = current_h1
                        last_analysis_attempt_at = now_fp()
                        _run_full_cycle(
                            "NEW_H1_WITH_ACTIVE_PENDING",
                            analysis_only=get_active_plan() is not None,
                        )

                    time.sleep(
                        POLL_INTERVAL_SECONDS
                    )
                    continue

                # Market-plan / прочий active plan:
                # Executor должен закончить SEND_INTENT/TTL lifecycle.
                if (
                    _seconds_since(
                        last_entry_retry_at
                    )
                    >= ENTRY_RETRY_INTERVAL_SECONDS
                ):
                    _run_full_cycle(
                        "ACTIVE_ENTRY_PLAN"
                    )
                    last_entry_retry_at = now_fp()

                # Market SEND_INTENT/reconciliation must not starve the H1 clock either.
                if not get_managed_positions():
                    observation_gate = inspect_market_runtime_gate(symbol=SYMBOL)
                    observation_due, observation_h1 = _fresh_h1_analysis_due(
                        observation_gate, last_attempt_h1=last_analysis_attempt_h1,
                        last_attempt_at=last_analysis_attempt_at)
                    if observation_due:
                        last_analysis_attempt_h1 = observation_h1
                        last_analysis_attempt_at = now_fp()
                        _run_full_cycle("FRESH_NEW_H1_ACTIVE_ENTRY_OBSERVATION", analysis_only=True)

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            # =================================================
            # FLAT — NEW H1 TRIGGER
            # =================================================

            market_gate = inspect_market_runtime_gate(
                symbol=SYMBOL
            )

            emit("scheduler", "market_gate", data=market_gate, repeat_key="market", repeat_seconds=300)
            if not market_gate.get(
                "allowed",
                False,
            ):
                if (
                    _seconds_since(
                        last_heartbeat_at
                    )
                    >= _heartbeat_interval(market_gate)
                ):
                    _print_heartbeat(
                        daily_state,
                        market_gate,
                    )
                    last_heartbeat_at = now_fp()

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )
                continue

            observation_only = False

            # Daily risk state can block execution, but it cannot stop the
            # market clock or erase an H1 from the analytical history.
            if not bool(daily_state.get("trusted", False)):
                observation_only = True
                if (
                    _seconds_since(
                        last_heartbeat_at
                    )
                    >= _heartbeat_interval(market_gate)
                ):
                    _print_heartbeat(
                        daily_state,
                        market_gate,
                    )
                    print(
                        "[RUNNER] WAIT: FundingPips daily baseline "
                        "Trusted=False; Claude и execution не запускаются "
                        "до безопасного восстановления базы."
                    )
                    last_heartbeat_at = now_fp()

            execution_safety = inspect_execution_safety_gate()

            if (
                not execution_safety.get("configuration_valid", False)
                or (
                    execution_safety.get("mode") == "LIVE"
                    and not execution_safety.get("order_send_allowed", False)
                )
            ):
                observation_only = True
                if (
                    _seconds_since(
                        last_heartbeat_at
                    )
                    >= _heartbeat_interval(market_gate)
                ):
                    _print_heartbeat(
                        daily_state,
                        market_gate,
                    )
                    print(
                        "[RUNNER] OBSERVATION ONLY: Execution Safety Gate "
                        "не разрешает order_send(); анализ продолжается."
                    )
                    last_heartbeat_at = now_fp()

            if not observation_only:
                entry_trigger = inspect_entry_trigger(SYMBOL)
                if entry_trigger.get("invalidated"):
                    print("[ENTRY WATCH] Условный план отменён закрытой свечой.")
                if entry_trigger.get("triggered"):
                    print()
                    print("=" * 80)
                    print("ENTRY WATCH -> SHORT ENTRY_CHECK")
                    print("=" * 80)
                    print(f"Closed bar: {entry_trigger.get('bar')}")
                    entry_result = run_entry_check(SYMBOL)
                    print(
                        "[ENTRY_CHECK] "
                        + ("завершён." if entry_result.get("ok") else f"fail closed: {entry_result.get('reason')}")
                    )
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # One intermediate decision halfway through the hour.  It
                # uses the current-day validated D1/H4/H1 map and cannot
                # consume/replace the ordinary H1 analysis state.
                m30_due = inspect_m30_decision_due(
                    symbol=SYMBOL,
                    market_gate=market_gate,
                )
                emit(
                    "scheduler",
                    "m30_decision_due",
                    data=m30_due,
                    repeat_key="m30_decision_due",
                    repeat_seconds=300,
                )
                if m30_due.get("due"):
                    m30_key = str(m30_due.get("m30_open_time_fp"))
                    mark_m30_decision_attempt(m30_key, status="STARTED")
                    try:
                        m30_result = run_m30_decision_cycle(
                            manage_connection=False,
                            analysis_only=False,
                        )
                    except Exception as m30_error:
                        mark_m30_decision_attempt(
                            m30_key,
                            status="FAILED",
                            error=f"{type(m30_error).__name__}: {m30_error}",
                        )
                        raise
                    if m30_result.get("ok"):
                        mark_m30_decision_attempt(
                            m30_key,
                            status="COMPLETED",
                        )
                    else:
                        mark_m30_decision_attempt(
                            m30_key,
                            status="FAILED",
                            error=str(
                                m30_result.get("error")
                                or m30_result.get("reason")
                            ),
                        )
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

            analysis_due, latest_h1 = _fresh_h1_analysis_due(
                market_gate,
                last_attempt_h1=last_analysis_attempt_h1,
                last_attempt_at=last_analysis_attempt_at,
            )
            if analysis_due:
                last_analysis_attempt_h1 = latest_h1
                last_analysis_attempt_at = now_fp()

                _run_full_cycle(
                    "FRESH_NEW_H1",
                    analysis_only=observation_only,
                )

            if (
                _seconds_since(
                    last_heartbeat_at
                )
                >= _heartbeat_interval(market_gate)
            ):
                _print_heartbeat(
                    daily_state,
                    market_gate,
                )
                last_heartbeat_at = now_fp()

            time.sleep(
                POLL_INTERVAL_SECONDS
            )

        except KeyboardInterrupt:
            raise

        except Exception as error:
            import traceback
            emit("scheduler", "loop_error", level="ERROR", data={"error": str(error),
                 "traceback": traceback.format_exc()})
            write_runner_status(
                connected=bool(connected),
                daily_state=None,
                status="error",
                last_error=f"{type(error).__name__}: {error}",
            )

            print()
            print("=" * 80)
            print("[RUNNER ERROR]")
            print("=" * 80)
            print(
                f"{type(error).__name__}: {error}"
            )
            print(
                "[FAIL CLOSED] Новый entry не выполняется в этом цикле."
            )
            print("=" * 80)

            # При ошибках MT5 лучше переподключиться чисто.
            try:
                mt5.shutdown()
            except Exception:
                pass

            connected = False

            time.sleep(
                RECONNECT_INTERVAL_SECONDS
            )

    # unreachable


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    log_path = install_console_log()
    if log_path is not None:
        print(f"[LOG] UTF-8 журнал: {log_path}")
    try:
        with SingleInstanceLock(
            LOCK_PATH
        ):
            try:
                run_forever()
            except KeyboardInterrupt:
                print()
                print("[RUNNER] Остановка по Ctrl+C.")

    except SingleInstanceError as error:
        print()
        print("=" * 80)
        print("[RUNNER BLOCKED]")
        print("=" * 80)
        print(str(error))
        print(
            "[SAFE MODE] Вторая копия робота не запущена."
        )
        print("=" * 80)

    finally:
        try:
            disconnect_mt5()
        except Exception:
            pass

        write_runner_status(
            connected=False,
            daily_state=None,
            status="stopped",
        )


if __name__ == "__main__":
    main()
