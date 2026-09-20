"""Stateful end-to-end trading lifecycle regression tests.

Unlike the legacy route tests, these do not replace the validator,
reconciliation or Trade State functions with successful mocks.  Only the
external MT5 terminal is simulated; production decision/risk/state/executor
code runs unchanged against a stateful broker model.
"""

from __future__ import annotations

import copy
import tempfile
import time
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import MetaTrader5 as mt5

import live_executor
import main as robot_main
import pending_executor
import position_protection
import risk_manager
import trade_state
from prop_time import now_fp
from trade_statistics import build_trade_statistics


class Record(SimpleNamespace):
    def _asdict(self):
        return dict(vars(self))


class StatefulMt5Broker:
    def __init__(self):
        self.balance = 10_000.0
        self.orders = {}
        self.positions = {}
        self.history_orders = []
        self.deals = []
        self.requests = []
        self.next_ticket = 700_000
        self.error = (0, "ok")
        self.reject_next = False
        self.unknown_next = False
        self.tick = Record(
            bid=4400.00,
            ask=4400.20,
            time=int(time.time()),
            time_msc=int(time.time() * 1000),
        )

    def _ticket(self):
        self.next_ticket += 1
        return self.next_ticket

    def account_info(self):
        floating = sum(float(item.profit) for item in self.positions.values())
        return Record(
            login=7911989,
            server="Stateful-Demo",
            trade_mode=mt5.ACCOUNT_TRADE_MODE_DEMO,
            trade_allowed=True,
            trade_expert=True,
            balance=self.balance,
            equity=self.balance + floating,
            margin=0.0,
            margin_free=self.balance + floating,
            margin_level=0.0,
            profit=floating,
            currency="USD",
            leverage=1000,
        )

    def terminal_info(self):
        return Record(
            connected=True,
            trade_allowed=True,
            tradeapi_disabled=False,
            build=9999,
        )

    def symbol_info(self, symbol):
        return Record(
            name=symbol,
            digits=2,
            point=0.01,
            trade_stops_level=10,
            trade_freeze_level=0,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            trade_exemode=getattr(mt5, "SYMBOL_TRADE_EXECUTION_MARKET", 2),
            filling_mode=(
                int(getattr(mt5, "SYMBOL_FILLING_FOK", 1))
                | int(getattr(mt5, "SYMBOL_FILLING_IOC", 2))
            ),
            trade_tick_size=0.01,
        )

    def symbol_info_tick(self, symbol):
        return self.tick

    def positions_get(self, *, symbol=None, ticket=None):
        values = list(self.positions.values())
        if symbol is not None:
            values = [item for item in values if item.symbol == symbol]
        if ticket is not None:
            values = [item for item in values if int(item.ticket) == int(ticket)]
        return tuple(values)

    def orders_get(self, *, symbol=None, ticket=None):
        values = list(self.orders.values())
        if symbol is not None:
            values = [item for item in values if item.symbol == symbol]
        if ticket is not None:
            values = [item for item in values if int(item.ticket) == int(ticket)]
        return tuple(values)

    def history_orders_get(self, *args, **kwargs):
        values = list(self.history_orders)
        ticket = kwargs.get("ticket")
        position = kwargs.get("position")
        if ticket is not None:
            values = [item for item in values if int(item.ticket) == int(ticket)]
        if position is not None:
            values = [
                item for item in values
                if int(getattr(item, "position_id", 0) or 0) == int(position)
            ]
        return tuple(values)

    def history_deals_get(self, *args, **kwargs):
        # Reproduce the real terminal compatibility failure seen in V8.4.
        # Production must recover with the UTC date-range overload.
        if kwargs.get("ticket") is not None:
            self.error = (-2, "Terminal: Invalid params")
            return None
        values = list(self.deals)
        position = kwargs.get("position")
        if position is not None:
            values = [
                item for item in values
                if int(getattr(item, "position_id", 0) or 0) == int(position)
            ]
        group = kwargs.get("group")
        if group:
            values = [item for item in values if item.symbol == group]
        self.error = (0, "ok")
        return tuple(values)

    def order_calc_profit(self, order_type, symbol, volume, price_open, price_close):
        direction = 1.0 if int(order_type) == int(mt5.ORDER_TYPE_BUY) else -1.0
        return direction * (float(price_close) - float(price_open)) * float(volume) * 100.0

    def order_calc_margin(self, order_type, symbol, volume, price):
        return abs(float(price) * float(volume) * 100.0 / 1000.0)

    def order_check(self, request):
        return Record(
            retcode=0,
            comment="Done",
            balance=self.balance,
            equity=self.balance,
            profit=0.0,
            margin=0.0,
            margin_free=self.balance,
            margin_level=0.0,
        )

    def last_error(self):
        return self.error

    def _new_position_from_request(self, request, *, order_ticket=None):
        order_ticket = int(order_ticket or self._ticket())
        position_ticket = self._ticket()
        deal_ticket = self._ticket()
        now = int(time.time())
        is_buy = int(request["type"]) in {
            int(mt5.ORDER_TYPE_BUY),
            int(mt5.ORDER_TYPE_BUY_LIMIT),
            int(mt5.ORDER_TYPE_BUY_STOP),
        }
        position = Record(
            ticket=position_ticket,
            identifier=position_ticket,
            time=now,
            symbol=request["symbol"],
            type=mt5.POSITION_TYPE_BUY if is_buy else mt5.POSITION_TYPE_SELL,
            volume=float(request["volume"]),
            price_open=float(request["price"]),
            price_current=float(request["price"]),
            sl=float(request["sl"]),
            tp=float(request["tp"]),
            magic=int(request["magic"]),
            comment=str(request["comment"]),
            profit=0.0,
            swap=0.0,
        )
        deal = Record(
            ticket=deal_ticket,
            order=order_ticket,
            position_id=position_ticket,
            time=now,
            time_msc=now * 1000,
            symbol=request["symbol"],
            type=mt5.DEAL_TYPE_BUY if is_buy else mt5.DEAL_TYPE_SELL,
            entry=mt5.DEAL_ENTRY_IN,
            reason=getattr(mt5, "DEAL_REASON_EXPERT", 0),
            volume=float(request["volume"]),
            price=float(request["price"]),
            profit=0.0,
            commission=-1.0,
            swap=0.0,
            fee=0.0,
            magic=int(request["magic"]),
            comment=str(request["comment"]),
        )
        self.positions[position_ticket] = position
        self.deals.append(deal)
        return position, deal, order_ticket

    def materialize_market_request(self, request):
        return self._new_position_from_request(copy.deepcopy(request))

    def order_send(self, request):
        request = copy.deepcopy(request)
        self.requests.append(request)
        if self.reject_next:
            self.reject_next = False
            return Record(
                retcode=getattr(mt5, "TRADE_RETCODE_INVALID", -999),
                comment="simulated reject",
                order=0,
                deal=0,
                volume=0.0,
                price=0.0,
            )
        if self.unknown_next:
            self.unknown_next = False
            self.error = (-10005, "IPC timeout")
            return None

        action = int(request["action"])
        if action == int(mt5.TRADE_ACTION_DEAL):
            position, deal, order_ticket = self._new_position_from_request(request)
            return Record(
                retcode=mt5.TRADE_RETCODE_DONE,
                comment="Done",
                order=order_ticket,
                deal=deal.ticket,
                volume=position.volume,
                price=position.price_open,
            )
        if action == int(mt5.TRADE_ACTION_PENDING):
            ticket = self._ticket()
            now = int(time.time())
            self.orders[ticket] = Record(
                ticket=ticket,
                time_setup=now,
                time_setup_msc=now * 1000,
                position_id=0,
                symbol=request["symbol"],
                type=int(request["type"]),
                state=getattr(mt5, "ORDER_STATE_PLACED", 1),
                volume_initial=float(request["volume"]),
                volume_current=float(request["volume"]),
                price_open=float(request["price"]),
                sl=float(request["sl"]),
                tp=float(request["tp"]),
                magic=int(request["magic"]),
                comment=str(request["comment"]),
            )
            return Record(
                retcode=mt5.TRADE_RETCODE_PLACED,
                comment="Placed",
                order=ticket,
                deal=0,
                volume=float(request["volume"]),
                price=float(request["price"]),
            )
        if action == int(mt5.TRADE_ACTION_REMOVE):
            ticket = int(request["order"])
            order = self.orders.pop(ticket)
            archived = copy.copy(order)
            archived.state = mt5.ORDER_STATE_CANCELED
            archived.time_done = int(time.time())
            self.history_orders.append(archived)
            return Record(
                retcode=mt5.TRADE_RETCODE_DONE,
                comment="Removed",
                order=ticket,
                deal=0,
                volume=0.0,
                price=0.0,
            )
        if action == int(mt5.TRADE_ACTION_SLTP):
            position = self.positions[int(request["position"])]
            position.sl = float(request["sl"])
            position.tp = float(request["tp"])
            return Record(
                retcode=mt5.TRADE_RETCODE_DONE,
                comment="Modified",
                order=0,
                deal=0,
                volume=position.volume,
                price=position.price_current,
            )
        raise AssertionError(f"Unsupported simulated request: {request}")

    def fill_pending(self, ticket):
        order = self.orders.pop(int(ticket))
        archived = copy.copy(order)
        archived.state = mt5.ORDER_STATE_FILLED
        request = {
            "symbol": order.symbol,
            "type": order.type,
            "volume": order.volume_initial,
            "price": order.price_open,
            "sl": order.sl,
            "tp": order.tp,
            "magic": order.magic,
            "comment": order.comment,
        }
        position, deal, _ = self._new_position_from_request(
            request,
            order_ticket=int(ticket),
        )
        archived.position_id = position.identifier
        self.history_orders.append(archived)
        return position

    def close_position(self, ticket, *, result, reason):
        position = self.positions.pop(int(ticket))
        now = int(time.time())
        is_buy = int(position.type) == int(mt5.POSITION_TYPE_BUY)
        close_price = position.tp if result > 0 else position.sl
        deal = Record(
            ticket=self._ticket(),
            order=self._ticket(),
            position_id=position.identifier,
            time=now,
            time_msc=now * 1000,
            symbol=position.symbol,
            type=mt5.DEAL_TYPE_SELL if is_buy else mt5.DEAL_TYPE_BUY,
            entry=mt5.DEAL_ENTRY_OUT,
            reason=reason,
            volume=position.volume,
            price=float(close_price),
            profit=float(result),
            commission=-1.0,
            swap=0.0,
            fee=0.0,
            magic=position.magic,
            comment=position.comment,
        )
        self.deals.append(deal)
        self.balance += float(result) - 1.0
        return deal


class TradeLifecycleV852Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        state_dir = self.root / "state"
        state_dir.mkdir()

        self.stack.enter_context(patch.object(trade_state, "STATE_DIR", state_dir))
        self.stack.enter_context(
            patch.object(trade_state, "TRADE_STATE_PATH", state_dir / "trade_state.json")
        )
        self.stack.enter_context(patch.object(risk_manager, "STATE_DIR", state_dir))
        self.stack.enter_context(
            patch.object(risk_manager, "RISK_STATE_PATH", state_dir / "risk_state.json")
        )
        self.stack.enter_context(
            patch.object(
                position_protection,
                "STATE_PATH",
                state_dir / "position_protection_state.json",
            )
        )
        self.stack.enter_context(patch.object(live_executor.time, "sleep", return_value=None))
        self.stack.enter_context(patch.object(pending_executor.time, "sleep", return_value=None))

        self.broker = StatefulMt5Broker()
        for name in (
            "account_info",
            "terminal_info",
            "symbol_info",
            "symbol_info_tick",
            "positions_get",
            "orders_get",
            "history_orders_get",
            "history_deals_get",
            "order_calc_profit",
            "order_calc_margin",
            "order_check",
            "order_send",
            "last_error",
        ):
            self.stack.enter_context(
                patch.object(mt5, name, new=getattr(self.broker, name))
            )
        trade_state.save_trade_state(trade_state.create_empty_state())

    def snapshot(self):
        closed = now_fp().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        return {
            "instrument": "XAUUSD",
            "generated_at_fp": now_fp().isoformat(),
            "timeframes": {
                "H1": {"closed_bars": [{"time_fp": closed.isoformat()}]},
            },
        }

    def analysis(self, order_type="market", action="enter_long"):
        is_long = action == "enter_long"
        if order_type == "market":
            entry = self.broker.tick.ask if is_long else self.broker.tick.bid
        elif order_type == "limit":
            entry = 4395.0 if is_long else 4405.0
        else:
            entry = 4405.0 if is_long else 4395.0
        stop = entry - 10.0 if is_long else entry + 10.0
        target = entry + 20.0 if is_long else entry - 20.0
        return {
            "timestamp": now_fp().isoformat(),
            "instrument": "XAUUSD",
            "data_quality": {"sufficient": True, "issues": "none"},
            "market_regime": {
                "primary_regime": "trend",
                "direction": "up" if is_long else "down",
                "current_phase": "impulse",
                "phase_status": "developing",
            },
            "wave_count": {
                "structure_type": "impulse",
                "direction": "up" if is_long else "down",
                "current_label": "3",
                "summary": "stateful lifecycle test",
                "invalidation_level": stop,
            },
            "scenario_map": {"primary_scenario": "test", "next_opportunity": "test"},
            "recommendation": {
                "action": action,
                "order_type": order_type,
                "confidence": "high",
                "setup_type": "elliott_continuation",
                "trade_horizon": "intraday",
                "setup_quality": "good",
                "entry_quality": "good",
                "entry_price": entry,
                "stop_loss": stop,
                "take_profit": target,
                "invalidation_level": stop,
                "why_now": "stateful test",
                "structural_stop_basis": "previous completed wave",
                "target_basis": "fibonacci",
                "reasoning": "stateful test",
                "invalidation_reason": "previous wave broken",
            },
        }

    def approve_and_register(self, order_type="market", action="enter_long"):
        snapshot = self.snapshot()
        analysis = self.analysis(order_type, action)
        risk = risk_manager.evaluate_trade(analysis, "XAUUSD")
        self.assertEqual(risk["decision"], "APPROVED", risk.get("reasons"))
        result = trade_state.register_trade_decision(analysis, risk, snapshot)
        self.assertEqual(result["state_action"], "CREATED_NEW_PLAN")
        return snapshot, analysis, risk

    def test_market_open_duplicate_guard_broker_close_and_statistics(self):
        snapshot, _, _ = self.approve_and_register("market", "enter_long")
        report = live_executor.execute_active_plan(snapshot, "XAUUSD")
        self.assertEqual(report["decision"], "ORDER_SEND_SUCCESS", report)
        self.assertEqual(len(self.broker.requests), 1)
        managed = trade_state.get_managed_positions()
        self.assertEqual(len(managed), 1)

        # Re-running the executor after state attachment cannot duplicate entry.
        second = live_executor.execute_active_plan(snapshot, "XAUUSD")
        self.assertEqual(second["decision"], "NO_ACTIVE_PLAN")
        self.assertIsInstance(second["validation_report"], dict)
        self.assertEqual(len(self.broker.requests), 1)

        ticket = managed[0]["position_ticket"]
        self.broker.close_position(
            ticket,
            result=150.0,
            reason=mt5.DEAL_REASON_TP,
        )
        gate = robot_main.inspect_position_gate("XAUUSD")
        self.assertTrue(gate["position_just_closed"])
        self.assertEqual(gate["closed_positions"][0]["close_reason"], "TAKE_PROFIT")
        self.assertEqual(trade_state.get_managed_positions(), [])

        stats = build_trade_statistics(trade_state.load_trade_state())
        self.assertEqual(stats["closed_trades"], 1)
        self.assertEqual(stats["wins"], 1)
        self.assertAlmostEqual(stats["net_result"], 148.0)

    def test_limit_pending_survives_restart_fills_and_closes(self):
        snapshot, _, _ = self.approve_and_register("limit", "enter_long")
        placed = live_executor.execute_active_plan(snapshot, "XAUUSD")
        self.assertEqual(placed["decision"], "PENDING_ORDER_PLACED", placed)
        self.assertEqual(len(self.broker.requests), 1)
        pending_ticket = trade_state.get_active_plan()["pending_ticket"]

        restarted = pending_executor.reconcile_active_pending_execution("XAUUSD")
        self.assertEqual(restarted["decision"], "PENDING_ACTIVE")
        self.assertEqual(len(self.broker.requests), 1)

        position = self.broker.fill_pending(pending_ticket)
        filled = pending_executor.reconcile_active_pending_execution("XAUUSD")
        self.assertEqual(filled["decision"], "PENDING_FILLED_POSITION_RECOVERED")
        self.assertEqual(trade_state.get_managed_positions()[0]["position_ticket"], position.ticket)

        self.broker.close_position(
            position.ticket,
            result=-100.0,
            reason=mt5.DEAL_REASON_SL,
        )
        gate = robot_main.inspect_position_gate("XAUUSD")
        self.assertEqual(gate["closed_positions"][0]["close_reason"], "STOP_LOSS")
        stats = build_trade_statistics(trade_state.load_trade_state())
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["lifecycle_counts"]["pending_ticket_issued"], 1)

    def test_pending_ttl_is_exactly_three_h1_bars(self):
        source = now_fp().replace(minute=0, second=0, microsecond=0)
        plan = {"source_h1_closed_bar_time": source.isoformat()}
        for offset in (0, 1, 2):
            validity = pending_executor.inspect_pending_h1_validity(
                plan,
                (source + timedelta(hours=offset)).isoformat(),
            )
            self.assertTrue(validity["valid"], validity)
        expired = pending_executor.inspect_pending_h1_validity(
            plan,
            (source + timedelta(hours=3)).isoformat(),
        )
        self.assertFalse(expired["valid"], expired)
        self.assertEqual(expired["age_h1_bars"], 3)

    def test_unsent_pending_can_retry_on_next_h1_within_ttl(self):
        original_snapshot, _, _ = self.approve_and_register(
            "limit", "enter_long"
        )
        plan = trade_state.get_active_plan()
        source = datetime.fromisoformat(plan["source_h1_closed_bar_time"])
        retry_snapshot = copy.deepcopy(original_snapshot)
        retry_snapshot["timeframes"]["H1"]["closed_bars"][-1]["time_fp"] = (
            source + timedelta(hours=1)
        ).isoformat()

        placed = live_executor.execute_active_plan(retry_snapshot, "XAUUSD")
        self.assertEqual(placed["decision"], "PENDING_ORDER_PLACED", placed)
        self.assertEqual(len(self.broker.requests), 1)
        self.assertIsNotNone(trade_state.get_active_plan()["pending_ticket"])

    def test_stop_pending_is_cancelled_once_and_archived(self):
        snapshot, _, _ = self.approve_and_register("stop", "enter_short")
        placed = live_executor.execute_active_plan(snapshot, "XAUUSD")
        self.assertEqual(placed["decision"], "PENDING_ORDER_PLACED", placed)
        plan = trade_state.get_active_plan()
        cancelled = pending_executor.execute_pending_cancel(
            plan=plan,
            reason="stateful expiry test",
        )
        self.assertEqual(cancelled["decision"], "PENDING_CANCELLED")
        remove_requests = [
            item for item in self.broker.requests
            if int(item["action"]) == int(mt5.TRADE_ACTION_REMOVE)
        ]
        self.assertEqual(len(remove_requests), 1)
        self.assertIsNone(trade_state.get_active_plan())
        again = pending_executor.reconcile_active_pending_execution("XAUUSD")
        self.assertEqual(again["decision"], "NO_ACTIVE_PLAN")
        self.assertEqual(len(remove_requests), 1)
        stats = build_trade_statistics(trade_state.load_trade_state())
        self.assertEqual(stats["lifecycle_counts"]["cancelled"], 1)

    def test_rejected_pending_does_not_leave_stuck_plan(self):
        snapshot, _, _ = self.approve_and_register("limit", "enter_long")
        self.broker.reject_next = True
        result = live_executor.execute_active_plan(snapshot, "XAUUSD")
        self.assertEqual(result["decision"], "PENDING_ORDER_SEND_REJECTED", result)
        self.assertIsNone(trade_state.get_active_plan())
        stats = build_trade_statistics(trade_state.load_trade_state())
        self.assertEqual(stats["lifecycle_counts"]["execution_failed"], 1)

    def test_unknown_market_result_is_reconciled_after_restart_without_resend(self):
        snapshot, _, _ = self.approve_and_register("market", "enter_long")
        self.broker.unknown_next = True
        result = live_executor.execute_active_plan(snapshot, "XAUUSD")
        self.assertEqual(result["decision"], "ORDER_SEND_STATE_UNKNOWN", result)
        self.assertEqual(len(self.broker.requests), 1)
        plan = trade_state.get_active_plan()
        self.assertEqual(plan["execution_status"], trade_state.EXECUTION_SEND_INTENT)

        self.broker.materialize_market_request(plan["send_request"])
        recovered = live_executor.reconcile_active_send_intent("XAUUSD")
        self.assertTrue(recovered["resolved"], recovered)
        self.assertEqual(len(self.broker.requests), 1)
        self.assertEqual(len(trade_state.get_managed_positions()), 1)

    def test_open_position_stop_is_moved_once_and_reconciled(self):
        snapshot, _, _ = self.approve_and_register("market", "enter_long")
        opened = live_executor.execute_active_plan(snapshot, "XAUUSD")
        self.assertEqual(opened["decision"], "ORDER_SEND_SUCCESS", opened)
        managed = trade_state.get_managed_positions()[0]
        ticket = int(managed["position_ticket"])
        current_position = self.broker.positions[ticket]

        bars = [
            {
                "time_fp": "2026-09-17T09:00:00+03:00",
                "open": 4398.0,
                "high": 4400.0,
                "low": 4395.0,
                "close": 4399.0,
            },
            {
                "time_fp": "2026-09-17T09:15:00+03:00",
                "open": 4399.0,
                "high": 4406.0,
                "low": 4397.0,
                "close": 4404.0,
            },
        ]
        protection_snapshot = {
            "instrument": "XAUUSD",
            "symbol_info": {
                "digits": 2,
                "point": 0.01,
                "trade_tick_size": 0.01,
                "trade_stops_level": 10,
                "trade_freeze_level": 0,
            },
            "tick": {"bid": self.broker.tick.bid, "ask": self.broker.tick.ask},
            "timeframes": {"M15": {"closed_bars": bars}},
        }
        review = {
            "position_ticket": str(ticket),
            "review_trigger": "m15_event",
            "confidence": "high",
            "position_status": "healthy",
            "advisory_action": "hold",
            "data_quality": {"sufficient": True, "issues": "none"},
            "position_management": {
                "action": "tighten_stop",
                "structure_confirmed": True,
                "position_direction": "bullish",
                "current_m15_wave": "3",
                "stop_reference_wave": "1",
                "stop_wave_start_time": bars[0]["time_fp"],
                "stop_wave_end_time": bars[1]["time_fp"],
                "stop_wave_status": "completed",
                "stop_anchor_time": bars[0]["time_fp"],
                "stop_anchor_price": 4395.0,
                "stop_anchor_kind": "low",
                "management_reason": "confirmed previous wave",
            },
        }
        context = {
            "live_positions": [
                {
                    "position_ticket": ticket,
                    "stop_loss": float(current_position.sl),
                    "take_profit": float(current_position.tp),
                    "errors": [],
                }
            ]
        }
        with patch.object(
            position_protection,
            "get_current_tick",
            return_value={
                "bid": self.broker.tick.bid,
                "ask": self.broker.tick.ask,
                "time_fp": now_fp().isoformat(),
            },
        ):
            result = position_protection.execute_position_protection(
                review=review,
                snapshot=protection_snapshot,
                position_context=context,
                config={
                    "automatic_trade_changes": True,
                    "minimum_confidence": "high",
                },
                event_key=f"{ticket}|m15|stop",
            )
        self.assertEqual(result["status"], "protection_modified", result)
        self.assertEqual(float(self.broker.positions[ticket].sl), 4394.99)
        s_l_t_p_requests = [
            item for item in self.broker.requests
            if int(item["action"]) == int(mt5.TRADE_ACTION_SLTP)
        ]
        self.assertEqual(len(s_l_t_p_requests), 1)
        saved = trade_state.get_managed_positions()[0]
        self.assertEqual(float(saved["stop_loss"]), 4394.99)


class DailyBaselineRecoveryTests(unittest.TestCase):
    def test_missed_midnight_is_reconstructed_from_booked_deals(self):
        current = now_fp().replace(hour=12, minute=0, second=0, microsecond=0)
        account = Record(balance=10_050.0, equity=10_050.0)
        deals = (
            Record(
                ticket=1,
                time=int(current.timestamp()),
                time_msc=int(current.timestamp() * 1000),
                type=getattr(mt5, "DEAL_TYPE_BALANCE", 99999),
                entry=-1,
                position_id=0,
                profit=50.0,
                commission=0.0,
                swap=0.0,
                fee=0.0,
            ),
        )
        with patch.object(risk_manager.mt5, "history_deals_get", return_value=deals):
            result = risk_manager.reconstruct_daily_state_from_history(
                account, (), current, risk_manager.fp_day_start(current)
            )
        self.assertTrue(result["trusted"])
        self.assertEqual(result["daily_baseline"], 10_000.0)

    def test_cross_midnight_position_fails_closed(self):
        current = now_fp().replace(hour=12, minute=0, second=0, microsecond=0)
        start = risk_manager.fp_day_start(current)
        position = Record(ticket=7, time=int((start - timedelta(hours=2)).timestamp()))
        result = risk_manager.reconstruct_daily_state_from_history(
            Record(balance=10_000.0, equity=10_100.0),
            (position,),
            current,
            start,
        )
        self.assertFalse(result["trusted"])
        self.assertIn("перенесена через", result["reasons"][0])


if __name__ == "__main__":
    unittest.main()
