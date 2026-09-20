import copy
import json
import re
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


BASE_DIR = Path(__file__).resolve().parent


def _install_runtime_stubs():
    mt5 = types.ModuleType("MetaTrader5")
    names = set()
    for source_path in BASE_DIR.glob("*.py"):
        names.update(
            re.findall(
                r"mt5\.([A-Z][A-Z0-9_]+)",
                source_path.read_text(encoding="utf-8"),
            )
        )
    for index, name in enumerate(sorted(names), 1):
        setattr(mt5, name, index)
    sys.modules.setdefault("MetaTrader5", mt5)

    if "anthropic" not in sys.modules:
        anthropic = types.ModuleType("anthropic")

        class Anthropic:
            pass

        class AnthropicError(Exception):
            pass

        anthropic.Anthropic = Anthropic
        for name in (
            "AuthenticationError",
            "PermissionDeniedError",
            "RateLimitError",
            "APITimeoutError",
            "APIConnectionError",
            "APIStatusError",
        ):
            setattr(anthropic, name, type(name, (AnthropicError,), {}))
        sys.modules["anthropic"] = anthropic


_install_runtime_stubs()

import claude_client
import claude_request_guard
import claude_staged_client as staged
import main
from claude_client import ClaudeRequestOutcomeUnknownError


def _bar(start: datetime, index: int, minutes: int) -> dict:
    opened = start + timedelta(minutes=index * minutes)
    price = 3000.0 + index
    return {
        "time": opened.isoformat(),
        "open": price,
        "high": price + 2.0,
        "low": price - 2.0,
        "close": price + 1.0,
        "tick_volume": 1000 + index,
        "spread_points": 30,
        "real_volume": 0,
    }


def _payload() -> dict:
    tz = timezone(timedelta(hours=3))
    start = datetime(2026, 8, 1, tzinfo=tz)
    specs = {
        "D1": (20, 1440),
        "H4": (60, 240),
        "H1": (200, 60),
        "M30": (180, 30),
        "M15": (150, 15),
        "M5": (144, 5),
    }
    history = {}
    live = {}
    for timeframe, (count, minutes) in specs.items():
        bars = [_bar(start, index, minutes) for index in range(count)]
        history[timeframe] = {
            "closed_bars_count": len(bars),
            "closed_bars": bars,
        }
        live_bars = [
            _bar(start + timedelta(days=20), index, minutes)
            for index in range(2)
        ]
        current = _bar(start + timedelta(days=21), 0, minutes)
        current["is_closed"] = False
        live[timeframe] = {
            "closed_bars_since_day_start_count": len(live_bars),
            "closed_bars_since_day_start": live_bars,
            "current_unclosed_bar": current,
        }

    return {
        "instrument": "XAUUSD",
        "timestamp": "2026-08-17T12:05:00+03:00",
        "timezone": "FundingPips Platform Time UTC+3",
        "analysis_policy": {"independent_full_reanalysis_when_full_runs": True},
        "cache_partition": {"base_cutoff_fp": "2026-08-17T00:00:00+03:00"},
        "cacheable_history": {
            "data_source": {"price_feed": "MetaTrader 5 broker feed"},
            "symbol_specification": {"point": 0.01},
            "closed_market_history_before_day_start": history,
        },
        "live_market": {
            "generated_at_fp": "2026-08-17T12:05:00+03:00",
            "current_price": {
                "bid": 3200.0,
                "ask": 3200.3,
                "spread_price": 0.3,
                "spread_points": 30,
            },
            "raw_timeframes_since_day_start": live,
        },
        "data_capabilities": {
            "raw_ohlc_supplied": True,
            "python_calculated_indicators_supplied": False,
        },
    }


def _visualization(wave_points=None) -> dict:
    return {
        "wave_points": list(wave_points or []),
        "levels": [],
        "zones": [],
        "scenario_paths": [],
        "trendlines": [],
        "channels": [],
        "pattern_shapes": [],
        "market_events": [],
        "projected_waves": [],
        "wave_structures": [],
        "chart_comment": "",
    }


def _market_map(payload: dict, revision: dict) -> dict:
    return {
        "timestamp": payload["timestamp"],
        "instrument": "XAUUSD",
        "market_regime": {
            "primary_regime": "trend",
            "direction": "bullish",
            "current_phase": "H1 pullback inside bullish trend",
            "phase_status": "developing",
            "maturity": "middle",
            "location": "above structural support",
            "summary": "Bullish higher-timeframe structure with a pullback.",
        },
        "timeframe_analysis": {
            "D1": "Bullish swing structure.",
            "H4": "Corrective pullback.",
            "H1": "Pullback is developing.",
            "relationship": "D1/H4/H1 are nested consistently.",
            "summary": "Trend context remains valid.",
        },
        "price_structure": {
            "structure_state": "bullish",
            "swing_structure": "HH/HL",
            "key_levels": "Support below current price.",
            "liquidity_context": "No confirmed sweep.",
            "summary": "Structure is intact.",
        },
        "patterns": "No standalone pattern overrides structure.",
        "wave_count": {
            "structure_type": "impulse",
            "direction": "bullish",
            "current_label": "2",
            "current_phase": "developing correction",
            "invalidation_level": 2998.0,
            "alternate_count": "Range alternative below support.",
            "summary": "Primary count expects a later wave 3.",
        },
        "higher_timeframe_context": {
            "d1_trend": "bullish",
            "d1_wave_context": "impulsive",
            "h4_trend": "bullish pullback",
            "h4_wave_context": "corrective",
            "alignment": "aligned",
            "summary": "Higher timeframes support the primary map.",
        },
        "scenario_map": {
            "primary_scenario": "Pullback holds.",
            "alternate_scenario": "Support fails.",
            "expected_path": "Base then continuation.",
            "current_opportunity": "Wait for confirmation.",
            "next_opportunity": "Potential wave 3 continuation.",
            "regime_change_trigger": "H1 close below support.",
        },
        "visualization": _visualization(),
        "data_quality": {"sufficient": True, "issues": "none"},
        "wave_revision": revision,
    }


def _decision(payload: dict, wave_points=None) -> dict:
    return {
        "timestamp": payload["timestamp"],
        "instrument": "XAUUSD",
        "h1_execution_context": "H1 confirmation is not complete.",
        "microstructure_and_patterns": "M15/M5 do not confirm an entry.",
        "multi_timeframe_relationship": "Microstructure is not ready.",
        "visualization": _visualization(wave_points),
        "recommendation": {
            "action": "stay_out",
            "setup_type": "no_trade",
            "trade_horizon": "unclear",
            "setup_quality": "weak",
            "entry_quality": "poor",
            "order_type": "none",
            "entry_price": None,
            "stop_loss": None,
            "take_profit": None,
            "invalidation_level": None,
            "confidence": "low",
            "why_now": "No confirmed trigger.",
            "structural_stop_basis": "Not applicable.",
            "target_basis": "Not applicable.",
            "reasoning": "Wait for a valid trigger without forcing a trade.",
            "invalidation_reason": "No active trade idea.",
            "fvg_role": "no_relevant_fvg",
            "fvg_ids": "",
            "fvg_basis": "No relevant FVG affects this decision.",
        },
        "data_quality": {"sufficient": True, "issues": "none"},
    }


def _wire_cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _wire_row(value: dict, columns: tuple[str, ...]) -> list[str]:
    return [_wire_cell(value.get(name)) for name in columns]


def _wire_visualization(value: dict) -> dict:
    return {
        "wave_points": [
            _wire_row(item, staged.WAVE_POINT_COLUMNS)
            for item in value["wave_points"]
        ],
        "levels": [
            _wire_row(item, staged.LEVEL_COLUMNS) for item in value["levels"]
        ],
        "zones": [
            _wire_row(item, staged.ZONE_COLUMNS) for item in value["zones"]
        ],
        "scenario_paths": [
            _wire_row(item, staged.SCENARIO_PATH_COLUMNS)
            for item in value["scenario_paths"]
        ],
        "trendlines": [_wire_row(item, staged.TRENDLINE_COLUMNS) for item in value.get("trendlines", [])],
        "channels": [_wire_row(item, staged.CHANNEL_COLUMNS) for item in value.get("channels", [])],
        "pattern_shapes": [_wire_row(item, staged.PATTERN_SHAPE_COLUMNS) for item in value.get("pattern_shapes", [])],
        "market_events": [_wire_row(item, staged.MARKET_EVENT_COLUMNS) for item in value.get("market_events", [])],
        "projected_waves": [_wire_row(item, staged.PROJECTED_WAVE_COLUMNS) for item in value.get("projected_waves", [])],
        "wave_structures": [_wire_row(item, staged.WAVE_STRUCTURE_COLUMNS) for item in value.get("wave_structures", [])],
        "chart_comment": value["chart_comment"],
    }


def _wire_market_map(value: dict) -> dict:
    result = copy.deepcopy(value)
    for name, columns in (
        ("market_regime", staged.MARKET_REGIME_COLUMNS),
        ("timeframe_analysis", staged.TIMEFRAME_ANALYSIS_COLUMNS),
        ("price_structure", staged.PRICE_STRUCTURE_COLUMNS),
        ("wave_count", staged.WAVE_COUNT_COLUMNS),
        (
            "higher_timeframe_context",
            staged.HIGHER_TIMEFRAME_CONTEXT_COLUMNS,
        ),
        ("scenario_map", staged.SCENARIO_MAP_COLUMNS),
    ):
        result[name] = _wire_row(value[name], columns)
    result["data_quality"] = {
        "sufficient": _wire_cell(value["data_quality"]["sufficient"]),
        "issues": value["data_quality"]["issues"],
    }
    result["visualization"] = _wire_visualization(value["visualization"])
    return result


def _wire_decision(value: dict) -> dict:
    result = copy.deepcopy(value)
    result["recommendation"] = _wire_row(
        value["recommendation"], staged.RECOMMENDATION_COLUMNS
    )
    result["data_quality"] = {
        "sufficient": _wire_cell(value["data_quality"]["sufficient"]),
        "issues": value["data_quality"]["issues"],
    }
    result["visualization"] = _wire_visualization(value["visualization"])
    return result


class StagedPayloadTests(unittest.TestCase):
    def test_paid_map_extra_scenario_cell_is_recovered_locally(self):
        payload = _payload()
        market_map = _market_map(
            payload,
            {"mode": "initialize", "preserved_anchor_ids": [],
             "invalidated_anchor_ids": [], "reason": "Initial map."},
        )
        wire = _wire_market_map(market_map)
        wire["scenario_map"].append("duplicate trailing explanation")
        expanded = staged._expand_market_map_wire_result(wire)
        self.assertEqual(expanded["scenario_map"], market_map["scenario_map"])
        self.assertTrue(staged.get_last_wire_normalization_warnings())

    def test_paid_decision_extra_recommendation_cell_is_recovered_locally(self):
        payload = _payload()
        decision = _decision(payload)
        wire = _wire_decision(decision)
        wire["recommendation"].append("duplicate trailing explanation")
        expanded = staged._expand_trade_decision_wire_result(wire)
        self.assertEqual(expanded["recommendation"], decision["recommendation"])

    def test_visual_channel_missing_only_basis_does_not_destroy_full(self):
        payload = _payload()
        decision = _decision(payload)
        wire = _wire_decision(decision)
        wire["visualization"]["channels"] = [[
            "c1", "parallel", "primary", "H1",
            "2026-08-17T01:00:00+03:00", "3100",
            "2026-08-17T02:00:00+03:00", "3110",
            "2026-08-17T01:00:00+03:00", "3080",
            "2026-08-17T02:00:00+03:00", "3090",
            "active", "", "", "", "", "Channel",
        ]]
        expanded = staged._expand_trade_decision_wire_result(wire)
        self.assertEqual(len(expanded["visualization"]["channels"]), 1)
        self.assertEqual(expanded["visualization"]["channels"][0]["basis"], "")

    def test_stage_schemas_are_strict_closed_objects(self):
        def inspect(schema):
            if isinstance(schema, dict):
                if schema.get("type") == "object":
                    properties = schema.get("properties", {})
                    self.assertFalse(schema.get("additionalProperties", True))
                    self.assertEqual(
                        set(properties),
                        set(schema.get("required", [])),
                    )
                    self.assertEqual(
                        len(schema.get("required", [])),
                        len(set(schema.get("required", []))),
                    )
                for value in schema.values():
                    inspect(value)
            elif isinstance(schema, list):
                for value in schema:
                    inspect(value)

        inspect(staged.MARKET_MAP_SCHEMA)
        inspect(staged.TRADE_DECISION_SCHEMA)
        inspect(staged.MARKET_MAP_WIRE_SCHEMA)
        inspect(staged.TRADE_DECISION_WIRE_SCHEMA)

    def test_wire_schemas_require_named_cells_without_optional_union_branches(self):
        for schema in (staged.MARKET_MAP_WIRE_SCHEMA, staged.TRADE_DECISION_WIRE_SCHEMA):
            def visit(node):
                if not isinstance(node, dict):
                    return
                self.assertNotIn("anyOf", node)
                self.assertNotIn("oneOf", node)
                if node.get("type") == "object":
                    self.assertEqual(set(node["properties"]), set(node["required"]))
                for value in node.values():
                    if isinstance(value, dict):
                        visit(value)
            visit(schema)
        self.assertEqual(staged.MARKET_MAP_WIRE_SCHEMA["properties"]["price_structure"]["type"], "object")

    def test_compact_wire_roundtrip_preserves_canonical_contract(self):
        payload = _payload()
        visualization = {
            "wave_points": [
                {
                    "scenario": "primary",
                    "degree": "minor",
                    "timeframe": "H1",
                    "sequence": 3,
                    "label": "3",
                    "time": "2026-08-17T10:00:00+03:00",
                    "price": 3202.0,
                    "status": "developing",
                    "structure_id": "primary_h1_3",
                    "parent_structure_id": "primary_h4_c",
                    "parent_wave_id": "C",
                    "wave_type": "impulse",
                }
            ],
            "levels": [
                {
                    "kind": "support",
                    "scenario": "primary",
                    "timeframe": "H1",
                    "price": 3190.5,
                    "label": "H1 support",
                    "basis": "Confirmed swing low.",
                }
            ],
            "zones": [
                {
                    "kind": "demand",
                    "scenario": "primary",
                    "timeframe": "H1",
                    "start_time": "2026-08-16T10:00:00+03:00",
                    "end_time": "2026-08-17T10:00:00+03:00",
                    "price_low": 3188.0,
                    "price_high": 3193.0,
                    "label": "Demand",
                }
            ],
            "scenario_paths": [
                {
                    "scenario": "primary",
                    "timeframe": "H1",
                    "anchor_time": "2026-08-17T10:00:00+03:00",
                    "anchor_price": 3202.0,
                    "direction": "up",
                    "target_price_low": 3220.0,
                    "target_price_high": 3230.0,
                    "label": "Wave 3 continuation",
                }
            ],
            "trendlines": [],
            "channels": [],
            "pattern_shapes": [],
            "market_events": [],
            "projected_waves": [],
            "wave_structures": [],
            "chart_comment": "Exact coordinates are transported losslessly.",
        }
        market_map = _market_map(
            payload,
            {
                "mode": "initialize",
                "preserved_anchor_ids": [],
                "invalidated_anchor_ids": [],
                "reason": "No previous anchors.",
            },
        )
        market_map["visualization"] = copy.deepcopy(visualization)
        decision = _decision(payload)
        decision["visualization"] = copy.deepcopy(visualization)

        self.assertEqual(
            staged._expand_market_map_wire_result(_wire_market_map(market_map)),
            market_map,
        )
        self.assertEqual(
            staged._expand_trade_decision_wire_result(
                _wire_decision(decision)
            ),
            decision,
        )

    def test_data_quality_wire_is_strict_object(self):
        for schema in (
            staged.MARKET_MAP_WIRE_SCHEMA,
            staged.TRADE_DECISION_WIRE_SCHEMA,
        ):
            quality = schema["properties"]["data_quality"]
            self.assertEqual(quality["type"], "object")
            self.assertEqual(
                set(quality["required"]), {"sufficient", "issues"}
            )
            self.assertFalse(quality["additionalProperties"])

    def test_position_management_wire_requires_complete_raw_anchors(self):
        wire = {
            "action": "tighten_stop_and_recalculate_target",
            "structure_confirmed": "true",
            "position_direction": "bullish",
            "current_m15_wave": "3",
            "stop_reference_wave": "1",
            "stop_wave_start_time": "2026-09-10T09:00:00+03:00",
            "stop_wave_end_time": "2026-09-10T09:15:00+03:00",
            "stop_wave_status": "completed",
            "stop_anchor_time": "2026-09-10T09:00:00+03:00",
            "stop_anchor_price": "4400.5",
            "stop_anchor_kind": "low",
            "fib_method": "wave3_extension",
            "fib_timeframe": "M15",
            "fib_ratio": "1.618",
            "fib_leg_start_time": "2026-09-10T08:00:00+03:00",
            "fib_leg_start_price": "4400.5",
            "fib_leg_start_kind": "low",
            "fib_leg_end_time": "2026-09-10T08:45:00+03:00",
            "fib_leg_end_price": "4420.5",
            "fib_leg_end_kind": "high",
            "fib_projection_time": "2026-09-10T09:00:00+03:00",
            "fib_projection_price": "4408.0",
            "fib_projection_kind": "low",
            "management_reason": "EN: Confirmed.\nRU: Подтверждено.",
        }
        expanded = staged._expand_position_management_wire(wire)
        self.assertTrue(expanded["structure_confirmed"])
        self.assertEqual(expanded["stop_anchor_price"], 4400.5)
        self.assertEqual(expanded["fib_ratio"], 1.618)

        invalid = copy.deepcopy(wire)
        invalid["stop_anchor_time"] = ""
        with self.assertRaisesRegex(ValueError, "полный M15 anchor"):
            staged._expand_position_management_wire(invalid)

    def test_legacy_multi_issue_data_quality_is_recovered_losslessly(self):
        payload = _payload()
        market_map = _market_map(
            payload,
            {
                "mode": "initialize",
                "preserved_anchor_ids": [],
                "invalidated_anchor_ids": [],
                "reason": "No previous anchors.",
            },
        )
        legacy = _wire_market_map(market_map)
        legacy["data_quality"] = [
            "true",
            "First saved issue.",
            "Second saved issue.",
            "Current bars are unclosed.",
        ]
        expanded = staged._expand_market_map_wire_result(legacy)
        self.assertTrue(expanded["data_quality"]["sufficient"])
        self.assertEqual(
            expanded["data_quality"]["issues"],
            "First saved issue.; Second saved issue.; "
            "Current bars are unclosed.",
        )

    def test_other_legacy_rows_remain_exact_length_fail_closed(self):
        payload = _payload()
        market_map = _market_map(
            payload,
            {
                "mode": "initialize",
                "preserved_anchor_ids": [],
                "invalidated_anchor_ids": [],
                "reason": "No previous anchors.",
            },
        )
        legacy = _wire_market_map(market_map)
        legacy["market_regime"].pop()
        with self.assertRaisesRegex(ValueError, "ожидалось 7 ячеек"):
            staged._expand_market_map_wire_result(legacy)

    def test_paid_stages_send_wire_schema_and_return_canonical_objects(self):
        payload = _payload()
        market_map = _market_map(
            payload,
            {
                "mode": "initialize",
                "preserved_anchor_ids": [],
                "invalidated_anchor_ids": [],
                "reason": "No previous anchors.",
            },
        )
        decision = _decision(payload)

        with patch.object(
            staged,
            "_request_structured_stage",
            return_value=_wire_market_map(market_map),
        ) as request:
            self.assertEqual(staged.analyze_market_map(payload), market_map)
            self.assertIs(
                request.call_args.kwargs["schema"],
                staged.MARKET_MAP_WIRE_SCHEMA,
            )
            self.assertEqual(
                request.call_args.kwargs["max_tokens"],
                64_000,
            )
            self.assertEqual(
                request.call_args.kwargs["effort_override"],
                "medium",
            )
            self.assertIn(
                "COMPACT WIRE-ФОРМАТ",
                request.call_args.kwargs["system_prompt"],
            )

        with patch.object(
            staged,
            "_request_structured_stage",
            return_value=_wire_decision(decision),
        ) as request:
            self.assertEqual(
                staged.analyze_trade_decision(payload, market_map), decision
            )
            self.assertIs(
                request.call_args.kwargs["schema"],
                staged.TRADE_DECISION_WIRE_SCHEMA,
            )
            self.assertEqual(
                request.call_args.kwargs["max_tokens"],
                48_000,
            )
            self.assertEqual(
                request.call_args.kwargs["effort_override"],
                "medium",
            )

        with patch.object(
            staged,
            "_request_structured_stage",
            return_value=_wire_market_map(market_map),
        ) as request:
            self.assertEqual(
                staged.repair_market_map(
                    payload,
                    invalid_result={"broken": True},
                    validation_error="test failure",
                ),
                market_map,
            )
            self.assertEqual(request.call_args.kwargs["max_tokens"], 32_000)
            self.assertEqual(
                request.call_args.kwargs["effort_override"], "medium"
            )

        with patch.object(
            staged,
            "_request_structured_stage",
            return_value=_wire_decision(decision),
        ) as request:
            self.assertEqual(
                staged.repair_trade_decision(
                    payload,
                    market_map,
                    invalid_result={"broken": True},
                    validation_error="test failure",
                ),
                decision,
            )
            self.assertEqual(request.call_args.kwargs["max_tokens"], 24_000)
            self.assertEqual(
                request.call_args.kwargs["effort_override"], "medium"
            )

    def test_stage_split_covers_all_timeframes_and_limits_duplicate_h1(self):
        payload = _payload()
        payload["deterministic_market_facts"] = {
            "imbalances": {
                timeframe: []
                for timeframe in ("D1", "H4", "H1", "M30", "M15", "M5")
            }
        }
        map_input = staged.build_market_map_stage_payload(payload, None)
        map_raw = map_input["raw_market_d1_h4_h1"]
        self.assertEqual(
            set(
                map_raw["cacheable_history"][
                    "closed_market_history_before_day_start"
                ]
            ),
            {"D1", "H4", "H1"},
        )

        market_map = _market_map(
            payload,
            {
                "mode": "initialize",
                "preserved_anchor_ids": [],
                "invalidated_anchor_ids": [],
                "reason": "No previous anchors.",
            },
        )
        decision_input = staged.build_trade_decision_stage_payload(
            payload, market_map
        )
        decision_raw = decision_input["raw_execution_market_h1_m30_m15_m5"]
        decision_history = decision_raw["cacheable_history"][
            "closed_market_history_before_day_start"
        ]
        self.assertEqual(set(decision_history), {"H1", "M30", "M15", "M5"})

        h1_history_count = len(decision_history["H1"]["closed_bars"]["rows"])
        h1_live_count = len(
            decision_raw["live_market"]["raw_timeframes_since_day_start"][
                "H1"
            ]["closed_bars_since_day_start"]["rows"]
        )
        self.assertEqual(
            h1_history_count + h1_live_count,
            staged.DECISION_H1_CLOSED_BARS,
        )
        self.assertEqual(
            set(staged.MAP_TIMEFRAMES) | set(staged.DECISION_TIMEFRAMES),
            {"D1", "H4", "H1", "M30", "M15", "M5"},
        )
        self.assertNotIn("deterministic_market_facts", map_raw)
        self.assertNotIn("deterministic_market_facts", decision_raw)
        self.assertEqual(
            set(map_input["deterministic_market_facts"]["imbalances"]),
            {"D1", "H4", "H1"},
        )
        self.assertEqual(
            set(decision_input["deterministic_market_facts"]["imbalances"]),
            {"H1"},
        )

        position_input = staged.build_position_review_stage_payload(
            payload,
            review_trigger="h1_close",
            position_context={"primary_position_ticket": "123"},
            previous_reference=None,
        )
        self.assertEqual(
            set(position_input["deterministic_market_facts"]["imbalances"]),
            {"D1", "H4", "H1"},
        )
        self.assertNotIn(
            "deterministic_market_facts",
            position_input["raw_market_d1_h4_h1_m30_m15_m5"],
        )

    def test_tailored_prompt_and_bounded_effort_policy(self):
        staged_prompts = (
            len(staged.MARKET_MAP_SYSTEM_PROMPT)
            + len(staged.MARKET_MAP_WIRE_INSTRUCTIONS)
            + len(
                staged._completion_reserve_instruction(
                    staged.MAP_MAX_TOKENS
                )
            )
            + len(staged.TRADE_DECISION_SYSTEM_PROMPT)
            + len(staged.TRADE_DECISION_WIRE_INSTRUCTIONS)
            + len(
                staged._completion_reserve_instruction(
                    staged.DECISION_MAX_TOKENS
                )
            )
        )
        # Bilingual narratives plus compact Elliott/Fibonacci display rules are
        # part of the paid-stage contract; prompts must still remain far below
        # the legacy monolithic prompt.
        self.assertLess(staged_prompts, len(claude_client.SYSTEM_PROMPT) * 0.90)
        self.assertIn("fib_retracement", staged.MARKET_MAP_SYSTEM_PROMPT)
        self.assertIn("(I)..(V)", staged.MARKET_MAP_SYSTEM_PROMPT)
        self.assertIn("recommendation.fvg_role", staged.TRADE_DECISION_SYSTEM_PROMPT)
        self.assertIn("H4, H1 и M15", staged.POSITION_REVIEW_SYSTEM_PROMPT)
        self.assertIn("wave 3", staged.POSITION_REVIEW_SYSTEM_PROMPT)
        self.assertIn("wave 5", staged.POSITION_REVIEW_SYSTEM_PROMPT)
        self.assertIn("triangle, flag, pennant", staged.POSITION_REVIEW_SYSTEM_PROMPT)
        self.assertIn("не равен полному заполнению", staged.POSITION_REVIEW_SYSTEM_PROMPT)
        self.assertIn("filled_inactive", staged.POSITION_REVIEW_SYSTEM_PROMPT)
        self.assertEqual(
            staged.MAP_MAX_TOKENS + staged.DECISION_MAX_TOKENS,
            112_000,
        )
        self.assertEqual(staged.MAP_EFFORT, "medium")
        self.assertEqual(staged.DECISION_EFFORT, "medium")
        self.assertEqual(staged.REPAIR_EFFORT, "medium")
        self.assertEqual(staged.MAP_REPAIR_MAX_TOKENS, 32_000)
        self.assertEqual(staged.DECISION_REPAIR_MAX_TOKENS, 24_000)


class WavePersistenceTests(unittest.TestCase):
    def test_confirmed_anchor_is_preserved_without_regeneration(self):
        payload = _payload()
        prior_bar = payload["cacheable_history"][
            "closed_market_history_before_day_start"
        ]["H1"]["closed_bars"][-1]
        prior_point = {
            "scenario": "primary",
            "degree": "minor",
            "timeframe": "H1",
            "sequence": 1,
            "label": "1",
            "time": prior_bar["time"],
            "price": prior_bar["high"],
            "status": "confirmed",
        }
        previous_reference = {
            "saved_at_fp": "2026-08-17T11:00:00+03:00",
            "market_snapshot_time_fp": "2026-08-17T11:00:00+03:00",
            "h1_closed_bar_time_fp": prior_bar["time"],
            "analysis": {"visualization": _visualization([prior_point])},
        }
        anchors = staged.build_previous_confirmed_anchor_reference(
            previous_reference
        )
        anchor_id = anchors["anchors"][0]["anchor_id"]

        market_map = _market_map(
            payload,
            {
                "mode": "unchanged",
                "preserved_anchor_ids": [anchor_id],
                "invalidated_anchor_ids": [],
                "reason": "Fresh D1/H4/H1 independently confirms the pivot.",
            },
        )
        staged.validate_market_map_result(market_map, payload, anchors)
        decision_stage_input = staged.build_trade_decision_stage_payload(
            payload,
            market_map,
            previous_reference=previous_reference,
        )
        self.assertIn(
            prior_point,
            decision_stage_input["validated_market_map"]["visualization"][
                "wave_points"
            ],
        )

        micro_bar = payload["cacheable_history"][
            "closed_market_history_before_day_start"
        ]["M15"]["closed_bars"][-1]
        micro_point = {
            "scenario": "primary",
            "degree": "subminuette",
            "timeframe": "M15",
            "sequence": 2,
            "label": "2",
            "time": micro_bar["time"],
            "price": micro_bar["low"],
            "status": "developing",
        }
        analysis = staged.assemble_staged_analysis(
            payload=payload,
            market_map=market_map,
            trade_decision=_decision(payload, [micro_point]),
            previous_reference=previous_reference,
        )

        self.assertEqual(set(analysis), set(claude_client.CLAUDE_RESPONSE_SCHEMA["required"]))
        self.assertIn(prior_point, analysis["visualization"]["wave_points"])
        self.assertIn(micro_point, analysis["visualization"]["wave_points"])
        self.assertEqual(analysis["recommendation"]["action"], "stay_out")

    def test_invalidated_anchor_is_removed(self):
        payload = _payload()
        prior_bar = payload["cacheable_history"][
            "closed_market_history_before_day_start"
        ]["H1"]["closed_bars"][-1]
        prior_point = {
            "scenario": "primary",
            "degree": "minor",
            "timeframe": "H1",
            "sequence": 1,
            "label": "1",
            "time": prior_bar["time"],
            "price": prior_bar["high"],
            "status": "confirmed",
        }
        previous_reference = {
            "analysis": {"visualization": _visualization([prior_point])}
        }
        anchors = staged.build_previous_confirmed_anchor_reference(
            previous_reference
        )
        anchor_id = anchors["anchors"][0]["anchor_id"]
        market_map = _market_map(
            payload,
            {
                "mode": "recount",
                "preserved_anchor_ids": [],
                "invalidated_anchor_ids": [anchor_id],
                "reason": "Fresh structure invalidates the old pivot role.",
            },
        )
        analysis = staged.assemble_staged_analysis(
            payload=payload,
            market_map=market_map,
            trade_decision=_decision(payload),
            previous_reference=previous_reference,
        )
        self.assertNotIn(prior_point, analysis["visualization"]["wave_points"])


class StageRetryIsolationTests(unittest.TestCase):
    def test_decision_retry_does_not_repeat_validated_map(self):
        snapshot = {"instrument": "XAUUSD"}
        h1_time = "2026-08-17T11:00:00+03:00"
        calls = {"map": 0, "decision": 0}

        with tempfile.TemporaryDirectory() as temporary_dir:
            temporary_dir = Path(temporary_dir)
            archive_path = temporary_dir / "archive.json"
            archive_path.write_text(
                json.dumps(
                    {
                        "market_map_result": None,
                        "market_map_usage": None,
                        "trade_decision_result": None,
                        "trade_decision_usage": None,
                    }
                ),
                encoding="utf-8",
            )

            def map_call(on_preflight, on_response):
                calls["map"] += 1
                on_preflight(
                    {
                        "input_tokens": 100,
                        "payload_sha256": "map_hash",
                        "transport_payload_bytes": 1000,
                    }
                )
                return {"validated_map": True}

            def decision_call(on_preflight, on_response):
                calls["decision"] += 1
                on_preflight(
                    {
                        "input_tokens": 80,
                        "payload_sha256": "decision_hash",
                        "transport_payload_bytes": 800,
                    }
                )
                if calls["decision"] == 1:
                    raise ClaudeRequestOutcomeUnknownError("lost decision stream")
                return {"validated_decision": True}

            with (
                patch.object(
                    claude_request_guard,
                    "STATE_PATH",
                    temporary_dir / "journal.json",
                ),
                patch.object(
                    main,
                    "extract_latest_closed_h1_time",
                    return_value=h1_time,
                ),
                patch.object(
                    main,
                    "get_api_retry_policy",
                    return_value={
                        "max_attempts": 2,
                        "retry_delays_seconds": [0],
                    },
                ),
                patch.object(main.time, "sleep"),
            ):
                map_run = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL_MAP",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-08-17T12:05:00+03:00",
                    archive_path=archive_path,
                    api_call=map_call,
                    result_archive_key="market_map_result",
                    usage_archive_key="market_map_usage",
                )
                decision_run = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL_DECISION",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-08-17T12:05:00+03:00",
                    archive_path=archive_path,
                    api_call=decision_call,
                    result_archive_key="trade_decision_result",
                    usage_archive_key="trade_decision_usage",
                )
                recovered_map = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL_MAP",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-08-17T12:05:00+03:00",
                    archive_path=archive_path,
                    api_call=lambda *_: self.fail("FULL_MAP must be recovered"),
                    result_archive_key="market_map_result",
                    usage_archive_key="market_map_usage",
                )

        self.assertTrue(map_run["ok"])
        self.assertTrue(decision_run["ok"])
        self.assertTrue(recovered_map["recovered"])
        self.assertEqual(calls, {"map": 1, "decision": 2})


if __name__ == "__main__":
    unittest.main()
