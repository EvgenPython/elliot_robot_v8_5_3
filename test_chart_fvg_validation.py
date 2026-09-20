import copy
import unittest

from chart_contract import sanitize_visualization


def _payload():
    bars = [
        {"time": "2026-09-10T01:00:00+03:00", "open": 99, "high": 100, "low": 98, "close": 99},
        {"time": "2026-09-10T02:00:00+03:00", "open": 99, "high": 105, "low": 99, "close": 104},
        {"time": "2026-09-10T03:00:00+03:00", "open": 105, "high": 107, "low": 104, "close": 106},
    ]
    return {
        "cacheable_history": {
            "symbol_specification": {"point": 0.1},
            "closed_market_history_before_day_start": {
                "H1": {"closed_bars": bars}
            },
        },
        "deterministic_market_facts": {
            "imbalances": {
                "H1": [
                    {
                        "id": "fvg_H1_2026-09-10T03:00:00+03:00_bullish",
                        "timeframe": "H1",
                        "direction": "bullish",
                        "price_low": 100.0,
                        "price_high": 104.0,
                        "midpoint": 102.0,
                        "status": "untouched",
                        "fill_fraction": 0.0,
                        "formed_at": "2026-09-10T03:00:00+03:00",
                        "active": True,
                        "active_price_low": 100.0,
                        "active_price_high": 104.0,
                    }
                ]
            }
        },
    }


def _analysis(low=100.0, high=104.0):
    return {
        "visualization": {
            "wave_points": [],
            "levels": [],
            "zones": [
                {
                    "kind": "fvg",
                    "scenario": "primary",
                    "timeframe": "H1",
                    "start_time": "2026-09-10T01:00:00+03:00",
                    "end_time": "2026-09-10T03:00:00+03:00",
                    "price_low": low,
                    "price_high": high,
                    "label": "H1 bullish FVG",
                }
            ],
            "scenario_paths": [],
            "trendlines": [],
            "channels": [],
            "pattern_shapes": [],
            "market_events": [],
            "projected_waves": [],
            "wave_structures": [],
            "chart_comment": "",
        },
        "recommendation": {"action": "stay_out"},
    }


class ChartFvgValidationTests(unittest.TestCase):
    def test_exact_python_candidate_is_enriched_as_claude_validated(self):
        analysis = _analysis()
        warnings = sanitize_visualization(analysis, _payload())
        self.assertEqual(warnings, [])
        zone = analysis["visualization"]["zones"][0]
        self.assertTrue(zone["python_geometry_validated"])
        self.assertTrue(zone["decision_relevant"])
        self.assertTrue(zone["active"])
        self.assertEqual(zone["midpoint"], 102.0)
        self.assertTrue(zone["fvg_id"].startswith("fvg_H1_"))

    def test_invented_fvg_coordinates_are_removed(self):
        analysis = _analysis(low=99.0, high=104.0)
        warnings = sanitize_visualization(analysis, copy.deepcopy(_payload()))
        self.assertTrue(warnings)
        self.assertEqual(analysis["visualization"]["zones"], [])

    def test_fully_filled_candidate_cannot_return_to_chart(self):
        payload = _payload()
        candidate = payload["deterministic_market_facts"]["imbalances"]["H1"][0]
        candidate.update({
            "status": "filled_inactive",
            "active": False,
            "active_price_low": None,
            "active_price_high": None,
            "fill_fraction": 1.0,
        })
        analysis = _analysis()
        warnings = sanitize_visualization(analysis, payload)
        self.assertTrue(warnings)
        self.assertEqual(analysis["visualization"]["zones"], [])

    def test_partial_zone_survives_repeated_validation_and_retires_on_full_fill(self):
        payload = _payload()
        candidate = payload["deterministic_market_facts"]["imbalances"]["H1"][0]
        candidate.update(status="midpoint_rejected", fill_fraction=0.5, active_price_high=102.0)
        analysis = _analysis()
        self.assertEqual(sanitize_visualization(analysis, payload), [])
        self.assertEqual(sanitize_visualization(analysis, payload), [])
        zone = analysis["visualization"]["zones"][0]
        self.assertEqual(zone["price_high"], 102.0)
        self.assertEqual(zone["original_price_high"], 104.0)
        candidate.update(status="filled_inactive", fill_fraction=1.0, active=False)
        sanitize_visualization(analysis, payload)
        self.assertEqual(analysis["visualization"]["zones"], [])


if __name__ == "__main__":
    unittest.main()
