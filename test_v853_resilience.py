from __future__ import annotations

import unittest

from claude_partial_json import recover_completed_top_level_fields


class PartialRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.schema = {
            "type": "object",
            "properties": {
                "primary_scenario": {"type": "string"},
                "alternate_scenario": {"type": "string"},
                "expected_path": {"type": "string"},
                "invalidation": {"type": "string"},
                "next_opportunity": {"type": "string"},
            },
            "required": [
                "primary_scenario", "alternate_scenario", "expected_path",
                "invalidation", "next_opportunity",
            ],
            "additionalProperties": False,
        }

    def test_recovers_only_closed_fields(self):
        text = (
            '{"primary_scenario":"p","alternate_scenario":"a",'
            '"expected_path":"e","invalidation":"unfinished'
        )
        recovered = recover_completed_top_level_fields(text, self.schema)
        self.assertEqual(
            recovered,
            {
                "primary_scenario": "p",
                "alternate_scenario": "a",
                "expected_path": "e",
            },
        )

    def test_nested_value_must_be_complete(self):
        schema = {
            "type": "object",
            "properties": {
                "levels": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"price": {"type": "string"}},
                        "required": ["price"],
                        "additionalProperties": False,
                    },
                },
                "comment": {"type": "string"},
            },
            "required": ["levels", "comment"],
            "additionalProperties": False,
        }
        text = '{"levels":[{"price":"1"}],"comment":"cut'
        recovered = recover_completed_top_level_fields(text, schema)
        self.assertEqual(recovered, {"levels": [{"price": "1"}]})

    def test_invalid_field_is_not_salvaged(self):
        schema = {
            "type": "object",
            "properties": {
                "mode": {"type": "string", "enum": ["a", "b"]},
                "reason": {"type": "string"},
            },
            "required": ["mode", "reason"],
            "additionalProperties": False,
        }
        recovered = recover_completed_top_level_fields(
            '{"mode":"wrong","reason":"ok"}', schema
        )
        self.assertEqual(recovered, {"reason": "ok"})

    def test_unknown_fields_ignored(self):
        recovered = recover_completed_top_level_fields(
            '{"garbage":"x","primary_scenario":"p",', self.schema
        )
        self.assertEqual(recovered, {"primary_scenario": "p"})


if __name__ == "__main__":
    unittest.main()
