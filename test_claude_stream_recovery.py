import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import claude_stream_recovery as recovery


SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "instrument": {"type": "string", "enum": ["XAUUSD"]},
        "signal": {"type": "string", "enum": ["stay_out"]},
    },
    "required": ["instrument", "signal"],
}


class _InterruptedTextStream:
    request_id = "req_partial"

    def __init__(self, chunks):
        self._chunks = list(chunks)

    @property
    def text_stream(self):
        def generator():
            for item in self._chunks:
                yield item
            raise ConnectionError("SSE lost before message_stop")

        return generator()

    def get_final_message(self):
        raise AssertionError("text_stream interruption must happen first")


class _CompletedStream:
    request_id = "req_complete"

    def __init__(self, text):
        self._text = text

    @property
    def text_stream(self):
        return iter([self._text[:10], self._text[10:]])

    def get_final_message(self):
        return SimpleNamespace(
            _request_id="req_complete",
            content=[SimpleNamespace(type="text", text=self._text)],
        )


class StreamRecoveryTests(unittest.TestCase):
    def test_complete_json_survives_lost_message_stop(self):
        text = json.dumps(
            {"instrument": "XAUUSD", "signal": "stay_out"},
            separators=(",", ":"),
        )
        with tempfile.TemporaryDirectory() as temporary_dir, patch.object(
            recovery,
            "STREAM_JOURNAL_DIR",
            Path(temporary_dir),
        ):
            result = recovery.consume_structured_stream(
                _InterruptedTextStream([text[:11], text[11:]]),
                stage="FULL_DECISION",
                schema=SCHEMA,
                payload_sha256="frozen",
            )
            journal_path = next(Path(temporary_dir).glob("*.json"))
            journal = json.loads(journal_path.read_text(encoding="utf-8"))

        self.assertIsNone(result["response"])
        self.assertIsNotNone(result["error"])
        self.assertEqual(
            result["recovered_result"],
            {"instrument": "XAUUSD", "signal": "stay_out"},
        )
        self.assertTrue(result["diagnostics"]["delivery_recovered"])
        self.assertEqual(journal["state"], "RECOVERED_COMPLETE_JSON")

    def test_incomplete_json_is_never_promoted(self):
        with tempfile.TemporaryDirectory() as temporary_dir, patch.object(
            recovery,
            "STREAM_JOURNAL_DIR",
            Path(temporary_dir),
        ):
            result = recovery.consume_structured_stream(
                _InterruptedTextStream(['{"instrument":"XAUUSD"']),
                stage="FULL_MAP",
                schema=SCHEMA,
                payload_sha256="frozen",
            )

        self.assertIsNone(result["recovered_result"])
        self.assertFalse(result["diagnostics"]["delivery_recovered"])
        self.assertEqual(result["diagnostics"]["stream_state"], "STREAM_INTERRUPTED")

    def test_unknown_fields_cannot_be_recovered(self):
        value = '{"instrument":"XAUUSD","signal":"stay_out","extra":1}'
        self.assertIsNone(recovery.recover_complete_json(value, SCHEMA))

    def test_normal_completed_stream_remains_normal(self):
        text = '{"instrument":"XAUUSD","signal":"stay_out"}'
        with tempfile.TemporaryDirectory() as temporary_dir, patch.object(
            recovery,
            "STREAM_JOURNAL_DIR",
            Path(temporary_dir),
        ):
            result = recovery.consume_structured_stream(
                _CompletedStream(text),
                stage="FULL_DECISION",
                schema=SCHEMA,
                payload_sha256="frozen",
            )

        self.assertIsNotNone(result["response"])
        self.assertIsNone(result["error"])
        self.assertFalse(result["diagnostics"]["delivery_recovered"])
        self.assertEqual(
            result["diagnostics"]["stream_state"],
            "FINAL_MESSAGE_RECEIVED",
        )


if __name__ == "__main__":
    unittest.main()

