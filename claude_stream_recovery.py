"""Durable recovery for interrupted Claude structured-output streams.

The trading strategy does not live in this module.  It only protects delivery
of an already requested JSON response:

* streamed text is journaled incrementally on local disk;
* a connection loss does not automatically discard a complete JSON object;
* recovered JSON must satisfy the same closed JSON schema before it is returned;
* incomplete output is never guessed, completed or promoted to trading input.
"""

from __future__ import annotations

import json
import os
import uuid

from claude_partial_json import recover_completed_top_level_fields
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
STREAM_JOURNAL_DIR = BASE_DIR / "debug" / "claude_stream_journal"
PERSIST_EVERY_CHARACTERS = 4096


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary_path, path)


def _relative_debug_path(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR)).replace("\\", "/")
    except ValueError:
        return str(path)


def _request_id_from_stream(stream) -> str | None:
    value = getattr(stream, "request_id", None)
    if value in (None, ""):
        value = getattr(stream, "_request_id", None)
    return str(value) if value not in (None, "") else None


def _response_text(response) -> str:
    parts = []
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) == "text":
            value = getattr(block, "text", "")
            if value:
                parts.append(str(value))
    return "".join(parts).strip()


def _matches_schema_type(value, expected_type: str) -> bool:
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "array":
        return isinstance(value, list)
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected_type == "null":
        return value is None
    return True


def validate_closed_json_schema(value, schema: dict, path: str = "$") -> None:
    """Validate the JSON-schema subset used by the robot's strict outputs."""
    if not isinstance(schema, dict):
        return

    if "anyOf" in schema:
        errors = []
        for candidate in schema.get("anyOf") or []:
            try:
                validate_closed_json_schema(value, candidate, path)
                break
            except ValueError as error:
                errors.append(str(error))
        else:
            raise ValueError(
                f"{path}: значение не соответствует ни одному anyOf: "
                + " | ".join(errors[:3])
            )
        return

    expected_type = schema.get("type")
    if isinstance(expected_type, list):
        if not any(_matches_schema_type(value, item) for item in expected_type):
            raise ValueError(f"{path}: неверный JSON type.")
    elif isinstance(expected_type, str) and not _matches_schema_type(
        value, expected_type
    ):
        raise ValueError(
            f"{path}: ожидался type={expected_type}, "
            f"получен {type(value).__name__}."
        )

    if "enum" in schema and value not in schema.get("enum", []):
        raise ValueError(f"{path}: значение отсутствует в enum.")
    if "const" in schema and value != schema.get("const"):
        raise ValueError(f"{path}: значение не совпадает с const.")

    if isinstance(value, dict):
        properties = schema.get("properties") or {}
        required = schema.get("required") or []
        missing = [name for name in required if name not in value]
        if missing:
            raise ValueError(f"{path}: отсутствуют required поля {missing}.")
        if schema.get("additionalProperties") is False:
            unknown = sorted(set(value) - set(properties))
            if unknown:
                raise ValueError(f"{path}: неизвестные поля {unknown}.")
        for name, child in value.items():
            child_schema = properties.get(name)
            if isinstance(child_schema, dict):
                validate_closed_json_schema(
                    child,
                    child_schema,
                    f"{path}.{name}",
                )

    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for index, child in enumerate(value):
            validate_closed_json_schema(
                child,
                schema["items"],
                f"{path}[{index}]",
            )


def recover_complete_json(text: str, schema: dict) -> dict | None:
    """Return only a complete, schema-valid top-level object."""
    candidate = str(text or "").strip()
    if not candidate:
        return None
    try:
        value = json.loads(candidate)
        if not isinstance(value, dict) or not value:
            return None
        validate_closed_json_schema(value, schema)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return value


class StreamJournal:
    """Incrementally persists one paid SSE delivery attempt."""

    def __init__(self, stage: str, payload_sha256: str | None):
        self.stage = str(stage).strip().upper()
        self.payload_sha256 = str(payload_sha256 or "") or None
        self.journal_id = str(uuid.uuid4())
        self.path = STREAM_JOURNAL_DIR / (
            f"{self.stage.lower()}_{self.journal_id}.json"
        )
        self.request_id = None
        self.state = "CREATED"
        self.text_parts: list[str] = []
        self.chunk_count = 0
        self.received_characters = 0
        self.persisted_characters = 0
        self.created_at_utc = _utc_now()
        self.updated_at_utc = self.created_at_utc
        self.error_type = None
        self.error_message = None
        self.persist_error = None
        self._persist()

    @property
    def text(self) -> str:
        return "".join(self.text_parts)

    def diagnostics(self) -> dict:
        return {
            "stream_journal_path": _relative_debug_path(self.path),
            "stream_state": self.state,
            "stream_chunks": self.chunk_count,
            "stream_received_characters": self.received_characters,
            "request_id": self.request_id,
            "stream_journal_error": self.persist_error,
        }

    def _record(self) -> dict:
        return {
            "journal_version": 1,
            "journal_id": self.journal_id,
            "stage": self.stage,
            "payload_sha256": self.payload_sha256,
            "request_id": self.request_id,
            "state": self.state,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "chunk_count": self.chunk_count,
            "received_characters": self.received_characters,
            "complete_json_recovered": self.state == "RECOVERED_COMPLETE_JSON",
            "error_type": self.error_type,
            "error_message": self.error_message,
            "streamed_text": self.text,
        }

    def _persist(self) -> None:
        self.updated_at_utc = _utc_now()
        try:
            _atomic_write_json(self.path, self._record())
            self.persisted_characters = self.received_characters
            self.persist_error = None
        except OSError as error:
            # A debug/journal disk failure must not interrupt a paid Claude
            # response that is still arriving over the network.
            self.persist_error = f"{type(error).__name__}: {error}"

    def mark_open(self, request_id=None) -> None:
        self.request_id = (
            str(request_id) if request_id not in (None, "") else None
        )
        self.state = "STREAM_OPEN"
        self._persist()

    def add_text(self, value) -> None:
        if value in (None, ""):
            return
        text_value = str(value)
        self.text_parts.append(text_value)
        self.received_characters += len(text_value)
        self.chunk_count += 1
        if (
            self.received_characters - self.persisted_characters
            >= PERSIST_EVERY_CHARACTERS
        ):
            self.state = "STREAMING"
            self._persist()

    def mark_final(self, request_id=None, response_text: str | None = None) -> None:
        if request_id not in (None, ""):
            self.request_id = str(request_id)
        if not self.text and response_text:
            self.text_parts = [str(response_text)]
            self.received_characters = len(str(response_text))
            self.chunk_count = max(1, self.chunk_count)
        self.state = "FINAL_MESSAGE_RECEIVED"
        self._persist()

    def mark_interrupted(self, error: Exception, request_id=None) -> None:
        if request_id not in (None, ""):
            self.request_id = str(request_id)
        self.state = "STREAM_INTERRUPTED"
        self.error_type = type(error).__name__
        self.error_message = str(error)
        self._persist()

    def mark_recovered(self) -> None:
        self.state = "RECOVERED_COMPLETE_JSON"
        self._persist()


def consume_structured_stream(
    stream,
    *,
    stage: str,
    schema: dict,
    payload_sha256: str | None,
    on_progress=None,
) -> dict:
    """Consume an SDK stream and preserve enough state for safe recovery.

    The function returns a dictionary instead of throwing delivery exceptions.
    API/business classification remains the caller's responsibility.
    """
    journal = StreamJournal(stage, payload_sha256)
    request_id = _request_id_from_stream(stream)
    journal.mark_open(request_id)
    if callable(on_progress):
        on_progress(dict(journal.diagnostics()))

    try:
        text_stream = getattr(stream, "text_stream", None)
        if text_stream is not None:
            last_partial_fields = {}
            for text_delta in text_stream:
                journal.add_text(text_delta)
                partial_fields = recover_completed_top_level_fields(
                    journal.text, schema
                )
                if partial_fields != last_partial_fields:
                    last_partial_fields = partial_fields
                    if callable(on_progress):
                        progress = dict(journal.diagnostics())
                        progress["partial_fields"] = partial_fields
                        on_progress(progress)

        response = stream.get_final_message()
        request_id = _request_id_from_stream(stream)
        if request_id in (None, ""):
            request_id = getattr(response, "_request_id", None)
        journal.mark_final(
            request_id=request_id,
            response_text=_response_text(response),
        )
        diagnostics = journal.diagnostics()
        diagnostics.update(
            {
                "delivery_recovered": False,
                "billing_status": "USAGE_AVAILABLE",
            }
        )
        if callable(on_progress):
            on_progress(dict(diagnostics))
        return {
            "response": response,
            "recovered_result": None,
            "error": None,
            "diagnostics": diagnostics,
        }
    except Exception as error:
        request_id = _request_id_from_stream(stream) or request_id
        journal.mark_interrupted(error, request_id=request_id)
        recovered_result = recover_complete_json(journal.text, schema)
        partial_fields = recover_completed_top_level_fields(journal.text, schema)
        if recovered_result is not None:
            journal.mark_recovered()

        diagnostics = journal.diagnostics()
        diagnostics.update(
            {
                "delivery_recovered": recovered_result is not None,
                "billing_status": "UNKNOWN_MAY_BE_BILLED",
                "partial_fields": partial_fields,
            }
        )
        if callable(on_progress):
            on_progress(dict(diagnostics))
        return {
            "response": None,
            "recovered_result": recovered_result,
            "error": error,
            "diagnostics": diagnostics,
        }
