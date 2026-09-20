"""Partial top-level JSON recovery for WaveFrame V8.5.3.

Structured-output streams can be interrupted after several complete top-level
fields have already arrived.  This module recovers only fields whose JSON value
is fully closed and locally valid.  It never invents or pads a missing value.
"""
from __future__ import annotations

import json
from typing import Any


def _skip_ws(text: str, index: int) -> int:
    while index < len(text) and text[index].isspace():
        index += 1
    return index


def _matches_type(value: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "null":
        return value is None
    return True


def _validate_value(value: Any, schema: dict, path: str = "$") -> None:
    """Small validator for the JSON-schema subset used by the micro pipeline."""
    if not isinstance(schema, dict):
        return

    if "anyOf" in schema:
        for candidate in schema.get("anyOf") or []:
            try:
                _validate_value(value, candidate, path)
                return
            except ValueError:
                pass
        raise ValueError(f"{path}: no anyOf branch matched")

    expected = schema.get("type")
    if isinstance(expected, list):
        if not any(_matches_type(value, item) for item in expected):
            raise ValueError(f"{path}: wrong type")
    elif isinstance(expected, str) and not _matches_type(value, expected):
        raise ValueError(f"{path}: expected {expected}")

    if "enum" in schema and value not in schema.get("enum", []):
        raise ValueError(f"{path}: value outside enum")
    if "const" in schema and value != schema.get("const"):
        raise ValueError(f"{path}: const mismatch")

    if isinstance(value, dict):
        properties = schema.get("properties") or {}
        if schema.get("additionalProperties") is False:
            unknown = set(value) - set(properties)
            if unknown:
                raise ValueError(f"{path}: unknown fields {sorted(unknown)}")
        for name in schema.get("required") or []:
            if name not in value:
                raise ValueError(f"{path}: missing {name}")
        for name, item in value.items():
            child = properties.get(name)
            if isinstance(child, dict):
                _validate_value(item, child, f"{path}.{name}")

    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for index, item in enumerate(value):
            _validate_value(item, schema["items"], f"{path}[{index}]")


def recover_completed_top_level_fields(text: str, schema: dict) -> dict:
    """Recover complete top-level key/value pairs from an incomplete JSON object.

    Example:
        {"a":"ok","b":{"x":"done"},"c":"unterminated...

    returns {"a": "ok", "b": {"x": "done"}} when a/b are present in schema.
    """
    source = str(text or "").strip()
    if not source:
        return {}

    # Structured output normally starts with '{'.  Ignore accidental leading
    # whitespace but deliberately do not try to repair prose/code fences.
    start = source.find("{")
    if start < 0:
        return {}
    source = source[start:]

    decoder = json.JSONDecoder()
    properties = (schema or {}).get("properties") or {}
    result = {}

    index = _skip_ws(source, 1)
    while index < len(source):
        if source[index] == "}":
            break

        try:
            key, after_key = decoder.raw_decode(source, index)
        except json.JSONDecodeError:
            break
        if not isinstance(key, str):
            break

        index = _skip_ws(source, after_key)
        if index >= len(source) or source[index] != ":":
            break
        index = _skip_ws(source, index + 1)

        try:
            value, after_value = decoder.raw_decode(source, index)
        except json.JSONDecodeError:
            break

        field_schema = properties.get(key)
        if isinstance(field_schema, dict):
            try:
                _validate_value(value, field_schema, f"$.{key}")
            except ValueError:
                pass
            else:
                result[key] = value

        index = _skip_ws(source, after_value)
        if index >= len(source):
            break
        if source[index] == ",":
            index = _skip_ws(source, index + 1)
            continue
        if source[index] == "}":
            break
        break

    return result
