"""Named wire records and bounded repairs. No market data is guessed here."""
from __future__ import annotations

import copy
import math
import re


def named_row(columns):
    return {"type": "object", "properties": {key: {"type": "string"} for key in columns},
            "required": list(columns), "additionalProperties": False}


def to_wire(value, schema):
    """Convert canonical values / complete legacy rows; never pad missing cells.

    This is used for saved-response repair only. A malformed row is preserved
    so the repair planner can request that entire section explicitly.
    """
    kind = schema.get("type")
    if kind == "object":
        properties = schema.get("properties", {})
        if isinstance(value, list) and all(s.get("type") == "string" for s in properties.values()):
            if len(value) != len(properties):
                return copy.deepcopy(value)
            value = dict(zip(properties, value))
        if isinstance(value, dict):
            return {key: to_wire(item, properties[key]) if key in properties else copy.deepcopy(item)
                    for key, item in value.items()}
    elif kind == "array" and isinstance(value, list):
        return [to_wire(item, schema["items"]) for item in value]
    elif kind == "string":
        if value is None:
            return ""
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, (int, float)) and math.isfinite(value):
            return str(value)
    return copy.deepcopy(value)


def contract_errors(value, schema, path="response"):
    """Small validator for the deliberately simple transmitted JSON schema."""
    errors = []
    kind = schema.get("type")
    if kind == "object":
        if not isinstance(value, dict):
            return [f"{path}: expected object"]
        props = schema.get("properties", {})
        for key in schema.get("required", []):
            if key not in value:
                errors.append(f"{path}.{key}: missing required field")
        for key, item in value.items():
            if key in props:
                errors.extend(contract_errors(item, props[key], f"{path}.{key}"))
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}.{key}: unexpected field")
    elif kind == "array":
        if not isinstance(value, list):
            return [f"{path}: expected array"]
        for index, item in enumerate(value):
            errors.extend(contract_errors(item, schema["items"], f"{path}[{index}]"))
    elif kind == "string" and not isinstance(value, str):
        errors.append(f"{path}: expected string")
    elif kind == "boolean" and not isinstance(value, bool):
        errors.append(f"{path}: expected boolean")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: unsupported enum value")
    return errors


def plan_repair(invalid, schema, validation_error):
    """Freeze good sections and expose only the broken sections in the schema.

    If a semantic error cannot be localized, all sections are explicitly in
    scope. This does not relax the final whole-response semantic validation.
    """
    original = to_wire(invalid, schema)
    if not isinstance(original, dict):
        original = {}
    props = schema["properties"]
    broken = [key for key, spec in props.items()
              if key not in original or contract_errors(original[key], spec, key)]
    semantic = [key for key in props
                if re.search(r"(?<!\w)" + re.escape(key) + r"(?!\w)", str(validation_error))]
    scope = [key for key in props if key in set(broken + semantic)]
    if not scope:
        scope = list(props)
    repair_schema = {"type": "object", "properties": {key: copy.deepcopy(props[key]) for key in scope},
                     "required": scope, "additionalProperties": False}
    return original, repair_schema


def merge_repair(original, patch, repair_schema, full_schema):
    # Reject unsolicited edits to valid sections, including accidental full
    # responses to a narrow repair request.
    patch = to_wire(patch, repair_schema)
    errors = contract_errors(patch, repair_schema, "repair")
    if errors:
        raise ValueError("; ".join(errors))
    merged = {key: copy.deepcopy(value) for key, value in original.items()
              if key in full_schema["properties"]}
    merged.update(copy.deepcopy(patch))
    errors = contract_errors(merged, full_schema)
    if errors:
        raise ValueError("; ".join(errors))
    return merged
