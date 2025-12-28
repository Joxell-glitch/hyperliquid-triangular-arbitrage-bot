from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


JSON_SAFE_TYPES = (str, int, float, bool, type(None))


def safe_config_snapshot(settings: Any) -> dict[str, Any]:
    snapshot = _extract_settings_snapshot(settings)
    if not isinstance(snapshot, Mapping):
        snapshot = {"value": snapshot}
    return _normalize_json_safe(snapshot)


def _extract_settings_snapshot(settings: Any) -> Any:
    if hasattr(settings, "model_dump") and callable(settings.model_dump):
        return settings.model_dump()
    if hasattr(settings, "dict") and callable(settings.dict):
        return settings.dict()
    if hasattr(settings, "to_dict") and callable(settings.to_dict):
        return settings.to_dict()
    try:
        return vars(settings)
    except TypeError:
        pass
    try:
        return dict(settings)
    except (TypeError, ValueError):
        return {"value": str(settings)}


def _normalize_json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_normalize_json_safe(item) for item in value]
    if isinstance(value, JSON_SAFE_TYPES):
        return value
    return str(value)
