"""Strict JSON decoding and exact field contracts for the Voice PE protocol."""

import json
from typing import Any, Iterable


MAX_CONTROL_MESSAGE_BYTES = 2048

TRUSTED_BACKEND_TO_DEVICE_FIELDS = {
    "hello": (
        "type",
        "nonce",
        "audio_out",
        "follow_up_ms",
        "follow_up_open_delay_ms",
        "wake_open_delay_ms",
        "playback_prebuffer_ms",
    ),
    "request_follow_up": ("type", "token", "session_nonce"),
    "cancel_request_follow_up": ("type", "token", "session_nonce"),
    "commit_follow_up": ("type", "token", "session_nonce", "ready_nonce"),
    "ack": ("type", "session_nonce", "wake_generation"),
    "phase": ("type", "value", "session_nonce", "wake_generation"),
    "follow_up_progress_phase": (
        "type",
        "value",
        "token",
        "session_nonce",
        "wake_generation",
    ),
    "prepare_suppress_followup": ("type", "token"),
    "commit_suppress_followup": ("type", "token"),
    "cancel_suppress_followup": ("type", "token"),
}

LEGACY_BACKEND_TO_DEVICE_FIELDS = {
    "hello": (
        "type",
        "audio_out",
        "follow_up_ms",
        "follow_up_open_delay_ms",
        "wake_open_delay_ms",
        "playback_prebuffer_ms",
    ),
    "ack": ("type",),
    "phase": ("type", "value"),
    "prepare_suppress_followup": ("type", "token"),
    "commit_suppress_followup": ("type", "token"),
    "cancel_suppress_followup": ("type", "token"),
}

TRUSTED_DEVICE_TO_BACKEND_FIELDS = {
    "hello_ack": (
        "type",
        "nonce",
        "accepted",
        "audio_out",
        "follow_up_ms",
        "follow_up_open_delay_ms",
        "wake_open_delay_ms",
        "playback_prebuffer_ms",
    ),
    "request_follow_up_ack": ("type", "token", "session_nonce", "accepted"),
    "follow_up_ready": ("type", "token", "session_nonce", "ready_nonce"),
    "commit_follow_up_ack": (
        "type",
        "token",
        "session_nonce",
        "ready_nonce",
        "accepted",
    ),
    "cancel_request_follow_up_ack": (
        "type",
        "token",
        "session_nonce",
        "accepted",
        "cleared",
    ),
    "suppress_followup_ack": (
        "type",
        "stage",
        "token",
        "session_nonce",
        "wake_generation",
        "accepted",
    ),
    "wake": ("type", "session_nonce", "wake_generation"),
    "flush": ("type", "session_nonce", "wake_generation"),
    "button_cancel": ("type", "session_nonce", "wake_generation"),
    "false_flag": ("type", "session_nonce", "wake_generation"),
    "interrupt": ("type", "session_nonce", "wake_generation", "reason"),
    "client_revoke": ("type", "session_nonce", "wake_generation", "reason"),
}

LEGACY_DEVICE_TO_BACKEND_FIELDS = {
    "suppress_followup_ack": ("type", "stage", "token", "accepted"),
    "wake": ("type",),
    "flush": ("type",),
    "button_cancel": ("type",),
    "false_flag": ("type",),
    "interrupt": ("type",),
}


class DuplicateJSONKeyError(ValueError):
    """Raised when a protocol object repeats a JSON member name."""


def _object_without_duplicates(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJSONKeyError("duplicate JSON member")
        result[key] = value
    return result


def _reject_nonstandard_number(_value: str) -> None:
    raise ValueError("non-standard JSON number")


def decode_protocol_object(message: str) -> dict[str, Any]:
    """Decode one bounded JSON object while rejecting duplicate keys and NaN."""
    if not isinstance(message, str):
        raise TypeError("protocol message must be text")
    if len(message.encode("utf-8")) > MAX_CONTROL_MESSAGE_BYTES:
        raise ValueError("protocol message is too large")
    value = json.loads(
        message,
        object_pairs_hook=_object_without_duplicates,
        parse_constant=_reject_nonstandard_number,
    )
    if not isinstance(value, dict):
        raise ValueError("protocol message must be an object")
    return value


def has_exact_fields(value: dict[str, Any], fields: Iterable[str]) -> bool:
    """Return whether an object has exactly the named fields, in any order."""
    expected = tuple(fields)
    return len(value) == len(expected) and set(value) == set(expected)
