"""Early bounded logging policy for Pipecat and the add-on."""

import logging
import os
import re
import sys
from typing import Any
from urllib.parse import quote, unquote, urlsplit

from loguru import logger as _loguru_logger


MAX_LOG_MESSAGE_CHARS = 1024
_PRODUCTION_LEVELS = {"INFO", "WARNING", "ERROR", "CRITICAL"}
_SENSITIVE_PIPECAT_MARKERS = (
    "arguments",
    "conversation.item",
    "input_audio",
    "messages",
    "payload",
    "transcript",
)
_SECRET_ENV_NAMES = (
    "OPENAI_API_KEY",
    "SUPERVISOR_TOKEN",
    "LONGLIVED_TOKEN",
    "HA_ACCESS_TOKEN",
    "HA_MCP_TOKEN",
    "HA_TOKEN",
    "HOME_ASSISTANT_TOKEN",
    "ANNOUNCE_TOKEN",
    "OPENCLAW_API_KEY",
    "OPENCLAW_TOKEN",
)
_BEARER_CREDENTIAL = re.compile(
    r"(?i)(\bbearer(?:\s+|%20))([^\s,;'\"}\]]+)"
)
_configured = False


def _bound_message(message: Any) -> str:
    text = str(message)
    if len(text) <= MAX_LOG_MESSAGE_CHARS:
        return text
    return text[: MAX_LOG_MESSAGE_CHARS - 12] + " [truncated]"


def _redact_openclaw_url(message: Any) -> str:
    """Remove the configured secret bridge URL and every secret path spelling."""
    text = str(message)
    configured = os.environ.get("OPENCLAW_URL", "").strip()
    if not configured:
        return text
    parsed = urlsplit(configured)
    replacements = {configured, configured.rstrip("/")}
    if parsed.path and parsed.path != "/":
        replacements.update(
            {
                parsed.path,
                unquote(parsed.path),
                quote(unquote(parsed.path), safe="/:@"),
            }
        )
    if parsed.query:
        decoded_query = unquote(parsed.query)
        encoded_query = quote(decoded_query, safe="=&/:@")
        replacements.update(
            {
                parsed.query,
                f"?{parsed.query}",
                decoded_query,
                f"?{decoded_query}",
                encoded_query,
                f"?{encoded_query}",
            }
        )
    if parsed.fragment:
        decoded_fragment = unquote(parsed.fragment)
        encoded_fragment = quote(decoded_fragment, safe="=&/:@")
        replacements.update(
            {
                parsed.fragment,
                f"#{parsed.fragment}",
                decoded_fragment,
                f"#{decoded_fragment}",
                encoded_fragment,
                f"#{encoded_fragment}",
            }
        )
    for secret in sorted(replacements, key=len, reverse=True):
        if secret:
            text = text.replace(secret, "<redacted-openclaw-url>")
    return text


def _redact_configured_secrets(message: Any) -> str:
    """Redact every configured credential without revealing which one matched."""
    text = _BEARER_CREDENTIAL.sub(r"\1<redacted-credential>", str(message))
    spellings = set()
    for name in _SECRET_ENV_NAMES:
        value = os.environ.get(name, "")
        if not value:
            continue
        spellings.update(
            {
                value,
                unquote(value),
                quote(unquote(value), safe=""),
                quote(unquote(value), safe="/:@"),
            }
        )
    for secret in sorted(spellings, key=len, reverse=True):
        if secret:
            text = text.replace(secret, "<redacted-credential>")
    return text


def _sanitize_message(message: Any) -> str:
    return _bound_message(
        _redact_configured_secrets(_redact_openclaw_url(message))
    )


class _BoundedRedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return _sanitize_message(super().format(record))


def sanitize_loguru_record(record: dict[str, Any]) -> bool:
    """Redact sensitive Pipecat event bodies and bound every Loguru record."""
    message = str(record.get("message", ""))
    logger_name = str(record.get("name", ""))
    lowered = message.lower()
    if logger_name.startswith("pipecat") and any(
        marker in lowered for marker in _SENSITIVE_PIPECAT_MARKERS
    ):
        message = "Pipecat event redacted (sensitive payload)"
    record["message"] = _sanitize_message(message)
    if record.get("exception") is not None:
        record["exception"] = None
    return True


def configure_production_logging() -> None:
    """Replace Loguru's default DEBUG sink before Pipecat is imported."""
    global _configured
    if _configured:
        return
    requested_level = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = (
        requested_level
        if requested_level in _PRODUCTION_LEVELS
        else "INFO"
    )
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )
    formatter = _BoundedRedactingFormatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)
    logging.getLogger("aiortc").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("__main__").setLevel(logging.INFO)

    _loguru_logger.remove()
    _loguru_logger.add(
        sys.stderr,
        level=level,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | "
            "{name}:{function}:{line} - {message}"
        ),
        filter=sanitize_loguru_record,
        backtrace=False,
        diagnose=False,
        enqueue=False,
    )
    _configured = True
