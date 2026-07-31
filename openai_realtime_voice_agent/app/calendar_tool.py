"""Read-only Home Assistant calendar function tool."""

import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import httpx
    from pipecat.services.llm_service import FunctionCallParams

logger = logging.getLogger(__name__)

CALENDAR_TOOL_NAME = "get_calendar_events"
DEFAULT_HA_API_BASE_URL = "http://supervisor/core/api"
MAX_CALENDAR_RANGE = timedelta(days=31)
MAX_RETURNED_EVENTS = 20

CALENDAR_ENTITIES = {
    "personal": "calendar.adam_hyland_personal",
    "work": "calendar.adam_hyland_work",
    "bigin": "calendar.bigin_calendar",
    "family": "calendar.family",
    "uk_holidays": "calendar.holidays_in_the_united_kingdom_2",
    "brand": "calendar.tt_brand_building",
    "admin": "calendar.tt_general_admin",
    "installations": "calendar.tt_installations",
    "training": "calendar.tt_training",
    "company_holidays": "calendar.tt_holidays",
    "quotes": "calendar.tt_quotes",
}


def get_calendar_tool_definition() -> Dict[str, Any]:
    """OpenAI Realtime function-tool definition for calendar reads."""
    return {
        "type": "function",
        "name": CALENDAR_TOOL_NAME,
        "description": (
            "Read events from one approved Home Assistant calendar. This tool is "
            "strictly read-only and cannot create, change, or delete events. Supply "
            "explicit ISO 8601 start and end timestamps with timezone offsets. Do not "
            "automatically retry an error; explain it briefly to the user."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "calendar_key": {
                    "type": "string",
                    "enum": list(CALENDAR_ENTITIES),
                    "description": "Approved calendar key to read.",
                },
                "start": {
                    "type": "string",
                    "description": (
                        "Inclusive ISO 8601 start timestamp with timezone, for example "
                        "2026-07-31T00:00:00+01:00."
                    ),
                },
                "end": {
                    "type": "string",
                    "description": (
                        "Exclusive ISO 8601 end timestamp with timezone. It must be "
                        "after start and no more than 31 days later."
                    ),
                },
            },
            "required": ["calendar_key", "start", "end"],
            "additionalProperties": False,
        },
    }


def _error(code: str, message: str) -> Dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "retryable": False,
        }
    }


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError
    text = value.strip()
    if "T" not in text.upper():
        raise ValueError
    if text.upper().endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError
    return parsed


def _event_time(value: Any) -> tuple[str, bool]:
    if not isinstance(value, dict):
        raise ValueError
    has_date = "date" in value
    has_datetime = "dateTime" in value
    if has_date == has_datetime:
        raise ValueError
    event_time = value["date" if has_date else "dateTime"]
    if not isinstance(event_time, str) or not event_time.strip():
        raise ValueError
    return event_time, has_date


def _normalize_event(event: Any) -> Dict[str, Any]:
    if not isinstance(event, dict) or not isinstance(event.get("summary"), str):
        raise ValueError

    start, start_all_day = _event_time(event.get("start"))
    end, end_all_day = _event_time(event.get("end"))
    if start_all_day != end_all_day:
        raise ValueError

    normalized = {
        "summary": event["summary"],
        "start": start,
        "end": end,
        "all_day": start_all_day,
    }
    if event.get("location") is not None:
        if not isinstance(event["location"], str):
            raise ValueError
        normalized["location"] = event["location"]
    return normalized


async def fetch_calendar_events(
    calendar_key: Any,
    start: Any,
    end: Any,
    *,
    access_token: str,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    client: Optional["httpx.AsyncClient"] = None,
) -> Dict[str, Any]:
    """Validate and fetch a bounded, normalized event list with one GET request."""
    if not isinstance(calendar_key, str) or calendar_key not in CALENDAR_ENTITIES:
        return _error("invalid_calendar", "Unknown calendar key.")

    try:
        parsed_start = _parse_timestamp(start)
        parsed_end = _parse_timestamp(end)
    except (TypeError, ValueError):
        return _error(
            "invalid_timestamp",
            "Start and end must be ISO 8601 timestamps with timezone offsets.",
        )

    if parsed_end <= parsed_start:
        return _error("invalid_range", "End must be after start.")
    if parsed_end - parsed_start > MAX_CALENDAR_RANGE:
        return _error("range_too_large", "Calendar range cannot exceed 31 days.")
    if not access_token:
        return _error("calendar_unavailable", "Calendar access is unavailable.")

    owns_client = client is None
    if client is None:
        import httpx

        http_client = httpx.AsyncClient(timeout=10.0)
    else:
        http_client = client
    try:
        try:
            response = await http_client.get(
                f"{base_url.rstrip('/')}/calendars/{CALENDAR_ENTITIES[calendar_key]}",
                params={"start": start.strip(), "end": end.strip()},
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/json",
                },
            )
        except Exception:
            logger.warning("Home Assistant calendar request failed for %s", calendar_key)
            return _error(
                "home_assistant_error",
                "Home Assistant could not provide calendar events.",
            )

        if response.status_code == 404:
            return _error("calendar_unavailable", "That calendar is unavailable.")
        if response.status_code in (401, 403):
            return _error(
                "calendar_access_denied",
                "Home Assistant denied calendar access.",
            )
        if not 200 <= response.status_code < 300:
            logger.warning(
                "Home Assistant calendar request failed for %s with status %s",
                calendar_key,
                response.status_code,
            )
            return _error(
                "home_assistant_error",
                "Home Assistant could not provide calendar events.",
            )

        try:
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError
            events = [_normalize_event(event) for event in payload[:MAX_RETURNED_EVENTS]]
        except (TypeError, ValueError):
            return _error(
                "malformed_response",
                "Home Assistant returned malformed calendar data.",
            )
        return {"events": events}
    finally:
        if owns_client:
            await http_client.aclose()


def create_calendar_tool_handler(
    access_token: str,
    *,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    client: Optional["httpx.AsyncClient"] = None,
) -> Callable[["FunctionCallParams"], Awaitable[None]]:
    """Create the Pipecat handler that dispatches the calendar GET."""

    async def calendar_tool_handler(params: "FunctionCallParams") -> None:
        arguments = params.arguments or {}
        try:
            result = await fetch_calendar_events(
                arguments.get("calendar_key"),
                arguments.get("start"),
                arguments.get("end"),
                access_token=access_token,
                base_url=base_url,
                client=client,
            )
        except Exception:
            logger.exception("Unexpected calendar tool failure")
            result = _error(
                "calendar_error",
                "Calendar events could not be read.",
            )
        await params.result_callback(result)

    return calendar_tool_handler


def register_calendar_tool(llm, access_token: str) -> None:
    """Dispatch-authorize the calendar function on the active OpenAI service."""
    llm.register_function(
        CALENDAR_TOOL_NAME,
        create_calendar_tool_handler(access_token),
    )
