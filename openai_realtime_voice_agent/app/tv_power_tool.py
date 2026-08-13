"""Authoritative Living Room TV power tool."""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import httpx
    from pipecat.services.llm_service import FunctionCallParams

logger = logging.getLogger(__name__)

TV_POWER_TOOL_NAME = "set_living_room_tv_power"
TV_POWER_ENTITY_ID = "switch.living_room_tv_smart_switch"
DEFAULT_HA_API_BASE_URL = "http://supervisor/core/api"
_GENERIC_POWER_TOOLS = {
    "HassTurnOn": "on",
    "HassTurnOff": "off",
}
_TV_ALIASES = {
    "living room smart switch",
    "living room television",
    "living room tv",
    "living room tv plug",
    "living room tv switch",
    "living tv switch",
    "television",
    "tv",
    "tv plug",
    "tv switch",
    TV_POWER_ENTITY_ID,
}


def resolve_generic_tv_power(function_name: Any, arguments: Any) -> Optional[str]:
    """Map only known Living Room TV generic calls to the fixed power state."""
    power = _GENERIC_POWER_TOOLS.get(function_name)
    if power is None or not isinstance(arguments, dict):
        return None

    area = arguments.get("area")
    normalized_area = (
        area.strip().casefold().replace("_", " ")
        if isinstance(area, str)
        else area
    )
    if area is not None and normalized_area != "living room":
        return None

    name = arguments.get("name")
    if isinstance(name, str) and name.strip().casefold() in _TV_ALIASES:
        return power

    device_classes = arguments.get("device_class")
    if isinstance(device_classes, str):
        device_classes = [device_classes]
    if (
        isinstance(area, str)
        and isinstance(device_classes, list)
        and "tv" in device_classes
    ):
        return power
    return None


def get_tv_power_tool_definition() -> Dict[str, Any]:
    """Return the fixed-target OpenAI function schema."""
    return {
        "type": "function",
        "name": TV_POWER_TOOL_NAME,
        "description": (
            "Turn the Living Room TV power on or off through its authoritative "
            "smart switch and verify the result. Always use this tool instead of "
            "generic HassTurnOn or HassTurnOff for Living Room TV, television, "
            "TV switch, TV plug, or TV power requests. Do not claim success when "
            "the tool returns an error."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "power": {
                    "type": "string",
                    "enum": ["on", "off"],
                    "description": "Requested Living Room TV power state.",
                }
            },
            "required": ["power"],
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


async def set_living_room_tv_power(
    power: Any,
    *,
    access_token: str,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    client: Optional["httpx.AsyncClient"] = None,
    verify_attempts: int = 6,
    verify_delay: float = 0.25,
) -> Dict[str, Any]:
    """Set the fixed TV switch and verify Home Assistant reports the target state."""
    if power not in ("on", "off"):
        return _error("invalid_power", "Power must be on or off.")
    if not access_token:
        return _error("home_assistant_unavailable", "TV power control is unavailable.")

    owns_client = client is None
    if client is None:
        import httpx

        http_client = httpx.AsyncClient(timeout=10.0)
    else:
        http_client = client

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    base = base_url.rstrip("/")
    try:
        try:
            response = await http_client.post(
                f"{base}/services/switch/turn_{power}",
                json={"entity_id": TV_POWER_ENTITY_ID},
                headers=headers,
            )
        except Exception:
            logger.warning("Home Assistant TV power request failed")
            return _error("home_assistant_error", "TV power could not be changed.")

        if not 200 <= response.status_code < 300:
            logger.warning("Home Assistant rejected TV power request")
            return _error("home_assistant_error", "TV power could not be changed.")

        for attempt in range(max(1, verify_attempts)):
            try:
                state_response = await http_client.get(
                    f"{base}/states/{TV_POWER_ENTITY_ID}",
                    headers=headers,
                )
                if 200 <= state_response.status_code < 300:
                    state = state_response.json()
                    if (
                        isinstance(state, dict)
                        and state.get("entity_id") == TV_POWER_ENTITY_ID
                        and state.get("state") == power
                    ):
                        return {"power": power, "verified": True}
            except Exception:
                logger.warning("Home Assistant TV power verification attempt failed")
            if attempt + 1 < max(1, verify_attempts):
                await asyncio.sleep(max(0.0, verify_delay))

        return _error(
            "verification_failed",
            "TV power changed request was sent, but the requested state was not confirmed.",
        )
    finally:
        if owns_client:
            try:
                await http_client.aclose()
            except Exception:
                logger.warning("Home Assistant TV power HTTP client could not close")


def create_tv_power_tool_handler(
    access_token: str,
    *,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    client: Optional["httpx.AsyncClient"] = None,
) -> Callable[["FunctionCallParams"], Awaitable[None]]:
    """Create the fixed-target TV power handler."""

    async def tv_power_tool_handler(params: "FunctionCallParams") -> None:
        arguments = params.arguments or {}
        if not isinstance(arguments, dict) or set(arguments) != {"power"}:
            result = _error("invalid_arguments", "Exactly one power state is required.")
        else:
            try:
                result = await set_living_room_tv_power(
                    arguments["power"],
                    access_token=access_token,
                    base_url=base_url,
                    client=client,
                )
            except Exception:
                logger.error("Unexpected TV power tool failure")
                result = _error("tv_power_error", "TV power could not be changed.")
        await params.result_callback(result)

    return tv_power_tool_handler


def create_tv_power_dispatcher(
    access_token: str,
    *,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    client: Optional["httpx.AsyncClient"] = None,
) -> Callable[[str, "FunctionCallParams"], Awaitable[None]]:
    """Create the dispatcher used by both explicit and intercepted TV calls."""

    async def tv_power_dispatcher(
        power: str,
        params: "FunctionCallParams",
    ) -> None:
        result = await set_living_room_tv_power(
            power,
            access_token=access_token,
            base_url=base_url,
            client=client,
        )
        await params.result_callback(result)

    return tv_power_dispatcher


def register_tv_power_tool(llm, access_token: str) -> None:
    """Register the authoritative TV power function."""
    llm.register_function(
        TV_POWER_TOOL_NAME,
        create_tv_power_tool_handler(access_token),
    )
