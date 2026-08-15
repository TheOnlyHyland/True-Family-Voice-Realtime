"""Authoritative Home Assistant room-light ON function tool."""

import logging
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import httpx
    from pipecat.services.llm_service import FunctionCallParams

logger = logging.getLogger(__name__)

ROOM_LIGHT_TOOL_NAME = "turn_on_room_lights"
DEFAULT_HA_API_BASE_URL = "http://supervisor/core/api"
_BRIGHTNESS_192_PAYLOAD = '{"brightness":192,"transition":1}'


@dataclass(frozen=True)
class ServiceStep:
    """One immutable Home Assistant service call in a room-light sequence."""

    domain: str
    service: str
    data: Mapping[str, Any]

    @property
    def action(self) -> str:
        return f"{self.domain}.{self.service}"


def _step(domain: str, service: str, data: Dict[str, Any]) -> ServiceStep:
    return ServiceStep(domain, service, MappingProxyType(dict(data)))


_LIVING_DINING_STEPS = (
    _step(
        "light",
        "turn_on",
        {
            "entity_id": "light.living_room_lights",
            "brightness_pct": 100,
            "color_temp_kelvin": 2950,
            "transition": 2,
        },
    ),
)


ROOM_LIGHT_STEPS: Mapping[str, tuple[ServiceStep, ...]] = MappingProxyType(
    {
        "kitchen": (
            _step(
                "scene",
                "turn_on",
                {
                    "entity_id": "scene.kitchen_lights_1_kitchen_daily",
                    "transition": 1,
                },
            ),
        ),
        "hallway": (
            _step(
                "scene",
                "turn_on",
                {"entity_id": "scene.hallway_lights_1_hallway_daily"},
            ),
            _step(
                "light",
                "turn_on",
                {"entity_id": "light.hallway_lamp", "brightness_pct": 50},
            ),
        ),
        "landing": (
            _step(
                "scene",
                "turn_on",
                {"entity_id": "scene.landing_lights_2_landing_daily"},
            ),
        ),
        "our_bedroom": (
            _step(
                "scene",
                "turn_on",
                {"entity_id": "scene.our_bedroom_lights_2_our_bedroom_daily"},
            ),
            _step(
                "input_number",
                "set_value",
                {
                    "entity_id": "input_number.our_bedroom_brightness_target",
                    "value": 192,
                },
            ),
            _step(
                "mqtt",
                "publish",
                {
                    "topic": "zigbee2mqtt/Our Bedroom Lights/set",
                    "payload": _BRIGHTNESS_192_PAYLOAD,
                },
            ),
        ),
        "living_room": _LIVING_DINING_STEPS,
        "dining_room": _LIVING_DINING_STEPS,
        "guest_bedroom": (
            _step(
                "input_number",
                "set_value",
                {
                    "entity_id": "input_number.guest_bedroom_brightness_target",
                    "value": 192,
                },
            ),
            _step(
                "mqtt",
                "publish",
                {
                    "topic": "zigbee2mqtt/Guest Bedroom Lights/set",
                    "payload": _BRIGHTNESS_192_PAYLOAD,
                },
            ),
        ),
        "courtyard": (
            _step(
                "mqtt",
                "publish",
                {
                    "topic": "zigbee2mqtt/Courtyard Lights/set",
                    "payload": _BRIGHTNESS_192_PAYLOAD,
                },
            ),
        ),
        "walk_in_wardrobe": (
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.walk_in_wardrobe_lights",
                    "brightness_pct": 100,
                    "color_temp_kelvin": 2699,
                },
            ),
        ),
        "utility_room": (
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.utility_room_pendant_light",
                    "brightness_pct": 100,
                    "color_temp_kelvin": 2485,
                    "transition": 2,
                },
            ),
        ),
        "upstairs_bathroom": (
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.upstairs_bathroom_lights",
                    "brightness_pct": 90,
                    "color_temp_kelvin": 2726,
                    "transition": 2,
                },
            ),
        ),
        "downstairs_bathroom": (
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.downstairs_bathroom_lights",
                    "brightness_pct": 80,
                },
            ),
        ),
        "clarks_bedroom": (
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.clarks_bedroom_lights",
                    "brightness_pct": 91,
                    "color_temp_kelvin": 2677,
                    "transition": 2,
                },
            ),
        ),
        "clarks_den": (
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.clarks_den_lights",
                    "brightness_pct": 100,
                    "color_temp_kelvin": 2699,
                },
            ),
        ),
        "clarks_toy_room": (
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.clarks_toy_room_light",
                    "brightness_pct": 100,
                },
            ),
        ),
        "cinema": (
            _step(
                "switch",
                "turn_on",
                {"entity_id": "switch.cinema_room_hulkbuster"},
            ),
            _step(
                "light",
                "turn_on",
                {
                    "entity_id": "light.cinema_room_lights",
                    "brightness_pct": 80,
                    "color_temp_kelvin": 2679,
                },
            ),
        ),
    }
)


def get_room_light_tool_definition() -> Dict[str, Any]:
    """OpenAI Realtime function-tool definition for mapped room-light ON calls."""
    return {
        "type": "function",
        "name": ROOM_LIGHT_TOOL_NAME,
        "description": (
            "Turn on one approved room's lights with its authoritative fixed sequence. "
            "This tool must be used instead of generic HassTurnOn for room-light ON "
            "requests because mixed Zigbee groups do not restore coherent state. It "
            "does not accept entity IDs, services, MQTT topics, or arbitrary targets. "
            "Do not automatically retry an error; explain it briefly to the user."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "room": {
                    "type": "string",
                    "enum": list(ROOM_LIGHT_STEPS),
                    "description": "Approved room whose complete light group to turn on.",
                }
            },
            "required": ["room"],
            "additionalProperties": False,
        },
    }


def _error(
    code: str,
    message: str,
    *,
    room: Optional[str] = None,
    completed_steps: int = 0,
    failed_step: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    error: Dict[str, Any] = {
        "code": code,
        "message": message,
        "retryable": False,
    }
    if failed_step is not None:
        error["failed_step"] = failed_step

    result: Dict[str, Any] = {
        "error": error,
        "completed_steps": completed_steps,
    }
    if room is not None:
        result["room"] = room
    return result


def _request_error(
    room: str,
    completed_steps: int,
    step: ServiceStep,
    message: str,
) -> Dict[str, Any]:
    return _error(
        "home_assistant_error",
        message,
        room=room,
        completed_steps=completed_steps,
        failed_step={
            "number": completed_steps + 1,
            "action": step.action,
        },
    )


async def turn_on_room_lights(
    room: Any,
    *,
    access_token: str,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    client: Optional["httpx.AsyncClient"] = None,
) -> Dict[str, Any]:
    """Run one room's exact ordered ON sequence through Home Assistant REST."""
    if not isinstance(room, str) or room not in ROOM_LIGHT_STEPS:
        return _error("invalid_room", "Unknown room.")

    steps = ROOM_LIGHT_STEPS[room]
    if not access_token:
        return _error(
            "home_assistant_unavailable",
            "Room-light control is unavailable.",
            room=room,
        )

    owns_client = client is None
    if client is None:
        import httpx

        http_client = httpx.AsyncClient(timeout=10.0)
    else:
        http_client = client

    try:
        completed_steps = 0
        for step in steps:
            try:
                response = await http_client.post(
                    f"{base_url.rstrip('/')}/services/{step.domain}/{step.service}",
                    json=dict(step.data),
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/json",
                    },
                )
            except Exception:
                logger.warning(
                    "Home Assistant room-light request failed at step=%s",
                    completed_steps + 1,
                )
                return _request_error(
                    room,
                    completed_steps,
                    step,
                    "Home Assistant could not complete the room-light step.",
                )

            if not 200 <= response.status_code < 300:
                logger.warning(
                    "Home Assistant rejected room-light request at step=%s",
                    completed_steps + 1,
                )
                return _request_error(
                    room,
                    completed_steps,
                    step,
                    "Home Assistant could not complete the room-light step.",
                )
            completed_steps += 1

        return {"room": room, "completed_steps": completed_steps}
    finally:
        if owns_client:
            try:
                await http_client.aclose()
            except Exception:
                logger.warning("Home Assistant room-light HTTP client could not close")


def create_room_light_tool_handler(
    access_token: str,
    *,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    client: Optional["httpx.AsyncClient"] = None,
) -> Callable[["FunctionCallParams"], Awaitable[None]]:
    """Create the Pipecat handler that dispatches mapped room-light ON calls."""

    async def room_light_tool_handler(params: "FunctionCallParams") -> None:
        arguments = params.arguments or {}
        if not isinstance(arguments, dict) or set(arguments) != {"room"}:
            result = _error(
                "invalid_arguments",
                "Exactly one approved room is required.",
            )
        else:
            try:
                result = await turn_on_room_lights(
                    arguments["room"],
                    access_token=access_token,
                    base_url=base_url,
                    client=client,
                )
            except Exception:
                logger.error("Unexpected room-light tool failure")
                result = _error(
                    "room_light_error",
                    "Room lights could not be turned on.",
                )
        await params.result_callback(result)

    return room_light_tool_handler


def register_room_light_tool(llm, access_token: str) -> None:
    """Dispatch-authorize the mapped room-light function on the OpenAI service."""
    llm.register_function(
        ROOM_LIGHT_TOOL_NAME,
        create_room_light_tool_handler(access_token),
    )
