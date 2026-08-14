"""Offline unit tests for the authoritative Home Assistant room-light tool."""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.room_light_tool import (  # noqa: E402
    DEFAULT_HA_API_BASE_URL,
    ROOM_LIGHT_STEPS,
    ROOM_LIGHT_TOOL_NAME,
    create_room_light_tool_handler,
    get_room_light_tool_definition,
    register_room_light_tool,
    turn_on_room_lights,
)


TEST_TOKEN = "unit-test-token-must-not-leak"
MQTT_PAYLOAD = '{"brightness":192,"transition":1}'


def _call(domain, service, body):
    return (
        f"{DEFAULT_HA_API_BASE_URL}/services/{domain}/{service}",
        body,
    )


EXPECTED_SEQUENCES = {
    "kitchen": (
        _call(
            "scene",
            "turn_on",
            {
                "entity_id": "scene.kitchen_lights_1_kitchen_daily",
                "transition": 1,
            },
        ),
    ),
    "hallway": (
        _call(
            "scene",
            "turn_on",
            {"entity_id": "scene.hallway_lights_1_hallway_daily"},
        ),
        _call(
            "light",
            "turn_on",
            {"entity_id": "light.hallway_lamp", "brightness_pct": 50},
        ),
    ),
    "landing": (
        _call(
            "scene",
            "turn_on",
            {"entity_id": "scene.landing_lights_2_landing_daily"},
        ),
    ),
    "our_bedroom": (
        _call(
            "scene",
            "turn_on",
            {"entity_id": "scene.our_bedroom_lights_2_our_bedroom_daily"},
        ),
        _call(
            "input_number",
            "set_value",
            {
                "entity_id": "input_number.our_bedroom_brightness_target",
                "value": 192,
            },
        ),
        _call(
            "mqtt",
            "publish",
            {
                "topic": "zigbee2mqtt/Our Bedroom Lights/set",
                "payload": MQTT_PAYLOAD,
            },
        ),
    ),
    "living_room": (
        _call(
            "light",
            "turn_on",
            {
                "entity_id": "light.living_room_lights",
                "brightness_pct": 100,
                "color_temp_kelvin": 2950,
                "transition": 2,
            },
        ),
    ),
    "dining_room": (
        _call(
            "light",
            "turn_on",
            {
                "entity_id": "light.living_room_lights",
                "brightness_pct": 100,
                "color_temp_kelvin": 2950,
                "transition": 2,
            },
        ),
    ),
    "guest_bedroom": (
        _call(
            "input_number",
            "set_value",
            {
                "entity_id": "input_number.guest_bedroom_brightness_target",
                "value": 192,
            },
        ),
        _call(
            "mqtt",
            "publish",
            {
                "topic": "zigbee2mqtt/Guest Bedroom Lights/set",
                "payload": MQTT_PAYLOAD,
            },
        ),
    ),
    "courtyard": (
        _call(
            "mqtt",
            "publish",
            {
                "topic": "zigbee2mqtt/Courtyard Lights/set",
                "payload": MQTT_PAYLOAD,
            },
        ),
    ),
    "walk_in_wardrobe": (
        _call(
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
        _call(
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
        _call(
            "light",
            "turn_on",
            {
                "entity_id": "light.upstairs_bathroom",
                "brightness_pct": 90,
                "color_temp_kelvin": 2726,
                "transition": 2,
            },
        ),
    ),
    "downstairs_bathroom": (
        _call(
            "light",
            "turn_on",
            {
                "entity_id": "light.downstairs_bathroom",
                "brightness_pct": 80,
            },
        ),
    ),
    "clarks_bedroom": (
        _call(
            "light",
            "turn_on",
            {
                "entity_id": "light.clarks_bedroom",
                "brightness_pct": 91,
                "color_temp_kelvin": 2677,
                "transition": 2,
            },
        ),
    ),
    "clarks_den": (
        _call(
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
        _call(
            "light",
            "turn_on",
            {
                "entity_id": "light.clarks_toy_room_light",
                "brightness_pct": 100,
            },
        ),
    ),
    "cinema": (
        _call(
            "switch",
            "turn_on",
            {"entity_id": "switch.cinema_room_hulkbuster"},
        ),
        _call(
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


class _MockResponse:
    def __init__(self, status_code, internal_body=None):
        self.status_code = status_code
        self.internal_body = internal_body


class _MockHttpClient:
    def __init__(self, statuses=None, *, exception_at=None, internal_body=None):
        self.statuses = list(statuses or [])
        self.exception_at = exception_at
        self.internal_body = internal_body
        self.requests = []

    async def post(self, url, *, json, headers):
        request_number = len(self.requests) + 1
        self.requests.append(SimpleNamespace(url=url, json=json, headers=headers))
        if request_number == self.exception_at:
            raise RuntimeError(f"internal transport detail {TEST_TOKEN}")
        status = (
            self.statuses[request_number - 1]
            if request_number <= len(self.statuses)
            else 200
        )
        return _MockResponse(status, self.internal_body)


class RoomLightToolTests(unittest.IsolatedAsyncioTestCase):
    def test_openai_schema_exposes_only_the_room_enum(self):
        definition = get_room_light_tool_definition()
        parameters = definition["parameters"]

        self.assertEqual(definition["name"], ROOM_LIGHT_TOOL_NAME)
        self.assertEqual(
            parameters["properties"]["room"]["enum"],
            list(EXPECTED_SEQUENCES),
        )
        self.assertEqual(parameters["required"], ["room"])
        self.assertFalse(parameters["additionalProperties"])
        self.assertEqual(set(parameters["properties"]), {"room"})
        self.assertIn(
            "must be used instead of generic HassTurnOn",
            definition["description"],
        )
        self.assertIn("mixed Zigbee groups", definition["description"])

    def test_step_definitions_are_ordered_and_immutable(self):
        self.assertEqual(list(ROOM_LIGHT_STEPS), list(EXPECTED_SEQUENCES))
        self.assertIs(
            ROOM_LIGHT_STEPS["living_room"],
            ROOM_LIGHT_STEPS["dining_room"],
        )
        with self.assertRaises(TypeError):
            cast(Any, ROOM_LIGHT_STEPS)["attic"] = ()
        with self.assertRaises(TypeError):
            cast(Any, ROOM_LIGHT_STEPS["kitchen"][0].data)["transition"] = 9

    async def test_every_room_posts_the_exact_ordered_url_and_body_sequence(self):
        for room, expected in EXPECTED_SEQUENCES.items():
            with self.subTest(room=room):
                client = _MockHttpClient()
                result = await turn_on_room_lights(
                    room,
                    access_token=TEST_TOKEN,
                    client=client,
                )

                self.assertEqual(
                    result,
                    {"room": room, "completed_steps": len(expected)},
                )
                self.assertEqual(
                    [(request.url, request.json) for request in client.requests],
                    list(expected),
                )
                for request in client.requests:
                    self.assertEqual(
                        request.headers,
                        {
                            "Authorization": f"Bearer {TEST_TOKEN}",
                            "Accept": "application/json",
                        },
                    )

    async def test_living_and_dining_aliases_are_equivalent(self):
        living_client = _MockHttpClient()
        dining_client = _MockHttpClient()

        await turn_on_room_lights(
            "living_room",
            access_token=TEST_TOKEN,
            client=living_client,
        )
        await turn_on_room_lights(
            "dining_room",
            access_token=TEST_TOKEN,
            client=dining_client,
        )

        self.assertEqual(
            [(request.url, request.json) for request in living_client.requests],
            [(request.url, request.json) for request in dining_client.requests],
        )

    async def test_invalid_room_performs_no_http(self):
        client = _MockHttpClient()

        result = await turn_on_room_lights(
            "garage",
            access_token=TEST_TOKEN,
            client=client,
        )

        self.assertEqual(
            result,
            {
                "error": {
                    "code": "invalid_room",
                    "message": "Unknown room.",
                    "retryable": False,
                },
                "completed_steps": 0,
            },
        )
        self.assertEqual(client.requests, [])

    async def test_non_2xx_failure_stops_before_later_steps(self):
        internal_body = f"sensitive response body {TEST_TOKEN}"
        client = _MockHttpClient(
            [200, 503, 200],
            internal_body=internal_body,
        )

        result = await turn_on_room_lights(
            "our_bedroom",
            access_token=TEST_TOKEN,
            client=client,
        )

        self.assertEqual(len(client.requests), 2)
        self.assertEqual(result["room"], "our_bedroom")
        self.assertEqual(result["completed_steps"], 1)
        self.assertEqual(result["error"]["code"], "home_assistant_error")
        self.assertFalse(result["error"]["retryable"])
        self.assertEqual(
            result["error"]["failed_step"],
            {"number": 2, "action": "input_number.set_value"},
        )
        self.assertNotIn(TEST_TOKEN, repr(result))
        self.assertNotIn(internal_body, repr(result))

    async def test_transport_exception_stops_before_later_steps_without_leaking(self):
        client = _MockHttpClient(exception_at=2)

        with self.assertLogs("app.room_light_tool", level="WARNING") as captured:
            result = await turn_on_room_lights(
                "our_bedroom",
                access_token=TEST_TOKEN,
                client=client,
            )

        self.assertEqual(len(client.requests), 2)
        self.assertEqual(result["completed_steps"], 1)
        self.assertEqual(
            result["error"]["failed_step"],
            {"number": 2, "action": "input_number.set_value"},
        )
        self.assertNotIn(TEST_TOKEN, repr(result))
        self.assertNotIn(TEST_TOKEN, "\n".join(captured.output))
        self.assertNotIn("internal transport detail", repr(result))

    async def test_handler_rejects_extra_arguments_without_http(self):
        client = _MockHttpClient()
        callback = AsyncMock()
        params = SimpleNamespace(
            arguments={
                "room": "kitchen",
                "entity_id": "light.unapproved",
            },
            result_callback=callback,
        )

        await create_room_light_tool_handler(
            TEST_TOKEN,
            client=client,
        )(params)

        callback.assert_awaited_once()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["error"]["code"], "invalid_arguments")
        self.assertFalse(result["error"]["retryable"])
        self.assertEqual(client.requests, [])

    async def test_handler_dispatches_the_mapped_function(self):
        client = _MockHttpClient()
        callback = AsyncMock()
        params = SimpleNamespace(
            arguments={"room": "courtyard"},
            result_callback=callback,
        )

        await create_room_light_tool_handler(
            TEST_TOKEN,
            client=client,
        )(params)

        callback.assert_awaited_once_with(
            {"room": "courtyard", "completed_steps": 1}
        )
        self.assertEqual(
            [(request.url, request.json) for request in client.requests],
            list(EXPECTED_SEQUENCES["courtyard"]),
        )

    def test_handler_is_registered_for_dispatch(self):
        registrations = {}

        class Llm:
            def register_function(self, name, handler):
                registrations[name] = handler

        register_room_light_tool(Llm(), TEST_TOKEN)

        self.assertEqual(set(registrations), {ROOM_LIGHT_TOOL_NAME})
        self.assertTrue(callable(registrations[ROOM_LIGHT_TOOL_NAME]))


if __name__ == "__main__":
    unittest.main()
