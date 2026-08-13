"""Offline tests for the authoritative Living Room TV power tool."""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.tv_power_tool import (  # noqa: E402
    DEFAULT_HA_API_BASE_URL,
    TV_POWER_ENTITY_ID,
    TV_POWER_TOOL_NAME,
    create_tv_power_tool_handler,
    get_tv_power_tool_definition,
    register_tv_power_tool,
    resolve_generic_tv_power,
    set_living_room_tv_power,
)


TEST_TOKEN = "unit-test-token-must-not-leak"


class _MockResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self.body = body

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return self.body


class _MockHttpClient:
    def __init__(
        self,
        *,
        post_status=200,
        states=None,
        post_exception=None,
        get_exceptions=None,
        entity_id=TV_POWER_ENTITY_ID,
    ):
        self.post_status = post_status
        self.states = list(states or [])
        self.post_exception = post_exception
        self.get_exceptions = list(get_exceptions or [])
        self.entity_id = entity_id
        self.requests = []

    async def post(self, url, *, json, headers):
        self.requests.append(SimpleNamespace(method="POST", url=url, json=json, headers=headers))
        if self.post_exception:
            raise self.post_exception
        return _MockResponse(self.post_status)

    async def get(self, url, *, headers):
        self.requests.append(SimpleNamespace(method="GET", url=url, json=None, headers=headers))
        if self.get_exceptions:
            exception = self.get_exceptions.pop(0)
            if exception is not None:
                raise exception
        state = self.states.pop(0) if self.states else "unknown"
        return _MockResponse(200, {"entity_id": self.entity_id, "state": state})


class TvPowerToolTests(unittest.IsolatedAsyncioTestCase):
    def test_schema_exposes_only_on_and_off_without_a_target(self):
        definition = get_tv_power_tool_definition()
        parameters = definition["parameters"]

        self.assertEqual(definition["name"], TV_POWER_TOOL_NAME)
        self.assertEqual(parameters["properties"]["power"]["enum"], ["on", "off"])
        self.assertEqual(parameters["required"], ["power"])
        self.assertFalse(parameters["additionalProperties"])
        self.assertEqual(set(parameters["properties"]), {"power"})
        self.assertIn("instead of generic HassTurnOn or HassTurnOff", definition["description"])

    def test_generic_tv_aliases_route_to_the_fixed_power_path(self):
        for function_name, expected in (("HassTurnOn", "on"), ("HassTurnOff", "off")):
            for arguments in (
                {"name": "Living Room TV"},
                {"name": "TV", "area": "Living Room"},
                {"name": "Living Room TV", "area": "living_room"},
                {"name": TV_POWER_ENTITY_ID},
                {"name": "Living TV Switch"},
                {"area": "Living Room", "device_class": ["tv"]},
            ):
                with self.subTest(function_name=function_name, arguments=arguments):
                    self.assertEqual(
                        resolve_generic_tv_power(function_name, arguments),
                        expected,
                    )

    def test_unrelated_generic_calls_remain_on_the_original_mcp_path(self):
        for function_name, arguments in (
            ("HassTurnOn", {"name": "Kitchen lamp"}),
            ("HassTurnOff", {"name": "TV", "area": "Cinema"}),
            ("HassTurnOff", {"name": "TV", "area": "cinema"}),
            ("HassLightSet", {"name": "Living Room TV"}),
            ("HassTurnOn", {"area": "Living Room", "domain": ["switch"]}),
            ("HassTurnOn", None),
        ):
            with self.subTest(function_name=function_name, arguments=arguments):
                self.assertIsNone(resolve_generic_tv_power(function_name, arguments))

    async def test_on_calls_only_the_fixed_switch_and_verifies_state(self):
        client = _MockHttpClient(states=["on"])

        result = await set_living_room_tv_power(
            "on",
            access_token=TEST_TOKEN,
            client=client,
            verify_delay=0,
        )

        self.assertEqual(result, {"power": "on", "verified": True})
        self.assertEqual(
            [(request.method, request.url, request.json) for request in client.requests],
            [
                (
                    "POST",
                    f"{DEFAULT_HA_API_BASE_URL}/services/switch/turn_on",
                    {"entity_id": TV_POWER_ENTITY_ID},
                ),
                (
                    "GET",
                    f"{DEFAULT_HA_API_BASE_URL}/states/{TV_POWER_ENTITY_ID}",
                    None,
                ),
            ],
        )

    async def test_off_uses_turn_off_and_verifies_after_one_stale_read(self):
        client = _MockHttpClient(states=["on", "off"])

        result = await set_living_room_tv_power(
            "off",
            access_token=TEST_TOKEN,
            client=client,
            verify_attempts=2,
            verify_delay=0,
        )

        self.assertEqual(result, {"power": "off", "verified": True})
        self.assertEqual(client.requests[0].url, f"{DEFAULT_HA_API_BASE_URL}/services/switch/turn_off")
        self.assertEqual(sum(request.method == "GET" for request in client.requests), 2)

    async def test_stale_state_never_reports_success(self):
        client = _MockHttpClient(states=["off", "off"])

        result = await set_living_room_tv_power(
            "on",
            access_token=TEST_TOKEN,
            client=client,
            verify_attempts=2,
            verify_delay=0,
        )

        self.assertEqual(result["error"]["code"], "verification_failed")
        self.assertFalse(result["error"]["retryable"])
        self.assertNotIn("verified", result)

    async def test_matching_state_from_another_entity_never_reports_success(self):
        client = _MockHttpClient(
            states=["on"],
            entity_id="switch.unapproved",
        )

        result = await set_living_room_tv_power(
            "on",
            access_token=TEST_TOKEN,
            client=client,
            verify_attempts=1,
            verify_delay=0,
        )

        self.assertEqual(result["error"]["code"], "verification_failed")

    async def test_transient_verification_failure_uses_later_attempt(self):
        client = _MockHttpClient(
            states=["on"],
            get_exceptions=[RuntimeError(f"private {TEST_TOKEN}"), None],
        )

        with self.assertLogs("app.tv_power_tool", level="WARNING") as captured:
            result = await set_living_room_tv_power(
                "on",
                access_token=TEST_TOKEN,
                client=client,
                verify_attempts=2,
                verify_delay=0,
            )

        self.assertEqual(result, {"power": "on", "verified": True})
        self.assertEqual(sum(request.method == "GET" for request in client.requests), 2)
        self.assertNotIn(TEST_TOKEN, "\n".join(captured.output))

    async def test_invalid_power_or_missing_token_performs_no_http(self):
        client = _MockHttpClient()

        invalid = await set_living_room_tv_power(
            "toggle",
            access_token=TEST_TOKEN,
            client=client,
        )
        unavailable = await set_living_room_tv_power(
            "on",
            access_token="",
            client=client,
        )

        self.assertEqual(invalid["error"]["code"], "invalid_power")
        self.assertEqual(unavailable["error"]["code"], "home_assistant_unavailable")
        self.assertEqual(client.requests, [])

    async def test_service_failure_is_sanitized_and_not_verified(self):
        client = _MockHttpClient(post_exception=RuntimeError(f"private {TEST_TOKEN}"))

        with self.assertLogs("app.tv_power_tool", level="WARNING") as captured:
            result = await set_living_room_tv_power(
                "on",
                access_token=TEST_TOKEN,
                client=client,
            )

        self.assertEqual(result["error"]["code"], "home_assistant_error")
        self.assertEqual(len(client.requests), 1)
        self.assertNotIn(TEST_TOKEN, repr(result))
        self.assertNotIn(TEST_TOKEN, "\n".join(captured.output))

    async def test_handler_rejects_entity_override_without_http(self):
        client = _MockHttpClient()
        callback = AsyncMock()
        params = SimpleNamespace(
            arguments={"power": "on", "entity_id": "switch.unapproved"},
            result_callback=callback,
        )

        await create_tv_power_tool_handler(TEST_TOKEN, client=client)(params)

        callback.assert_awaited_once()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["error"]["code"], "invalid_arguments")
        self.assertEqual(client.requests, [])

    async def test_handler_dispatches_and_registration_is_exact(self):
        client = _MockHttpClient(states=["on"])
        callback = AsyncMock()
        params = SimpleNamespace(arguments={"power": "on"}, result_callback=callback)

        await create_tv_power_tool_handler(TEST_TOKEN, client=client)(params)
        callback.assert_awaited_once_with({"power": "on", "verified": True})

        registrations = {}

        class Llm:
            def register_function(self, name, handler):
                registrations[name] = handler

        register_tv_power_tool(Llm(), TEST_TOKEN)
        self.assertEqual(set(registrations), {TV_POWER_TOOL_NAME})
        self.assertTrue(callable(registrations[TV_POWER_TOOL_NAME]))


if __name__ == "__main__":
    unittest.main()
