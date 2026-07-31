"""Offline unit tests for the read-only Home Assistant calendar tool."""

import sys
import unittest
from pathlib import Path
from urllib.parse import urlparse


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.calendar_tool import (  # noqa: E402
    CALENDAR_ENTITIES,
    CALENDAR_TOOL_NAME,
    fetch_calendar_events,
    get_calendar_tool_definition,
    register_calendar_tool,
)


START = "2026-07-01T00:00:00+01:00"
END = "2026-07-08T00:00:00+01:00"
TEST_TOKEN = "unit-test-token"


class _MockRequest:
    def __init__(self, url, params, headers):
        self.method = "GET"
        self.url = url
        self.params = params
        self.headers = headers


class _MockResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _MockHttpClient:
    def __init__(self, handler):
        self._handler = handler

    async def get(self, url, *, params, headers):
        return self._handler(_MockRequest(url, params, headers))


class CalendarToolTests(unittest.IsolatedAsyncioTestCase):
    async def _fetch(self, handler, **overrides):
        arguments = {
            "calendar_key": "personal",
            "start": START,
            "end": END,
            "access_token": TEST_TOKEN,
            "client": _MockHttpClient(handler),
        }
        arguments.update(overrides)
        return await fetch_calendar_events(**arguments)

    def test_approved_calendar_mapping(self):
        self.assertEqual(
            CALENDAR_ENTITIES,
            {
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
            },
        )

    def test_openai_schema_exposes_only_the_mapped_inputs(self):
        definition = get_calendar_tool_definition()
        parameters = definition["parameters"]

        self.assertEqual(definition["name"], CALENDAR_TOOL_NAME)
        self.assertEqual(
            parameters["properties"]["calendar_key"]["enum"],
            list(CALENDAR_ENTITIES),
        )
        self.assertEqual(
            parameters["required"],
            ["calendar_key", "start", "end"],
        )
        self.assertFalse(parameters["additionalProperties"])

    def test_handler_is_registered_for_dispatch(self):
        registrations = {}

        class Llm:
            def register_function(self, name, handler):
                registrations[name] = handler

        register_calendar_tool(Llm(), TEST_TOKEN)

        self.assertIn(CALENDAR_TOOL_NAME, registrations)
        self.assertTrue(callable(registrations[CALENDAR_TOOL_NAME]))

    async def test_successful_event_normalization(self):
        def handler(request):
            self.assertEqual(request.method, "GET")
            self.assertEqual(
                urlparse(request.url).path,
                "/core/api/calendars/calendar.adam_hyland_personal",
            )
            self.assertEqual(request.params["start"], START)
            self.assertEqual(request.params["end"], END)
            self.assertEqual(request.headers["Authorization"], f"Bearer {TEST_TOKEN}")
            return _MockResponse(
                200,
                [
                    {
                        "summary": "Planning",
                        "start": {"dateTime": "2026-07-02T09:00:00+01:00"},
                        "end": {"dateTime": "2026-07-02T10:00:00+01:00"},
                        "location": "Office",
                        "description": "Must not be returned",
                        "uid": "unrelated",
                    },
                    {
                        "summary": "Holiday",
                        "start": {"date": "2026-07-04"},
                        "end": {"date": "2026-07-05"},
                    },
                ],
            )

        result = await self._fetch(handler)

        self.assertEqual(
            result,
            {
                "events": [
                    {
                        "summary": "Planning",
                        "start": "2026-07-02T09:00:00+01:00",
                        "end": "2026-07-02T10:00:00+01:00",
                        "all_day": False,
                        "location": "Office",
                    },
                    {
                        "summary": "Holiday",
                        "start": "2026-07-04",
                        "end": "2026-07-05",
                        "all_day": True,
                    },
                ]
            },
        )

    async def test_no_events(self):
        result = await self._fetch(lambda request: _MockResponse(200, []))
        self.assertEqual(result, {"events": []})

    async def test_bad_calendar_key_does_not_call_home_assistant(self):
        def handler(request):
            self.fail("HTTP must not be called for an invalid calendar key")

        result = await self._fetch(handler, calendar_key="private")
        self.assertEqual(result["error"]["code"], "invalid_calendar")
        self.assertFalse(result["error"]["retryable"])

    async def test_range_over_31_days_does_not_call_home_assistant(self):
        def handler(request):
            self.fail("HTTP must not be called for an oversized range")

        result = await self._fetch(
            handler,
            start="2026-07-01T00:00:00Z",
            end="2026-08-02T00:00:00Z",
        )
        self.assertEqual(result["error"]["code"], "range_too_large")

    async def test_end_must_be_after_start(self):
        def handler(request):
            self.fail("HTTP must not be called for an inverted range")

        result = await self._fetch(handler, start=END, end=START)
        self.assertEqual(result["error"]["code"], "invalid_range")

    async def test_malformed_timestamp_does_not_call_home_assistant(self):
        def handler(request):
            self.fail("HTTP must not be called for an invalid timestamp")

        result = await self._fetch(handler, start="next Tuesday")
        self.assertEqual(result["error"]["code"], "invalid_timestamp")

    async def test_unavailable_calendar(self):
        result = await self._fetch(lambda request: _MockResponse(404, {}))
        self.assertEqual(result["error"]["code"], "calendar_unavailable")

    async def test_home_assistant_failure_is_concise_and_token_free(self):
        result = await self._fetch(
            lambda request: _MockResponse(503, {"detail": "internal details"})
        )
        self.assertEqual(result["error"]["code"], "home_assistant_error")
        self.assertFalse(result["error"]["retryable"])
        self.assertNotIn(TEST_TOKEN, repr(result))
        self.assertNotIn("internal details", repr(result))

    async def test_malformed_home_assistant_response(self):
        result = await self._fetch(
            lambda request: _MockResponse(200, {"events": []})
        )
        self.assertEqual(result["error"]["code"], "malformed_response")

    async def test_event_limit_is_20(self):
        payload = [
            {
                "summary": f"Event {index}",
                "start": {"dateTime": f"2026-07-02T{index % 24:02d}:00:00+01:00"},
                "end": {"dateTime": f"2026-07-02T{(index + 1) % 24:02d}:00:00+01:00"},
            }
            for index in range(25)
        ]
        result = await self._fetch(
            lambda request: _MockResponse(200, payload)
        )
        self.assertEqual(len(result["events"]), 20)
        self.assertEqual(result["events"][-1]["summary"], "Event 19")


if __name__ == "__main__":
    unittest.main()
