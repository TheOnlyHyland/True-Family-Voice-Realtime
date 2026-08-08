"""Offline tests for the internal nearby-media follow-up guard."""

import asyncio
import json
import sys
import unittest
from pathlib import Path


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.media_activity import (  # noqa: E402
    MAX_NEARBY_MEDIA_PLAYERS,
    MAX_STATE_RESPONSE_BYTES,
    MediaActivity,
    parse_nearby_media_players,
    query_nearby_media_activity,
)


class _Response:
    def __init__(self, body, *, status=200, delay=0.0, headers=None):
        self.status_code = status
        self._body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self._delay = delay
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def aiter_bytes(self):
        if self._delay:
            await asyncio.sleep(self._delay)
        yield self._body


class _Client:
    def __init__(self, responses):
        self.responses = responses
        self.requests = []
        self.closed = False

    def stream(self, method, url, *, headers):
        entity_id = url.rsplit("/", 1)[-1]
        self.requests.append((method, url, headers))
        response = self.responses[entity_id]
        return response() if callable(response) else response

    async def aclose(self):
        self.closed = True


class MediaActivityTests(unittest.IsolatedAsyncioTestCase):
    def test_parser_accepts_only_a_bounded_fixed_media_player_list(self):
        self.assertEqual(
            parse_nearby_media_players(
                " media_player.living_room ,media_player.kitchen "
            ),
            ("media_player.living_room", "media_player.kitchen"),
        )
        self.assertEqual(parse_nearby_media_players(""), ())

        invalid = (
            "light.living_room",
            "media_player.Living_Room",
            "media_player.one,media_player.one",
            ",".join(
                f"media_player.room_{index}"
                for index in range(MAX_NEARBY_MEDIA_PLAYERS + 1)
            ),
        )
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    parse_nearby_media_players(value)

    async def test_active_and_conservatively_paused_states_suppress(self):
        for state in ("playing", "buffering", "on", "paused"):
            with self.subTest(state=state):
                entity_id = "media_player.nearby"
                client = _Client(
                    {entity_id: _Response({"entity_id": entity_id, "state": state})}
                )

                result = await query_nearby_media_activity(
                    (entity_id,),
                    access_token="test-token",
                    client=client,
                )

                self.assertIs(result, MediaActivity.ACTIVE)

    async def test_every_configured_player_must_be_clearly_inactive(self):
        entities = (
            "media_player.one",
            "media_player.two",
            "media_player.three",
        )
        client = _Client(
            {
                entity_id: _Response(
                    {"entity_id": entity_id, "state": state}
                )
                for entity_id, state in zip(
                    entities,
                    ("off", "idle", "standby"),
                )
            }
        )

        result = await query_nearby_media_activity(
            entities,
            access_token="test-token",
            client=client,
        )

        self.assertIs(result, MediaActivity.CLEAR)
        self.assertEqual(
            {request[1] for request in client.requests},
            {
                f"http://supervisor/core/api/states/{entity_id}"
                for entity_id in entities
            },
        )
        for method, _url, headers in client.requests:
            self.assertEqual(method, "GET")
            self.assertEqual(headers["Authorization"], "Bearer test-token")
            self.assertEqual(headers["Accept"], "application/json")

    async def test_unknown_unavailable_denied_and_malformed_are_uncertain(self):
        entity_id = "media_player.nearby"
        cases = {
            "unknown": _Response({"entity_id": entity_id, "state": "unknown"}),
            "unavailable": _Response(
                {"entity_id": entity_id, "state": "unavailable"}
            ),
            "denied": _Response({}, status=403),
            "wrong_entity": _Response(
                {"entity_id": "media_player.other", "state": "off"}
            ),
            "bad_json": _Response(b"not-json"),
            "oversized": _Response(b"x" * (MAX_STATE_RESPONSE_BYTES + 1)),
            "bad_length": _Response(
                {"entity_id": entity_id, "state": "off"},
                headers={"content-length": "invalid"},
            ),
            "negative_length": _Response(
                {"entity_id": entity_id, "state": "off"},
                headers={"content-length": "-1"},
            ),
        }
        for name, response in cases.items():
            with self.subTest(name=name):
                result = await query_nearby_media_activity(
                    (entity_id,),
                    access_token="test-token",
                    client=_Client({entity_id: response}),
                )
                self.assertIs(result, MediaActivity.UNCERTAIN)

    async def test_timeout_and_missing_auth_are_uncertain(self):
        entity_id = "media_player.nearby"
        slow_client = _Client(
            {
                entity_id: _Response(
                    {"entity_id": entity_id, "state": "off"},
                    delay=1.0,
                )
            }
        )

        with self.assertLogs("app.media_activity", level="WARNING"):
            timed_out = await query_nearby_media_activity(
                (entity_id,),
                access_token="test-token",
                timeout_s=0.001,
                client=slow_client,
            )
        no_auth = await query_nearby_media_activity(
            (entity_id,),
            access_token="",
            client=slow_client,
        )

        self.assertIs(timed_out, MediaActivity.UNCERTAIN)
        self.assertIs(no_auth, MediaActivity.UNCERTAIN)
        self.assertEqual(len(slow_client.requests), 1)

    async def test_invalid_direct_query_scope_fails_closed_without_http(self):
        entity_id = "media_player.nearby"
        client = _Client({})

        duplicate = await query_nearby_media_activity(
            (entity_id, entity_id),
            access_token="test-token",
            client=client,
        )
        invalid_timeout = await query_nearby_media_activity(
            (entity_id,),
            access_token="test-token",
            timeout_s=0,
            client=client,
        )

        self.assertIs(duplicate, MediaActivity.UNCERTAIN)
        self.assertIs(invalid_timeout, MediaActivity.UNCERTAIN)
        self.assertEqual(client.requests, [])

    async def test_empty_configuration_fails_closed_without_home_assistant_request(self):
        client = _Client({})

        result = await query_nearby_media_activity(
            (),
            access_token="",
            client=client,
        )

        self.assertIs(result, MediaActivity.UNCERTAIN)
        self.assertEqual(client.requests, [])


if __name__ == "__main__":
    unittest.main()
