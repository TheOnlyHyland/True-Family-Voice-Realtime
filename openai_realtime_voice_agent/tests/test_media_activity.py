"""Offline tests for the internal nearby-media follow-up guard."""

import asyncio
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.media_activity import (  # noqa: E402
    MAX_NEARBY_MEDIA_PLAYERS,
    MAX_STATE_RESPONSE_BYTES,
    MediaActivity,
    NearbyMediaActivityGuard,
    parse_nearby_media_power_entity,
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

    def test_power_parser_accepts_only_blank_or_one_lowercase_switch(self):
        self.assertEqual(parse_nearby_media_power_entity(None), "")
        self.assertEqual(parse_nearby_media_power_entity("  "), "")
        self.assertEqual(
            parse_nearby_media_power_entity(
                " switch.living_room_tv_smart_switch "
            ),
            "switch.living_room_tv_smart_switch",
        )

        invalid = (
            False,
            1,
            "switch.",
            "switch.Living_Room",
            "light.living_room_tv_smart_switch",
            "switch.one,switch.two",
            "switch.living-room",
        )
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    parse_nearby_media_power_entity(value)

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

    async def test_power_off_short_circuits_unavailable_players(self):
        power_entity_id = "switch.living_room_tv_smart_switch"
        players = (
            "media_player.living_room_tv",
            "media_player.living_room_tv_audio",
        )
        client = _Client(
            {
                power_entity_id: _Response(
                    {"entity_id": power_entity_id, "state": "off"}
                ),
                players[0]: _Response(
                    {"entity_id": players[0], "state": "unavailable"}
                ),
                players[1]: _Response({}, status=404),
            }
        )

        result = await query_nearby_media_activity(
            players,
            access_token="test-token",
            client=client,
            power_entity_id=power_entity_id,
        )

        self.assertIs(result, MediaActivity.CLEAR)
        self.assertEqual(
            [request[1] for request in client.requests],
            [f"http://supervisor/core/api/states/{power_entity_id}"],
        )

    async def test_power_on_requires_all_players_to_be_clearly_inactive(self):
        power_entity_id = "switch.living_room_tv_smart_switch"
        players = (
            "media_player.living_room_tv",
            "media_player.living_room_tv_audio",
        )
        client = _Client(
            {
                power_entity_id: _Response(
                    {"entity_id": power_entity_id, "state": "on"}
                ),
                players[0]: _Response(
                    {"entity_id": players[0], "state": "standby"}
                ),
                players[1]: _Response(
                    {"entity_id": players[1], "state": "idle"}
                ),
            }
        )

        result = await query_nearby_media_activity(
            players,
            access_token="test-token",
            client=client,
            power_entity_id=power_entity_id,
        )

        self.assertIs(result, MediaActivity.CLEAR)
        self.assertEqual(
            client.requests[0][1],
            f"http://supervisor/core/api/states/{power_entity_id}",
        )
        self.assertEqual(
            {request[1] for request in client.requests[1:]},
            {
                f"http://supervisor/core/api/states/{entity_id}"
                for entity_id in players
            },
        )

    async def test_uncertain_power_state_never_queries_players(self):
        power_entity_id = "switch.living_room_tv_smart_switch"
        player = "media_player.living_room_tv"
        cases = {
            "unknown": _Response(
                {"entity_id": power_entity_id, "state": "unknown"}
            ),
            "unavailable": _Response(
                {"entity_id": power_entity_id, "state": "unavailable"}
            ),
            "missing": _Response({}, status=404),
            "denied": _Response({}, status=403),
            "malformed": _Response(b"not-json"),
        }

        for name, response in cases.items():
            with self.subTest(name=name):
                client = _Client({power_entity_id: response})

                result = await query_nearby_media_activity(
                    (player,),
                    access_token="test-token",
                    client=client,
                    power_entity_id=power_entity_id,
                )

                self.assertIs(result, MediaActivity.UNCERTAIN)
                self.assertEqual(
                    [request[1] for request in client.requests],
                    [f"http://supervisor/core/api/states/{power_entity_id}"],
                )

    async def test_power_and_player_reads_share_one_overall_timeout(self):
        power_entity_id = "switch.living_room_tv_smart_switch"
        player = "media_player.living_room_tv"
        client = _Client(
            {
                power_entity_id: _Response(
                    {"entity_id": power_entity_id, "state": "on"},
                    delay=0.2,
                ),
                player: _Response(
                    {"entity_id": player, "state": "off"},
                    delay=0.2,
                ),
            }
        )

        with self.assertLogs("app.media_activity", level="WARNING"):
            result = await query_nearby_media_activity(
                (player,),
                access_token="test-token",
                timeout_s=0.3,
                client=client,
                power_entity_id=power_entity_id,
            )

        self.assertIs(result, MediaActivity.UNCERTAIN)
        self.assertEqual(len(client.requests), 2)

    async def test_power_timeout_is_uncertain_and_does_not_query_players(self):
        power_entity_id = "switch.living_room_tv_smart_switch"
        player = "media_player.living_room_tv"
        client = _Client(
            {
                power_entity_id: _Response(
                    {"entity_id": power_entity_id, "state": "off"},
                    delay=1,
                )
            }
        )

        with self.assertLogs("app.media_activity", level="WARNING"):
            result = await query_nearby_media_activity(
                (player,),
                access_token="test-token",
                timeout_s=0.001,
                client=client,
                power_entity_id=power_entity_id,
            )

        self.assertIs(result, MediaActivity.UNCERTAIN)
        self.assertEqual(
            [request[1] for request in client.requests],
            [f"http://supervisor/core/api/states/{power_entity_id}"],
        )

    async def test_cancellation_during_power_read_is_not_swallowed(self):
        power_entity_id = "switch.living_room_tv_smart_switch"
        player = "media_player.living_room_tv"
        started = asyncio.Event()
        never = asyncio.Event()

        class _BlockingResponse(_Response):
            async def aiter_bytes(self):
                started.set()
                await never.wait()
                yield self._body

        client = _Client(
            {
                power_entity_id: _BlockingResponse(
                    {"entity_id": power_entity_id, "state": "on"}
                )
            }
        )
        task = asyncio.create_task(
            query_nearby_media_activity(
                (player,),
                access_token="test-token",
                timeout_s=10,
                client=client,
                power_entity_id=power_entity_id,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(client.requests), 1)

    async def test_blank_power_entity_retains_player_only_behavior(self):
        player = "media_player.living_room_tv"
        client = _Client(
            {player: _Response({"entity_id": player, "state": "off"})}
        )

        result = await query_nearby_media_activity(
            (player,),
            access_token="test-token",
            client=client,
            power_entity_id="",
        )

        self.assertIs(result, MediaActivity.CLEAR)
        self.assertEqual(len(client.requests), 1)
        self.assertTrue(client.requests[0][1].endswith(f"/states/{player}"))

    async def test_guard_lazily_creates_and_reuses_one_http_client(self):
        power_entity_id = "switch.living_room_tv_smart_switch"
        player = "media_player.living_room_tv"
        client = _Client(
            {
                power_entity_id: lambda: _Response(
                    {"entity_id": power_entity_id, "state": "off"}
                )
            }
        )
        created_clients = []

        def create_client(**_kwargs):
            created_clients.append(client)
            return client

        fake_httpx = SimpleNamespace(
            AsyncClient=create_client,
            Limits=lambda **kwargs: kwargs,
            Timeout=lambda *args, **kwargs: (args, kwargs),
        )
        guard = NearbyMediaActivityGuard(
            (player,),
            access_token="test-token",
            power_entity_id=power_entity_id,
        )

        with patch.dict(sys.modules, {"httpx": fake_httpx}):
            results = await asyncio.gather(*(guard.check() for _index in range(32)))

        self.assertTrue(all(result is MediaActivity.CLEAR for result in results))
        self.assertEqual(created_clients, [client])
        self.assertEqual(len(client.requests), 32)

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
        invalid_power = await query_nearby_media_activity(
            (entity_id,),
            access_token="test-token",
            client=client,
            power_entity_id="switch.Living_Room",
        )
        missing_client = await query_nearby_media_activity(
            (entity_id,),
            access_token="test-token",
        )

        self.assertIs(duplicate, MediaActivity.UNCERTAIN)
        self.assertIs(invalid_timeout, MediaActivity.UNCERTAIN)
        self.assertIs(invalid_power, MediaActivity.UNCERTAIN)
        self.assertIs(missing_client, MediaActivity.UNCERTAIN)
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

    async def test_guard_passes_fixed_power_entity_to_query(self):
        players = ("media_player.living_room_tv",)
        power_entity_id = "switch.living_room_tv_smart_switch"
        client = _Client({})
        guard = NearbyMediaActivityGuard(
            players,
            access_token="test-token",
            base_url="http://home-assistant.test/api",
            power_entity_id=power_entity_id,
            client=client,
        )

        with patch(
            "app.media_activity.query_nearby_media_activity",
            new=AsyncMock(return_value=MediaActivity.CLEAR),
        ) as query:
            result = await guard.check()

        self.assertIs(result, MediaActivity.CLEAR)
        query.assert_awaited_once_with(
            players,
            access_token="test-token",
            base_url="http://home-assistant.test/api",
            power_entity_id=power_entity_id,
            client=client,
        )


if __name__ == "__main__":
    unittest.main()
