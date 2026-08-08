"""Bounded internal Home Assistant media-state guard for voice follow-ups."""

import asyncio
import json
import logging
import re
from enum import Enum
from typing import Any, Optional, TYPE_CHECKING


if TYPE_CHECKING:
    import httpx


logger = logging.getLogger(__name__)

DEFAULT_HA_API_BASE_URL = "http://supervisor/core/api"
MAX_NEARBY_MEDIA_PLAYERS = 16
MAX_STATE_RESPONSE_BYTES = 16 * 1024
MEDIA_STATE_TIMEOUT_S = 1.25

_ENTITY_ID_PATTERN = re.compile(r"media_player\.[a-z0-9_]+\Z")
_ACTIVE_STATES = frozenset({"playing", "buffering", "on", "paused"})
_INACTIVE_STATES = frozenset({"idle", "off", "standby"})


class MediaActivity(str, Enum):
    """Conservative aggregate state of configured nearby media players."""

    CLEAR = "clear"
    ACTIVE = "active"
    UNCERTAIN = "uncertain"


def parse_nearby_media_players(value: Any) -> tuple[str, ...]:
    """Parse one bounded, duplicate-free comma-separated media-player list."""
    if value is None or value == "":
        return ()
    if not isinstance(value, str):
        raise ValueError("nearby_media_players must be a comma-separated string")

    entities = tuple(part.strip() for part in value.split(",") if part.strip())
    if len(entities) > MAX_NEARBY_MEDIA_PLAYERS:
        raise ValueError(
            f"nearby_media_players cannot contain more than {MAX_NEARBY_MEDIA_PLAYERS} entities"
        )
    if len(set(entities)) != len(entities):
        raise ValueError("nearby_media_players cannot contain duplicates")
    if any(_ENTITY_ID_PATTERN.fullmatch(entity_id) is None for entity_id in entities):
        raise ValueError(
            "nearby_media_players must contain only media_player entity IDs"
        )
    return entities


async def _read_media_player_state(
    client: "httpx.AsyncClient",
    entity_id: str,
    *,
    access_token: str,
    base_url: str,
) -> MediaActivity:
    """Read and strictly normalize one exact Home Assistant state response."""
    url = f"{base_url.rstrip('/')}/states/{entity_id}"
    try:
        async with client.stream(
            "GET",
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
        ) as response:
            if type(response.status_code) is not int or response.status_code != 200:
                return MediaActivity.UNCERTAIN

            content_length = response.headers.get("content-length")
            if content_length is not None:
                try:
                    parsed_length = int(content_length)
                    if not 0 <= parsed_length <= MAX_STATE_RESPONSE_BYTES:
                        return MediaActivity.UNCERTAIN
                except (TypeError, ValueError):
                    return MediaActivity.UNCERTAIN

            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > MAX_STATE_RESPONSE_BYTES:
                    return MediaActivity.UNCERTAIN
    except asyncio.CancelledError:
        raise
    except Exception:
        return MediaActivity.UNCERTAIN

    try:
        payload = json.loads(body)
    except (TypeError, ValueError, UnicodeDecodeError):
        return MediaActivity.UNCERTAIN
    if not isinstance(payload, dict) or payload.get("entity_id") != entity_id:
        return MediaActivity.UNCERTAIN

    state = payload.get("state")
    if not isinstance(state, str):
        return MediaActivity.UNCERTAIN
    if state in _ACTIVE_STATES:
        return MediaActivity.ACTIVE
    if state in _INACTIVE_STATES:
        return MediaActivity.CLEAR
    return MediaActivity.UNCERTAIN


async def query_nearby_media_activity(
    entity_ids: tuple[str, ...],
    *,
    access_token: str,
    base_url: str = DEFAULT_HA_API_BASE_URL,
    timeout_s: float = MEDIA_STATE_TIMEOUT_S,
    client: Optional["httpx.AsyncClient"] = None,
) -> MediaActivity:
    """Return active/uncertain unless every configured player is clearly inactive."""
    if not entity_ids:
        return MediaActivity.UNCERTAIN
    if (
        not access_token
        or timeout_s <= 0
        or not 0 < len(entity_ids) <= MAX_NEARBY_MEDIA_PLAYERS
        or len(set(entity_ids)) != len(entity_ids)
        or any(_ENTITY_ID_PATTERN.fullmatch(entity_id) is None for entity_id in entity_ids)
    ):
        return MediaActivity.UNCERTAIN

    owns_client = client is None
    if client is None:
        import httpx

        timeout = httpx.Timeout(timeout_s, connect=min(timeout_s, 0.5))
        http_client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(
                max_connections=MAX_NEARBY_MEDIA_PLAYERS,
                max_keepalive_connections=MAX_NEARBY_MEDIA_PLAYERS,
            ),
        )
    else:
        http_client = client

    async def read_all() -> list[MediaActivity]:
        return await asyncio.gather(
            *(
                _read_media_player_state(
                    http_client,
                    entity_id,
                    access_token=access_token,
                    base_url=base_url,
                )
                for entity_id in entity_ids
            )
        )

    try:
        try:
            states = await asyncio.wait_for(read_all(), timeout=timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Nearby media state could not be verified")
            return MediaActivity.UNCERTAIN
    finally:
        if owns_client:
            try:
                await http_client.aclose()
            except Exception:
                logger.warning("Nearby media HTTP client could not close cleanly")

    if MediaActivity.ACTIVE in states:
        return MediaActivity.ACTIVE
    if MediaActivity.UNCERTAIN in states:
        return MediaActivity.UNCERTAIN
    return MediaActivity.CLEAR


class NearbyMediaActivityGuard:
    """Fixed-configuration callable used only by the backend follow-up fence."""

    def __init__(
        self,
        entity_ids: tuple[str, ...],
        *,
        access_token: str,
        base_url: str = DEFAULT_HA_API_BASE_URL,
    ) -> None:
        self._entity_ids = entity_ids
        self._access_token = access_token
        self._base_url = base_url

    async def check(self) -> MediaActivity:
        return await query_nearby_media_activity(
            self._entity_ids,
            access_token=self._access_token,
            base_url=self._base_url,
        )
