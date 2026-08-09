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
MEDIA_STATE_TIMEOUT_S = 0.75

_ENTITY_ID_PATTERN = re.compile(r"media_player\.[a-z0-9_]+\Z")
_POWER_ENTITY_ID_PATTERN = re.compile(r"switch\.[a-z0-9_]+\Z")
_ACTIVE_STATES = frozenset({"playing", "buffering", "on", "paused"})
_INACTIVE_STATES = frozenset({"idle", "off", "standby"})


class MediaActivity(str, Enum):
    """Conservative state of the configured nearby-media fence."""

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


def parse_nearby_media_power_entity(value: Any) -> str:
    """Parse one optional, exact Home Assistant switch entity ID."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("nearby_media_power_entity must be a switch entity ID")

    entity_id = value.strip()
    if not entity_id:
        return ""
    if _POWER_ENTITY_ID_PATTERN.fullmatch(entity_id) is None:
        raise ValueError("nearby_media_power_entity must be one switch entity ID")
    return entity_id


async def _read_entity_state(
    client: "httpx.AsyncClient",
    entity_id: str,
    *,
    access_token: str,
    base_url: str,
) -> Optional[str]:
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
                return None

            content_length = response.headers.get("content-length")
            if content_length is not None:
                try:
                    parsed_length = int(content_length)
                    if not 0 <= parsed_length <= MAX_STATE_RESPONSE_BYTES:
                        return None
                except (TypeError, ValueError):
                    return None

            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > MAX_STATE_RESPONSE_BYTES:
                    return None
    except asyncio.CancelledError:
        raise
    except Exception:
        return None

    try:
        payload = json.loads(body)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("entity_id") != entity_id:
        return None

    state = payload.get("state")
    if not isinstance(state, str):
        return None
    return state


async def _read_media_player_state(
    client: "httpx.AsyncClient",
    entity_id: str,
    *,
    access_token: str,
    base_url: str,
) -> MediaActivity:
    """Read and classify one exact nearby media-player state."""
    state = await _read_entity_state(
        client,
        entity_id,
        access_token=access_token,
        base_url=base_url,
    )
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
    power_entity_id: str = "",
) -> MediaActivity:
    """Return clear only when power is off or every player is clearly inactive."""
    if not entity_ids:
        return MediaActivity.UNCERTAIN
    if (
        not access_token
        or timeout_s <= 0
        or not 0 < len(entity_ids) <= MAX_NEARBY_MEDIA_PLAYERS
        or len(set(entity_ids)) != len(entity_ids)
        or any(_ENTITY_ID_PATTERN.fullmatch(entity_id) is None for entity_id in entity_ids)
        or not isinstance(power_entity_id, str)
        or (
            power_entity_id != ""
            and _POWER_ENTITY_ID_PATTERN.fullmatch(power_entity_id) is None
        )
    ):
        return MediaActivity.UNCERTAIN

    if client is None:
        return MediaActivity.UNCERTAIN
    http_client = client

    async def read_scope() -> MediaActivity:
        if power_entity_id:
            power_state = await _read_entity_state(
                http_client,
                power_entity_id,
                access_token=access_token,
                base_url=base_url,
            )
            if power_state == "off":
                return MediaActivity.CLEAR
            if power_state != "on":
                return MediaActivity.UNCERTAIN

        states = await asyncio.gather(
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
        if MediaActivity.ACTIVE in states:
            return MediaActivity.ACTIVE
        if MediaActivity.UNCERTAIN in states:
            return MediaActivity.UNCERTAIN
        return MediaActivity.CLEAR

    try:
        return await asyncio.wait_for(read_scope(), timeout=timeout_s)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning("Nearby media state could not be verified")
        return MediaActivity.UNCERTAIN


class NearbyMediaActivityGuard:
    """Fixed-configuration callable used only by the backend follow-up fence."""

    def __init__(
        self,
        entity_ids: tuple[str, ...],
        *,
        access_token: str,
        base_url: str = DEFAULT_HA_API_BASE_URL,
        power_entity_id: str = "",
        client: Optional["httpx.AsyncClient"] = None,
    ) -> None:
        self._entity_ids = entity_ids
        self._access_token = access_token
        self._base_url = base_url
        self._power_entity_id = power_entity_id
        self._client = client
        self._client_lock = asyncio.Lock()

    async def _get_client(self) -> "httpx.AsyncClient":
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._client is None:
                import httpx

                timeout = httpx.Timeout(
                    MEDIA_STATE_TIMEOUT_S,
                    connect=min(MEDIA_STATE_TIMEOUT_S, 0.5),
                )
                self._client = httpx.AsyncClient(
                    timeout=timeout,
                    limits=httpx.Limits(
                        max_connections=MAX_NEARBY_MEDIA_PLAYERS,
                        max_keepalive_connections=MAX_NEARBY_MEDIA_PLAYERS,
                    ),
                )
        return self._client

    async def check(self) -> MediaActivity:
        client = await self._get_client()
        return await query_nearby_media_activity(
            self._entity_ids,
            access_token=self._access_token,
            base_url=self._base_url,
            power_entity_id=self._power_entity_id,
            client=client,
        )
