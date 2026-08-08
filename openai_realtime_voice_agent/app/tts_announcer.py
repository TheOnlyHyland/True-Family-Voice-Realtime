"""Guarded out-of-band TTS for timers and authenticated announcements."""

import asyncio
import hashlib
import os

import httpx


class DeviceAnnouncer:
    """Synthesize cached prompts and stream 24 kHz PCM to the Voice PE."""

    CHUNK_BYTES = 4800

    def __init__(self, broadcast_bytes, api_key: str, voice: str = "fable"):
        self.broadcast_bytes = broadcast_bytes
        self.api_key = api_key
        self.voice = voice

    async def _tts(self, text: str) -> bytes:
        cache_dir = "/data/announcement_prompts"
        os.makedirs(cache_dir, exist_ok=True)
        key = hashlib.sha256(f"{self.voice}:{text}".encode()).hexdigest()
        path = os.path.join(cache_dir, f"{key}.pcm")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "rb") as cached:
                return cached.read()
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                "https://api.openai.com/v1/audio/speech",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": "gpt-4o-mini-tts",
                    "voice": self.voice,
                    "input": text,
                    "response_format": "pcm",
                    "instructions": (
                        "Calm, composed British butler. Brisk but unhurried."
                    ),
                },
            )
            response.raise_for_status()
            pcm = response.content
        with open(path, "wb") as cached:
            cached.write(pcm)
        return pcm

    async def say(self, text: str) -> None:
        pcm = await self._tts(text)
        for offset in range(0, len(pcm), self.CHUNK_BYTES):
            await self.broadcast_bytes(pcm[offset : offset + self.CHUNK_BYTES])
            await asyncio.sleep(0.095)
