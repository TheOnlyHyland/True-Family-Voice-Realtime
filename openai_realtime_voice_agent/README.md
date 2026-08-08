# True Family Voice Realtime

Talk to your home with **OpenAI's Realtime** speech-to-speech models. This Home
Assistant add-on runs the realtime voice session and bridges it to Home Assistant
device control (via the official **MCP Server** integration), **web search**,
**voice timers**, **speaker recognition**, opt-in **voice-taught memory**, and
optional **agent integration** for deep recall and
long-running task delegation with voice report-back.

It is the cloud-facing half of a two-part project. The other half is custom
**firmware for the Home Assistant Voice PE** device, which streams microphone audio
to this add-on and plays the reply back. **This add-on is designed for that Voice PE
firmware** (a thin client that talks a small WebSocket protocol); it is not a
drop-in for the stock HA voice pipeline.

> **You need both halves.** This add-on does nothing without the **Voice PE firmware**
> that streams audio to it →
> **[TheOnlyHyland/True-Family-Voice-Firmware](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware)**.

> **0.21 rollout warning:** install firmware 0.19.0 or newer first, verify it,
> and only then start/update this backend to 0.21.0. For rollback, reverse that
> order: backend first, firmware second. Backend-first rollout and firmware-first
> rollback are unsupported. See the repository's `RELEASE.md`.

## What it does

- **Natural voice conversations** (speech in → speech out, no separate STT/TTS
  step) — interrupt mid-sentence; ordinary replies close the mic, while one
  necessary model-requested follow-up may continue without re-waking.
- **Controls Home Assistant** through the official HA *MCP Server* integration —
  lights, switches, scenes, climate, etc., scoped to both the entities you expose
  to Assist and an exact nonempty tool allow-list enforced again at dispatch.
- **Knows who's speaking** — local voice-print identification and speaker-gated
  tools. Existing prints can be consumed, but backend microphone enrollment is
  absent from the 0.21 rapid pilot.
- **Remembers what you teach it when explicitly enabled** — persistent memory is
  off by default, stored locally, and writable only by identified household voices.
- **Voice timers** — personal spoken announcement, then a gentle bell only if
  unacknowledged.
- **Web search** (on by default) — weather, news, facts via a single OpenAI call.
- **Agent-ready** — connect any external agent for instant memory recall and
  background tasks that announce their results in the room that asked.
- **Tunable from the UI** — model, voice, speed, turn detection, selective
  follow-up media guard, language, and more; every option has inline help.

## Quick start

1. Add this repository to Home Assistant (Settings → Add-ons → Add-on Store → ⋮ →
   **Repositories**): `https://github.com/TheOnlyHyland/True-Family-Voice-Realtime`
2. Install **True Family Voice Realtime**, configure it, but do not start 0.21.0 yet.
3. Flash firmware 0.19.0 or newer from
   **[TheOnlyHyland/True-Family-Voice-Firmware](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware)**
   using its pinned ESPHome Builder stub. Later updates require deliberately
   advancing both immutable refs to the approved newer tag.
4. After the firmware update succeeds, install the HA **MCP Server** integration,
   expose the intended entities to Assist, populate the complete exact
   `mcp_tool_allowlist`, set `nearby_media_players` to the Living Room TV and
   Chromecast entity IDs, and start the add-on.

Setup steps are on the **Documentation** tab (`DOCS.md`); the full guides live at
**<https://github.com/TheOnlyHyland/True-Family-Voice-Realtime>**.

## Credits

- Backend forked from **[fjfricke/ha-openai-realtime](https://github.com/fjfricke/ha-openai-realtime)** (Felix Fricke).
- Firmware thin-client design based on **[maxmaxme/home-assistant-voice-pe](https://github.com/maxmaxme/home-assistant-voice-pe)**, a fork of **[esphome/home-assistant-voice-pe](https://github.com/esphome/home-assistant-voice-pe)** (Nabu Casa / ESPHome).
- Inspiration from **[marcinnowak79/home-assistant-voice-pe](https://github.com/marcinnowak79/home-assistant-voice-pe)** (gemini-live-proxy).
- Built on **[pipecat-ai](https://github.com/pipecat-ai/pipecat)**, the **OpenAI Realtime API**, and the official **[Home Assistant MCP Server](https://www.home-assistant.io/integrations/mcp_server/)** integration.
