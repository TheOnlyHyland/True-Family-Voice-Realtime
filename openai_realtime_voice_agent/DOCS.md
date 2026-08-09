# Voice PE Realtime — Add-on Documentation

This add-on runs an **OpenAI Realtime** voice session (default model
`gpt-realtime-2.1`) and bridges it to explicitly allow-listed Home Assistant
control, web search, voice timers, speaker recognition, and opt-in persistent
voice-taught memory. It is the
backend half of a two-part project; the front half is custom **firmware for the
Home Assistant Voice PE** device (see
[Firmware](#firmware-home-assistant-voice-pe-only) below).

```
Voice PE device  ──WebSocket──▶  this add-on  ──▶  OpenAI Realtime API
(mic up / speaker down)               │  tools
                                      ▼
                             HA MCP Server (/api/mcp)  → controls your home
```

**Full documentation** lives in the project repository — feature guides, the
complete option reference, agent integration, and FAQ:
<https://github.com/TheOnlyHyland/True-Family-Voice-Realtime/tree/main/docs>.
This page covers setup and day-to-day essentials.

> **The backend 0.22.3 firmware binding is finalized to exact firmware 0.20.0.**
> Update and verify firmware first, then install only the protected published
> backend image. Until that image and GitHub release exist, continue using
> released backend 0.21.1. A source checkout is not deployable. Rollback remains
> backend first and firmware second.

---

## 1. Install the add-on

1. In Home Assistant go to **Settings → Add-ons → Add-on Store**.
2. Top-right **⋮ → Repositories**, add:
   `https://github.com/TheOnlyHyland/True-Family-Voice-Realtime`
3. Find **True Family Voice Realtime** in the store and click **Install**.
   Home Assistant pulls the CI-verified image for this exact add-on version.
4. Open the add-on's **Configuration** tab to set it up. For 0.22.3, update exact
   firmware 0.20.0 first and install only the protected published backend image,
   never a source checkout.

**One add-on instance serves one Voice PE device.** For multiple devices, run one
instance per device (a local-add-on copy with its own `slug`, `name` and
`websocket_port`) — see the
[Getting Started guide](https://github.com/TheOnlyHyland/True-Family-Voice-Realtime/blob/main/docs/getting-started.md#part-6--multiple-devices).

## 2. Get an OpenAI API key

1. Go to <https://platform.openai.com/> → **API keys** → **Create new secret key**.
2. Make sure **billing** is set up on your OpenAI account — Realtime audio and web
   search are paid usage.
3. Paste the key into the add-on's **`openai_api_key`** option.

> **Heads-up on rate limits:** new accounts start at a low tokens-per-minute (TPM)
> tier. Realtime audio is token-heavy, so if you see *"Rate limit reached"* in the
> logs, raise your usage tier in the OpenAI dashboard, or keep
> `max_context_messages` modest (default 12).

## 3. Let it control Home Assistant (MCP)

The assistant controls your home through Home Assistant's **official MCP Server**.

1. In HA: **Settings → Devices & Services → Add Integration → "Model Context
   Protocol Server"** and add it.
2. **Expose the entities** you want voice control over to **Assist**
   (Settings → Voice assistants → *Exposed entities*). The MCP server only offers
   what's exposed.
3. In the add-on, leave **`ha_mcp_url`** **blank** — it then uses the built-in
   endpoint (`http://supervisor/core/api/mcp`) with the add-on's own token. Leave
   **`longlived_token`** blank too, unless startup logs a 401/403 on
   `/core/api/mcp` (then paste a HA long-lived token there).

You get a small fixed set of Assist tools (`HassTurnOn`, `HassTurnOff`,
`HassLightSet`, `GetLiveContext`, `GetDateTime`, …). **`GetLiveContext`** is the
"what's the current state?" tool — keep it; it's what answers *"is the light on?"*.

**`mcp_tool_allowlist` is required.** Populate it with every exact,
case-sensitive Home Assistant and custom-script tool this deployment should keep.
Empty exposes no MCP tools. The backend intersects the configured list with the
current session schema and checks the exact name again at dispatch, so do not
replace your complete deployment list with a shortened documentation example.

## 4. Recommended starting settings

**The defaults are the recommended settings** — for a first run you only need the
API key, the MCP integration (section 3), and ideally your language. The
Configuration tab is grouped: **🔑 Basics → 🗣️ Model & voice → 💬 Conversation →
🌐 Web search → 🎚️ Audio → 🏠 Home Assistant → ⚙️ Advanced → 🔍 Debug**, and every
option has plain-language inline help.

| Option | Default | Note |
|---|---|---|
| `openai_model` | `gpt-realtime-2.1` | newest speech-to-speech model |
| `openai_voice` | `marin` | `marin`/`cedar` are the newest voices |
| `max_output_tokens` | `1200` | finite answer and cost bound |
| `transcription_language` | *(blank)* | auto-detects for bounded context replay; set an ISO code (e.g. `nl`) to lock it |
| `instructions` | *(English default)* | the system prompt; swap the LANGUAGE line for your language |
| `follow_up_listen_seconds` | `0` | ordinary replies close; the model may request one useful no-wake question at a time and continue again after each genuine answer, without a round-count limit inside the 120-second physical wake |
| `mcp_tool_allowlist` | *(blank: no MCP tools)* | must contain the complete desired exact tool list before deployment |
| `nearby_media_power_entity` | *(blank: player-only checks)* | for this home use `switch.living_room_tv_smart_switch`; exact `off` skips unavailable players, exact `on` verifies them, and uncertain power fails closed |
| `nearby_media_players` | *(blank: startup blocked)* | for this home use `media_player.living_room_tv,media_player.living_room_tv_audio`; active, paused, missing, or uncertain state suppresses a requested open-mic window |
| `follow_up_open_delay_ms` | `700` | echo guard before the follow-up mic opens; lower = snappier but risks ghost turns |
| `wake_open_delay_ms` | `700` | the same echo guard right after the wake chime |
| `vad_eagerness` | `low` | waits longest before deciding you're done talking |
| `playback_prebuffer_ms` | `150` | raise to ~250 if you hear crackle; 0 = play immediately |
| `max_context_messages` | `12` | complete user-led turns retained; version 0.22.3 requires a value above `0` |
| `enable_web_search` | `true` | online lookups; set `false` to disable |
| `enable_voice_memory` | `false` | explicit opt-in for persistent memory tools and memory-file reads |
| `web_search_model` | `gpt-5.5` | best-quality search model; mini/nano are cheaper |

> **Rapid-pilot limitation:** the device WebSocket is unauthenticated plaintext.
> Protocol nonces reject stale transactions; they are not device authentication
> or encryption. Keep the service on a trusted isolated LAN and never expose its
> WebSocket port to an untrusted network or the internet.

The legacy `server_vad` turn-detection fields live at the bottom of ⚙️ Advanced and
only appear when you enable **"Show unused optional configuration options"**.
Leave them unset: version 0.22.3 requires managed `semantic_vad` and rejects
`server_vad` at startup.

The **complete option reference** (every option, purpose, default, when to change
it) is in the
[Configuration Reference](https://github.com/TheOnlyHyland/True-Family-Voice-Realtime/blob/main/docs/configuration.md).

## 5. Web search

When **`enable_web_search`** is on (**the default**), the assistant gets a `web_search`
tool. When it needs current or general info (weather, news, facts), it calls that
tool; the add-on then makes a **second, server-side OpenAI call** (the Responses API
`web_search` built-in tool, on **`web_search_model`**) and reads a short spoken
answer back.

- Uses your **existing OpenAI key** — no extra account.
- Default model `gpt-5.5` (best quality). Cheaper options trade price/quality
  (`gpt-5.4`, `gpt-5-mini`, the nano models, …) — a few cents per search.
- Adds ~1–3 s while it searches (the device shows "thinking").
- If the model name is rejected, the assistant just says it couldn't search — it
  won't crash the session, so you can change `web_search_model` and retry.

## 6. Voice timers

Set, cancel and list timers by voice. On expiry: one personal spoken announcement
(addressed to whoever set the timer), a 20-second grace window (any wake counts as
acknowledged), then a gentle bell only if unacknowledged — silenced by the center
button or "stop".

Setup: set **`timer_ring_entity`** to your device's exposed
`switch.<device>_timer_ringing` entity. Without it, the assistant will say timers
are unavailable. Timers survive the hourly session refresh but not add-on restarts.

## 7. Speaker awareness

Set `speaker_male_name` / `speaker_female_name` and each wake is tagged with the
likely speaker (pitch heuristic for a one-male-one-female household). Existing,
administrator-provisioned voice prints can provide per-person identity. The
assistant can address people by name, and
`male_only_tools` (comma-separated tool names) are enforced below the model — a
gated tool politely refuses unless the gated voice was identified. Convenience,
not biometric security. Leave names empty to disable.

**Backend enrollment is not available in 0.22.3.** The add-on exposes no model,
device, configuration, or administrator control that can start microphone
enrollment. The rapid pilot only consumes prints that an administrator already
provisioned under `/share/voice-prints/`; creating new reference audio and prints
is an offline task outside this release.

Full guide:
[Speaker recognition](https://github.com/TheOnlyHyland/True-Family-Voice-Realtime/blob/main/docs/features.md#speaker-recognition).

## 8. Voice-instructed memory

First set `enable_voice_memory: true`. Then say "remember that..." / "from now
on..." and the note becomes a standing
instruction in every future conversation (it takes effect at the next session —
minutes, at most an hour). "Forget about..." removes matching notes; "what do you
remember" reads them back. Notes are stored locally in
`/share/voice-memory/memory.md` (plain markdown — you can edit it by hand), capped
at 60 notes, each attributed to the household member whose voice gave it. Guests
and unidentified voices cannot change memory.
With the default `false`, the tools are absent and the memory file is not read.

## 9. Agent integration (optional)

**`openclaw_url`**: direct endpoint for an external agent
(`POST {"question", "room"}` → `{"answer"}`). When set, the add-on registers the
`ask_openclaw` escalation tool natively and calls the endpoint directly with a
~2.5-minute timeout — bypassing Home Assistant's hard 60-second MCP request cap
that kills long agent turns. You also get **`recall_memory`**: the bridge answers
`{"recall": "<query>"}` with `{"matches": [...]}` — instant, deterministic recall
(contacts, dates, preferences) with the full agent turn as fallback.
Treat `openclaw_url` as a secret if its path contains a hook token; the backend
redacts that configured URL and path from application and HTTP-client logs.

**`announce_port` + `announce_token`** (set both): a LAN route *back to the
device*. `POST http://<ha-host>:<announce_port>/announce` with
`Authorization: Bearer <announce_token>` and body `{"message": "..."}` speaks the
message aloud through the device's guarded TTS lane — this is what lets a
delegated task report back by voice minutes later. Returns 503 when no device is
connected, so callers can fall back to a text channel. Generate a long random
token; the add-on runs on the host network, so the token is the lock. The
Configuration tab masks `announce_token` as a password.

The integration is agent-agnostic — any agent behind a small bridge works. Full
contracts and examples:
[Agent Integration](https://github.com/TheOnlyHyland/True-Family-Voice-Realtime/blob/main/docs/agent-integration.md).

## 10. False-wake flagging & HA sensors

Every wake's opening audio is archived locally (auto-pruned, newest 500). Flag a
false trigger by saying *"that was a false alarm"*, **double-pressing the center
button**, or automatically when a wake is silenced without speech. Labeled
captures remain local hard negatives for a deliberate external offline
wake-word retraining workflow.

Set **`instance_name`** (e.g. `kitchen`) to publish
`sensor.voicepe_kitchen_speaker`, `_active_timers`, `_wakes_today`,
`_false_wakes_today`, `_openai_cost_today`, and `_voice_prints` for dashboards
and automations.

## 11. Reading the logs

The add-on log shows bounded assistant-completion metadata, `📞 phase ->`
(device state), tool names, and `🔌 …reconnecting` / `✅ reconnected` on a
connection recovery. Conversation text, user payloads, and tool arguments are
not logged. View it on the add-on **Log** tab.

## Known limitations

- **A brief reconnect about once an hour.** OpenAI limits a realtime session to
  60 minutes. The add-on refreshes proactively during a quiet moment, so you'll
  rarely notice it, but a reconnect can occasionally cause a ~1–2 second pause.
- **Timers don't survive add-on restarts** (they do survive the hourly session
  refresh).
- **Rarely, the assistant may stop itself** on a word in its own reply that sounds
  like "stop" — just ask again.

**Using "stop":** say "stop" (or press the center button) to interrupt the assistant
*while it's speaking* — during a reply, or during the short listening window right
after one. It has no effect before the assistant has started answering (there's
nothing to stop yet).

## Firmware (Home Assistant Voice PE only)

This add-on expects the custom **Voice PE firmware** that turns the device into a
thin client (it streams mic audio here and plays the reply). That firmware:

- is **specific to the Home Assistant Voice PE** hardware (ESP32-S3 + XMOS), and
- lives in its own **public** repository:
  **[TheOnlyHyland/True-Family-Voice-Firmware](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware)**.

You flash it via a tiny per-device stub in ESPHome Builder. Its two immutable
release refs do not auto-discover updates; advance both deliberately only after
reviewing the newer release. The full from-scratch guide is the
[Getting Started guide](https://github.com/TheOnlyHyland/True-Family-Voice-Realtime/blob/main/docs/getting-started.md).

## Credits

- Backend forked from **[fjfricke/ha-openai-realtime](https://github.com/fjfricke/ha-openai-realtime)** (Felix Fricke).
- Firmware thin-client design based on **[maxmaxme/home-assistant-voice-pe](https://github.com/maxmaxme/home-assistant-voice-pe)**, a fork of **[esphome/home-assistant-voice-pe](https://github.com/esphome/home-assistant-voice-pe)** (Nabu Casa / ESPHome).
- Inspiration from **[marcinnowak79/home-assistant-voice-pe](https://github.com/marcinnowak79/home-assistant-voice-pe)** (gemini-live-proxy).
- Built on **[pipecat-ai](https://github.com/pipecat-ai/pipecat)**, the **OpenAI Realtime API**, and the official **[Home Assistant MCP Server](https://www.home-assistant.io/integrations/mcp_server/)** integration.
