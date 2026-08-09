# Configuration Reference

Two places hold configuration:

- **The add-on** — the Configuration tab in Home Assistant. Options are grouped:
  🔑 Basics → 🗣️ Model & voice → 💬 Conversation → 🌐 Web search → 🎚️ Audio →
  🏠 Home Assistant → ⚙️ Advanced → 🔍 Debug. Every option has inline help.
- **The firmware** — substitutions in your per-device stub in ESPHome Builder.

> The add-on UI renders every text option as a single-line input. For long text like
> `instructions`, use the Configuration tab's **⋮ → Edit in YAML** for a real editor.

> The `*_custom` fields and legacy `server_vad` fields are hidden until you toggle
> **"Show unused optional configuration options"** at the bottom of the tab.
> Version 0.22.1 requires the default `semantic_vad`; selecting `server_vad`
> blocks startup.

> **Version 0.22.1 is bound to exact firmware 0.20.0.** Update and verify that
> firmware first, then install only the protected published backend image. A
> source checkout is not a release artifact. Rollback remains backend first and
> firmware second.

## 🔑 Basics

| Option | Default | Purpose / when to change |
|---|---|---|
| `openai_api_key` | *(empty)* | Your OpenAI key (`sk-...`), created at platform.openai.com with billing enabled. Everything — listening, thinking, speaking, web search — runs on this key. **Required.** |
| `instructions` | English voice-tuned prompt | The system prompt: personality, language, house rules. Write it like you'd brief a person. See [Persona & voices](features.md#persona--voices) for what it can and can't change. |
| `transcription_language` | *(empty)* | Two-letter ISO code (`en`, `nl`, `de`, …). Setting it pins the private in-memory transcript language; empty auto-detects. User transcripts are not logged. |

## 🗣️ Model & voice

| Option | Default | Purpose / when to change |
|---|---|---|
| `openai_model` | `gpt-realtime-2.1` | The speech-to-speech model. Choices include `gpt-realtime-2.1` (newest), `gpt-realtime-2`, `gpt-realtime-1.5`, `gpt-realtime-mini`, `gpt-realtime`, or `custom`. |
| `openai_model_custom` | *(hidden)* | Any valid Realtime model id, used when `openai_model` is `custom`. Expert escape hatch. |
| `openai_voice` | `marin` | The voice it speaks with. `marin`/`cedar` are the newest and most natural; also `alloy`, `ash`, `ballad`, `coral`, `echo`, `sage`, `shimmer`, `verse`. Restart the add-on after changing — a running conversation keeps its voice. |
| `openai_voice_custom` | *(hidden)* | Any valid OpenAI voice name, used when `openai_voice` is `custom`. |
| `openai_speed` | `1.0` | Speaking pace, `0.25`–`1.5`. Changes pace only, not the words. |
| `max_output_tokens` | `1200` | Finite cap on answer length (≈ 0.75 words per token). The default bounds runaway replies and cost without affecting normal answers; too low can cut an answer off mid-sentence. |

## 💬 Conversation

| Option | Default | Purpose / when to change |
|---|---|---|
| `follow_up_listen_seconds` | `0` | Must remain `0` in version 0.22.1. Ordinary replies close the mic. The model may request one useful no-wake question at a time and, after each genuine answer, request another without a round-count limit inside the existing 120-second physical wake. A saved nonzero value fails startup instead of enabling legacy automatic mode. |
| `follow_up_open_delay_ms` | `700` | Echo guard: pause between the end of a reply and the mic re-opening, so the speaker's tail can't become a ghost question. Lower (300–500) is snappier but risks the assistant answering its own echo — raise it back if it "answers nobody" or repeats itself. |
| `wake_open_delay_ms` | `700` | The same echo guard after the wake chime, before the mic opens. Lower for a snappier wake; raise if a wake sometimes triggers an answer to nothing. |
| `vad_eagerness` | `low` | How quickly it decides you're done talking. `low` waits patiently (best if you pause mid-sentence), `high` answers faster but may cut you off, `auto` lets OpenAI decide. |
| `phase_idle_debounce_ms` | `1500` | How long the assistant must stay silent before the device counts the answer as finished. Bridges pauses between sentences so the LED and "stop" keep working through long answers. Raise if the device flips to idle mid-answer. |

## 🌐 Web search

| Option | Default | Purpose / when to change |
|---|---|---|
| `enable_web_search` | `true` | Online lookups (weather, news, facts). Each lookup is one extra OpenAI call on your key (a few cents) and adds ~1–3 s. Set `false` to disable. |
| `web_search_model` | `gpt-5.5` | The model that searches and summarises. `gpt-5.5` is best quality; `gpt-5.4`, `gpt-5`, mini and nano variants are cheaper but miss more. |
| `web_search_model_custom` | *(hidden)* | Any model supporting OpenAI's `web_search` tool, when set to `custom`. |

## 🎚️ Audio

| Option | Default | Purpose / when to change |
|---|---|---|
| `playback_prebuffer_ms` | `150` | How much of the answer to buffer before playing — absorbs Wi-Fi jitter (the start-of-reply crackle) at the cost of that much reply latency. Raise to ~250 if you hear crackle; `0` = play immediately. |
| `noise_reduction` | `off` | Extra input filtering before OpenAI. Usually leave `off` — the device's XMOS chip already filters. Try `near_field` (talking close) or `far_field` (across the room) if it mishears in noise. |

## 🏠 Home Assistant & speakers

| Option | Default | Purpose / when to change |
|---|---|---|
| `speaker_male_name` | *(empty)* | Name to use when a male voice is detected. Leave both name fields empty to disable speaker detection. |
| `speaker_female_name` | *(empty)* | Name to use when a female voice is detected. |
| `male_only_tools` | *(empty)* | Comma-separated tool names that only execute for the male voice. Enforced below the model — it can't be talked around. Convenience gating, not biometric security. |
| `timer_ring_entity` | *(empty)* | The device's exposed `switch.<device>_timer_ringing` entity. Empty = voice timers unavailable (the assistant will say so). |
| `instance_name` | *(empty)* | Sensor prefix, e.g. `kitchen` → `sensor.voicepe_kitchen_*`. Also sent to your agent as the `room` for report-backs. Empty = `device`. |
| `ha_mcp_url` | *(empty)* | Leave empty (recommended): uses HA's built-in MCP Server integration. Only set a URL if you run the separate ha-mcp add-on. |
| `longlived_token` | *(empty)* | Leave empty (recommended): the add-on uses its own supervisor permission. Only paste a long-lived token (HA profile → Security) if startup logs a 401/403 on `/core/api/mcp`. |
| `mcp_tool_allowlist` | *(empty: no MCP tools)* | Required comma-separated list of exact, case-sensitive MCP tool names. Empty fails closed. To preserve whole-home behavior, populate the complete desired list before deployment, including every Home Assistant control/read tool and exposed custom script currently in use; do not replace that deployment list with a shortened example. A call is also checked against the tools exposed in the current session at dispatch time. |
| `nearby_media_power_entity` | *(empty: player-only checks)* | Optional exact lowercase `switch` entity ID that powers the nearby players. For this home, set `switch.living_room_tv_smart_switch`. Exact readable `off` returns clear without querying players that are unavailable while unpowered. Exact `on` requires every configured player to pass the existing strict inactive-state check. Missing, denied, malformed, unknown, unavailable, or timed-out power state keeps the mic closed. The backend never controls this switch or exposes it to the model. |
| `nearby_media_players` | *(empty: startup blocked)* | Required list of up to 16 exact `media_player` entity IDs near this Voice PE. For this home, set `media_player.living_room_tv,media_player.living_room_tv_audio`. Before a requested no-wake follow-up, the backend checks only these entities through authenticated HA REST, then checks again after firmware READY while the mic is still closed and before COMMIT. When the optional power switch is blank or exactly `on`, playing, buffering, on, paused, unavailable, unknown, denied, malformed, timed-out, or empty scope keeps the mic closed. The list is not exposed as a model tool. |
| `enable_voice_memory` | `false` | Explicit privacy opt-in for the persistent `remember`, `forget`, and `list_memories` tools and for loading `/share/voice-memory/memory.md` into model instructions. Disabled sessions neither expose those tools nor read the file. |
| `openclaw_url` | *(empty)* | Direct endpoint of your agent bridge. Enables the `ask_openclaw` escalation tool (called directly, ~2.5-minute budget, bypassing HA MCP's 60 s cap) and the instant `recall_memory` tool. Contract in [Agent Integration](agent-integration.md). |
| `announce_port` | `0` | Port for the announce endpoint — a LAN route back to the device so an agent can speak in the room. Enabled only when **both** this and `announce_token` are set. |
| `announce_token` | *(empty)* | Password-masked bearer token for the announce endpoint. The add-on runs on the host network, so the token is the lock — generate a long random one and treat it as a secret. |

> **Rapid-pilot network limitation:** the Voice PE WebSocket protocol is
> unauthenticated plaintext. Nonces prevent stale transaction reuse but do not
> authenticate a device or encrypt traffic. Run it only on a trusted, isolated
> LAN; do not expose the WebSocket port to untrusted networks or the internet.

> **Accepted inherited privileges:** 0.22.1 still uses host networking, the Home
> Assistant API credential, and read-write `/share`. Those process privileges are
> not a model authorization policy. MCP authority remains limited to the exact
> configured allow-list and the exact tool schema exposed in the current session.

## ⚙️ Advanced

| Option | Default | Purpose / when to change |
|---|---|---|
| `websocket_port` | `8080` | The port the Voice PE connects to. Must match the `va_url` in the device firmware. Change only on a port clash (and for second devices — see [multi-device](getting-started.md#part-6--multiple-devices)); `8081` is used by dev builds. |
| `session_reuse_timeout_seconds` | `300` | If the device reconnects within this window after a Wi-Fi blip, the conversation resumes. A full add-on restart starts fresh. |
| `max_context_messages` | `12` | Complete user-led turns retained across the hourly OpenAI reconnect; tool calls and results stay together. Version 0.22.1 requires a value above `0` so a requested conversational turn survives a fresh wake. |
| `transcription_model` | `gpt-4o-transcribe` | Creates private bounded text for reconnect replay. Does **not** affect understanding — the main model hears your audio natively. Also: `gpt-realtime-whisper`, `gpt-4o-mini-transcribe`, `whisper-1`. |
| `transcription_model_custom` | *(hidden)* | Custom transcription model id. |
| `turn_detection_type` | *(unset)* | Leave unset. The 0.22.1 rapid pilot requires managed `semantic_vad`; selecting legacy `server_vad` blocks startup. |
| `vad_threshold` | *(unset)* | Legacy `server_vad` saved-config field. Leave unset; using `server_vad` blocks 0.22.1 startup. |
| `vad_prefix_padding_ms` | *(unset)* | Legacy `server_vad` saved-config field. Leave unset; using `server_vad` blocks 0.22.1 startup. |
| `vad_silence_duration_ms` | *(unset)* | Legacy `server_vad` saved-config field. Leave unset; using `server_vad` blocks 0.22.1 startup. |

## 🔍 Debug

| Option | Default | Purpose / when to change |
|---|---|---|
| `enable_recording` | `false` | Saves mic and speaker audio to files inside the add-on, for troubleshooting only. Also saves speaker-probe captures for offline threshold calibration. Leave off normally. |

Backend microphone enrollment is deliberately absent from the 0.22 rapid pilot.
There are no enrollment options or model, device, or administrator start controls.

---

## Firmware substitutions

Set these in your per-device stub in ESPHome Builder (the stub overrides the
firmware's defaults). The secrets (`wifi_ssid`, `wifi_password`, `ota_password`,
`api_encryption_key`) are passed as substitutions because a remote package can't use
`!secret` directly.

| Substitution | Default | Purpose / when to change |
|---|---|---|
| `name` | `home-assistant-voice` | The device's ESPHome name. **Keep it stable** across re-flashes of an adopted device. |
| `friendly_name` | `Home Assistant Voice` | Display name in Home Assistant. |
| `wifi_ssid` / `wifi_password` | *(from secrets)* | Your Wi-Fi credentials. |
| `ota_password` | *(from secrets)* | Protects over-the-air flashes. Use the one ESPHome generated at adopt time (or pick one on a fresh flash). |
| `api_encryption_key` | *(from secrets)* | ESPHome Noise/API encryption key — 32 random bytes, base64 (`openssl rand -base64 32`). Not an HA token, not your OpenAI key. |
| `va_url` | `ws://homeassistant.local:8080/` | WebSocket endpoint of the backend add-on. Change if your HA host has a different name/IP or the add-on uses another port, e.g. `ws://192.168.1.x:8082/`. No auth token — it's a LAN-local service. |
| `wake_word_model` | `models/hey_leonard.json` (this repo) | The microWakeWord model URL. Point it at your own trained model, or at a stock model. Runtime switching between the built-in options needs no reflash — use the "Wake word" dropdown in HA. |
| `default_wake_word` | `Hey Leonard` | Which entry of the HA "Wake word" dropdown is selected on first boot (`Hey Leonard`, `Hey Jarvis`, `Okay Nabu`). |
| `wake_cutoff_slight` / `wake_cutoff_moderate` / `wake_cutoff_very` | `217` / `178` / `140` | The three sensitivity tiers of the HA "Wake word sensitivity" select, as quantized uint8 probability cutoffs (`round(p × 255)`; **lower = more sensitive** = more false accepts). Custom-trained models ship calibrated values — override these with your model's calibration. |
| `hidden_ssid` | `false` | Set `"true"` if your Wi-Fi SSID is hidden. |
| `timer_finished_sound_file` | `sounds/gentle_timer.flac` (this repo) | The timer bell — a gentle two-tone bell (~-14 dBFS) replacing the more intense stock ring. Point at any FLAC/MP3 URL to change it (the other `*_sound_file` substitutions swap the stock chimes the same way). |
| `static_ip` / `gateway` / `subnet` / `dns1` / `dns2` | *(DHCP)* | Only with the static-IP stub (`esphome-builder.static-ip.yaml`) — pins a fixed LAN IP. |

Two firmware-side controls live in Home Assistant, not in YAML:

- **"Wake word" dropdown** — Hey Leonard / Hey Jarvis / Okay Nabu, switched at
  runtime, no reflash.
- **"Wake word sensitivity" select** — Slightly / Moderately / Very sensitive,
  applying the calibrated cutoffs above.
