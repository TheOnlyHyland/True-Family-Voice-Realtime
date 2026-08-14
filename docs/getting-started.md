# Getting Started

A complete, from-zero walkthrough. You'll set up two halves:

1. the **backend add-on** (the voice "brain" that runs the OpenAI Realtime session), and
2. the **device firmware** (turns the Voice PE into a thin client that listens and speaks).

Plan ~30–45 minutes the first time. Later firmware updates require deliberately
advancing the two pinned release references after reviewing the new release.

> **The backend 0.22.7 firmware binding is finalized to exact firmware 0.20.2.**
> Update and verify firmware first, then install backend 0.22.7 only from its
> protected published image. Until that image and GitHub release exist, keep
> using released backend 0.22.6. A source checkout is not
> deployable. Roll back backend first and firmware second.

```
Home Assistant Voice PE          Home Assistant (your box)             Cloud
┌──────────────────────────┐   plain WS   ┌────────────────────────┐  WS  ┌─────────────┐
│ custom firmware          │ ───────────▶ │ OpenAI Realtime 2      │ ───▶ │ OpenAI      │
│  (va_client thin client) │ 16k mic up   │  Voice Agent add-on    │ 24k  │ Realtime    │
│  wake word + XMOS DSP    │ ◀─────────── │  (Python / pipecat)    │ ◀─── │ API         │
└──────────────────────────┘ 24k spkr dn  │          │ tools       │      └─────────────┘
                                          ▼          ▼
                                 HA MCP Server (/api/mcp) → controls your home
```

## What you need

- A **Home Assistant Voice PE** device (the firmware is **only** for that hardware).
- **Home Assistant OS** (so you can install add-ons).
- An **OpenAI account** with **billing enabled** (the voice runs on OpenAI's paid API).
- A few minutes at the keyboard, and the device on the **same network** as Home Assistant.

---

## Part 1 — The backend add-on (the brain)

### 1.1 Add the repository & install

1. In Home Assistant: **Settings → Add-ons → Add-on Store**.
2. Top-right **⋮ → Repositories** → paste and add:
   `https://github.com/TheOnlyHyland/True-Family-Voice-Realtime`
3. Find **True Family Voice Realtime** in the store and click **Install**.
   Home Assistant pulls the immutable CI-verified image for this add-on version.

> **One add-on instance serves one device.** For a second Voice PE, see
> [Part 6 — Multiple devices](#part-6--multiple-devices).

### 1.2 Add your OpenAI API key

1. Go to <https://platform.openai.com/> → **API keys** → **Create new secret key**,
   and make sure **billing** is enabled on the account.
2. Open the add-on's **Configuration** tab and paste the key into **`openai_api_key`**.

> New OpenAI accounts start on a low rate-limit tier. If you later see *"Rate limit
> reached"* in the log, raise your usage tier on the OpenAI dashboard, or keep
> `max_context_messages` modest (default 12).

### 1.3 Let it control your home (Home Assistant MCP)

The assistant controls your home through Home Assistant's official, built-in
**[MCP Server](https://www.home-assistant.io/integrations/mcp_server/)** integration —
that's what lets the voice turn your lights, switches, scenes and climate on and off.

1. Add it: **Settings → Devices & Services → Add Integration**, search **"Model Context
   Protocol Server"**, and add it
   ([one-click add](https://my.home-assistant.io/redirect/config_flow_start/?domain=mcp_server)).
2. **Settings → Voice assistants → Expose** → tick the lights, switches, scenes and
   climate you want to control by voice. **Only exposed entities are controllable** —
   this is your safety boundary.
3. In the add-on Configuration, leave **`ha_mcp_url`** and **`longlived_token`**
   **blank**. The add-on then uses Home Assistant's built-in MCP endpoint with its own
   token. (Only fill `longlived_token` if the startup log shows a 401/403 on
   `/core/api/mcp`.)
4. Populate **`mcp_tool_allowlist`** with the complete exact, case-sensitive list
   of MCP tools this deployment needs. Empty now means **no MCP tools**, not all.
   Preserve every desired whole-home tool and exposed custom script; do not trim
   the list merely to match an example.

You get a small fixed set of Assist tools (`HassTurnOn`, `HassTurnOff`, `HassLightSet`,
`GetLiveContext`, `GetDateTime`, …). **`GetLiveContext`** is the "what's the current
state?" tool — keep it; it's what answers *"is the light on?"*.

### 1.4 Minimal configuration

Before the first run, configure all required authority and media fences:

| Option | Value |
|---|---|
| `openai_api_key` | your key |
| `transcription_language` | your ISO code (e.g. `en`, `nl`) — optional but recommended |
| `mcp_tool_allowlist` | the complete exact list of desired MCP and custom-script tools; empty exposes none |
| `nearby_media_power_entity` | `switch.living_room_tv_smart_switch` (optional; safely skips unavailable player reads only when exactly `off`) |
| `nearby_media_players` | `media_player.living_room_tv,media_player.living_room_tv_audio` |

Everything else can wait. The full reference — every option, its default, and when
to change it — is in the [Configuration Reference](configuration.md).

### 1.5 Use only the protected 0.22.7 release artifact

Save the configuration, but do **not** install or start backend 0.22.7 from a
source checkout. Its binding is finalized to firmware 0.20.2, which must be
updated and verified first. Install backend 0.22.7 only after the protected image
and GitHub release exist; otherwise remain on released backend 0.22.6.

---

## Part 2 — The device firmware

This replaces the stock Home Assistant voice pipeline on the Voice PE with a thin
client that streams audio to the add-on. You set it up via a tiny pinned "stub"
config. The pin does not auto-discover later releases.

The firmware lives in its own repo:
**[TheOnlyHyland/True-Family-Voice-Firmware](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware)**.

### 2.1 Install the ESPHome Builder add-on

You build and flash the firmware with the **ESPHome Device Builder** add-on — the
official [ESPHome](https://esphome.io/) tool that runs inside Home Assistant.

1. Open **Settings → Add-ons → Add-on Store**, search **ESPHome Device Builder**, and
   click **Install**
   ([one-click open](https://my.home-assistant.io/redirect/supervisor_addon/?addon=5c53de3b_esphome)).
2. Enable **Show in sidebar**, then **Start** → **Open Web UI**.

### 2.2 Adopt the Voice PE

1. The Voice PE (on its stock firmware) should appear in ESPHome Builder as a
   **discovered device**. If it doesn't, add it by its `home-assistant-voice-xxxx.local`
   address.
2. Click **Adopt**. ESPHome creates a device entry. **Don't install the stock config
   yet.**
3. ESPHome generates an **API encryption key** and an **OTA password** for the device —
   note both; you'll put them in `secrets.yaml` next.

### 2.3 Add your secrets

In ESPHome Builder → **Secrets** (top-right ⋮), add:

```yaml
wifi_ssid: "Your-WiFi"
wifi_password: "your-wifi-password"
ota_password: "the-OTA-password-from-step-2.2"
api_encryption_key: "the-API-encryption-key-from-step-2.2" # 44-char base64

# Optional — ONLY if you want a fixed IP (otherwise the device uses DHCP):
# static_ip: "192.168.1.50"
# gateway:   "192.168.1.1"
# subnet:    "255.255.255.0"
# dns1:      "1.1.1.1"
# dns2:      "1.0.0.1"
```

Two of these confuse people, so to be clear:

- **`api_encryption_key`** is an **ESPHome Noise/API encryption key** — NOT a Home Assistant
  token, NOT your OpenAI key. It's 32 random bytes, base64-encoded. If you flash a
  factory-fresh device (no key from step 2.2), generate your own:
  `openssl rand -base64 32`.
- **`ota_password`** is any password you choose; it protects future over-the-air
  flashes. For an already-adopted device, use the one ESPHome generated so wireless
  updates keep working.

> The firmware itself is pulled from the **public** repo at build time — no token needed.

### 2.4 Paste the device stub & flash

1. In ESPHome Builder, **Edit** the adopted device and **replace its entire YAML** with
   a ready-made stub from the firmware repo:
   - DHCP: [`esphome-builder.dhcp.yaml`](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware/blob/0.20.2/esphome-builder.dhcp.yaml)
   - Fixed IP: [`esphome-builder.static-ip.yaml`](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware/blob/0.20.2/esphome-builder.static-ip.yaml)

   Set `name` and `friendly_name`, and **keep** the `packages:` / `dashboard_import:`
   lines — those are what pull the full firmware from the repo. Keep the device
   `name` stable if you're re-flashing an already-adopted device. Save.

   > Optional: if your add-on isn't reachable at `ws://homeassistant.local:8080/`, add a
   > `va_url:` line under `substitutions:` with your HA host, e.g.
   > `va_url: "ws://192.168.1.x:8080/"`.

   The two `0.20.2` references are immutable and do not discover future releases.
   For an approved update, advance both references to the exact newer tag or
   deliberately use that release's pinned stub.

2. Click **Install →**
   - **First time:** choose **Plug into this computer** — the first flash from stock
     firmware needs the device connected by **USB** to the machine running your browser.
   - **After that:** **Wirelessly (OTA)** — every later flash goes over Wi-Fi.

The device now boots with compatible firmware and waits for the backend.

### 2.5 Start the protected published backend

Only after exact firmware 0.20.2 succeeds, install and start the protected
published backend 0.22.7 image. If that release is not yet available, keep using
released backend 0.22.6 rather than a source checkout. Click **Start** on the
add-on and open the **Log** tab. A healthy start shows
`✅ Fetched N MCP tools` and
`Creating session with N tools` (with `Hass*` names). The add-on listens on port
**8080**, and the device can now connect.

---

## Part 3 — First conversation

1. After boot, the LED ring should settle to **idle** — that means the device
   reached the add-on's WebSocket. The add-on log shows `device (re)connected`.
2. Say **"Hey Leonard"** (the default wake word — switch to Hey Jarvis or Okay Nabu
   any time via the device's **"Wake word" dropdown** in Home Assistant, no reflash)
   → a wake chime plays and the ring shows **listening**.
3. Ask for something you exposed, e.g. *"turn on the bedroom lamp"* → the ring shows
   **thinking** → it acts and replies.
4. Ordinary replies close the mic. In 0.22.7, the assistant may request one
   useful no-wake question at a time and continue again after each genuine
   answer within the same 120-second physical wake. Missing, active, or uncertain
   nearby media keeps each requested window closed while conversation context
   remains available after re-waking.
5. To interrupt a reply: say **"stop"** or press the **center button**.

**If something's off, check the logs:**

- Add-on **Log** tab: bounded assistant-completion metadata, phase/tool names,
  and `🔌 reconnecting` / `✅ reconnected`. Conversation text and tool arguments
  are not logged.
- Device logs: ESPHome Builder → your device → **Logs**.
- Tools missing? Re-check Part 1.3. A 401/403 in the log means set `longlived_token`
  (HA profile → Security).

---

## Part 4 — Updating later

- **Firmware:** review the new firmware release, update both immutable refs in
  your device stub to that exact tag, then compile and flash it over Wi-Fi. The
  pinned `0.20.2` stub deliberately does not advertise moving releases.
- **Add-on:** Home Assistant shows an **Update** badge on the add-on (with a changelog).
  Update firmware first when the release notes require a coordinated protocol
  version, verify it, then update the add-on image.

Your device-specific settings (name, Wi-Fi, IP) live in your stub + `secrets.yaml` and
are **never** overwritten by an update.

---

## Part 5 — Make it yours

Once the basics work, the fun starts. Each of these has a full guide in
[Features](features.md):

- **Persona** — rewrite `instructions` to change personality, language, house rules.
  See [Persona & voices](features.md#persona--voices).
- **Speaker recognition** — set speaker names for the local voice-type heuristic;
  pre-provisioned voice prints are also consumed when present. Backend enrollment
  is not part of the 0.22 rapid pilot. See
  [Speaker recognition](features.md#speaker-recognition).
- **Memory** — first opt in with `enable_voice_memory: true`, then say *"remember
  that the bins go out Thursday"*. See
  [Voice-instructed memory](features.md#voice-instructed-memory).
- **Timers** — set `timer_ring_entity`. See [Voice timers](features.md#voice-timers).
- **Sensors** — set `instance_name` to publish per-device HA sensors. See
  [HA sensors](features.md#ha-sensors).
- **Your own wake word** — use an external offline microWakeWord training workflow
  with your own positive samples and locally flagged false wakes.
- **An agent** — deep recall and background task delegation. See
  [Agent Integration](agent-integration.md).

---

## Part 6 — Multiple devices

One add-on instance serves **one** device. For a second Voice PE:

1. Install a copy of this add-on as a
   [local add-on](https://developers.home-assistant.io/docs/add-ons/tutorial/):
   copy `openai_realtime_voice_agent/` into `/addons`, and change `slug` and `name`
   in its `config.yaml`.
2. Give the copy a different `websocket_port` (e.g. `8082` — avoid `8081`, used by
   dev builds).
3. Point the second device's `va_url` at that port
   (`ws://<ha-host>:8082/` in its firmware stub).

Each instance gets its own configuration — different rooms can have different
personas, voices, and `instance_name` sensor prefixes. Memory
(`/share/voice-memory/`) is shared by all instances.
