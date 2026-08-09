# Changelog

All notable changes to this add-on. Newest first.

## 0.22.0 (source candidate)

- **Not deployable yet:** the exact compatible firmware source commit and
  release-artifact hashes are pending. CI fails closed for every image
  publication and release event until that binding is finalized. Firmware
  `0.19.0` remains regression evidence only.
- Follow-up turns are now serial rather than count-limited. The model may request
  one useful question at a time and rearm only after the current OPEN window is
  consumed by a genuine answer bound to its exact fresh speech-start item and
  transcript, with no round-count limit inside the existing 120-second
  physical-wake ceiling. Repeated-OPEN progression phases carry that
  transaction's token, terminal idle is tokenless, and expired, silently closed,
  queued prior-round, historical speech-item, and recovery-raced authority fails
  closed.
- Added a silent, sole-tool `end_conversation` path for random or unrelated
  answers. The backend briefly holds text, transcript, and PCM from that decision
  response and discards them only when the immutable terminal ledger contains
  exactly one authorized close call. Mixed, pending, late, invalid, or timed-out
  outcomes release normally. It cannot reopen the microphone or act on a stale
  session, wake, answer item, or follow-up token.
- Tool continuation now waits for response A's exact Pipecat source frames,
  chunker work, queued chunks, adapter-owned partial PCM, and active WebSocket
  writes before creating response B. At most one final partial PCM chunk is
  zero-padded and sent. Stop, mute, disconnect, replacement, recovery, timeout,
  processing stall, and write failure discard the old generation immediately;
  cancellation-resistant work, including graceful-finish deadline expiry,
  retires its physical socket before the response grant is released or failure
  returns, without creating the tool continuation response.
- Added stub and real-Pipecat 0.0.97 coverage for strict A-before-B wire order,
  processing/queued/active/partial audio, no-audio completion, ownership loss,
  timeout, physical send failure, cancellation-resistant writes, Pipecat idle
  buffer cleanup, slow-tool phase transitions, cancellation-resistant socket
  close, finish-deadline settlement with a live Pipecat sender,
  recovery-vs-bind races, historical speech replay, and zero-output silent
  terminal decisions.

## 0.21.1 (fork)

- Removed the unsupported top-level `strict` field from the
  `request_follow_up` Realtime session tool. The handler retains exact argument
  validation and rejects every purpose except `necessary_clarification`.

## 0.21.0 (fork)

- **Compatibility warning:** deploy Voice PE firmware 0.19.0 or newer first,
  verify it, and only then deploy backend 0.21.0. Rollbacks must reverse that
  order: backend first, firmware second. Backend-first rollout and
  firmware-first rollback are unsupported.
- Backend CI now requires the exact firmware `0.19.0` source tag and immutable
  release package. It validates the final manifest, factory, OTA, ELF, and
  `SHA256SUMS` hashes; missing or mismatched external inputs cannot be skipped in
  CI, while standalone local tests may use the committed protocol fixture.
- MCP authority is now fail-closed and checked at dispatch: an exact tool name
  must be present in both the administrator's `mcp_tool_allowlist` and the current
  Realtime session schema. Empty exposes no MCP tools. Deployment must carry
  forward the complete desired whole-home and custom-script list explicitly.
- Microphone enrollment is de-scoped from the rapid pilot. Enrollment protocol,
  capture, conductor, configuration, and sensor controls are absent; the backend
  only consumes voice prints that an administrator provisioned offline.
- Persistent voice memory is gated by `enable_voice_memory` and defaults off.
  Disabled sessions expose no memory tools and do not read the memory file.
- Selective follow-up startup now requires a nonempty fixed nearby-media scope;
  this deployment must configure the Living Room TV and Chromecast. The default
  output cap is now a finite 1200 tokens.
- Production logging now redacts every known configured OpenAI, Supervisor, Home
  Assistant, announce, and OpenClaw credential spelling, including MCP exception
  messages and bearer headers.
- Ordinary replies now close the microphone by default. In managed semantic-VAD
  mode, the model can call a strict internal `request_follow_up` tool whose only
  accepted purpose enum is `necessary_clarification`, and only for one necessary
  question.
- At most one no-wake follow-up can be accepted per physical device wake. The
  budget resets only on a new device wake and stays spent across additional
  turns, socket replacement, OpenAI replay/recovery, stop, mute, timeout,
  competing responses, and other tool calls.
- Requested follow-ups are bound to the exact tool continuation, response audio,
  playback boundary, admitted device socket, session nonce, wake generation, and
  unpredictable token. Opening is now two-phase: PREPARE ACK, firmware READY with
  a fresh nonce, final media check, COMMIT ACK, then microphone audio. Stop, mute,
  recovery, replacement, late audio, and uncertain cancellation fail closed.
- Added `nearby_media_players`, a fixed administrator-supplied list of up to 16
  exact `media_player` entity IDs. The add-on checks them internally through the
  authenticated Home Assistant REST boundary before reservation and again at the
  firmware READY boundary. Active, paused, uncertain, unavailable, denied, malformed,
  or timed-out state keeps the microphone closed without discarding conversation
  context. No model-callable state tool was added.
- Version 0.21.0 requires `follow_up_listen_seconds: 0`, managed semantic VAD,
  backend-owned response creation, and bounded context. Saved nonzero legacy mode now fails
  startup with a clear error rather than silently opening after every reply.
- Added a project-owned Pipecat 0.0.97 single-owner transport adapter. Raw
  candidates receive no assistant audio, can send only the exact hello receipt,
  cannot displace an admitted owner, and delayed old disconnects cannot clear a
  replacement socket. Scheduled connection handlers now require explicit bounded
  completion, and failed physical closes enter a bounded uncertain-socket quarantine.
- Control JSON now rejects duplicate keys before dispatch. Trusted phase controls
  carry the exact session nonce and wake generation, while the exact unbound
  0.20.6 legacy shape remains documented. Semantic-VAD `thinking` now closes PCM
  admission immediately while retaining the logical wake and one-shot follow-up
  ownership needed for a model-authorized second turn.
- Reconnect, device Stop, follow-up timeout, and firmware `client_revoke` now use
  one exact generation-fenced OpenAI input-buffer clear and wait for its receipt.
  Future wake, mic, and output paths remain closed while it settles. A failed
  clear enters recovery and retires the physical socket before another wake can
  advance. Phase sends remain serialized with wake/session transitions, and
  assistant PCM is bound to the exact socket, wake, OpenAI response ID, and
  response generation.
- CI now installs the Poetry-locked production dependency closure and runs fresh-
  process transport tests against the real Pipecat 0.0.97 package. The vendored
  contract asserts exact control field arrays; normal CI requires exact firmware
  source and package alignment.
- The production image now copies one lock-built virtual environment from a
  multi-stage builder. Docker and CI verify the same lock digest and pinned
  Pipecat, NumPy, and Loguru versions. Loguru is configured before Pipecat imports
  at INFO or higher with bounded records and redacted conversation/tool payloads.
- Release images now wheel-install the application so `python -m app.main` works
  under `PYTHONSAFEPATH`, pin each Home Assistant base by digest, freeze Debian
  package indexes and direct package versions, pin Poetry and Poetry Core, and
  verify the TitaNet source and digest. CI builds each architecture once, runs
  amd64 natively and arm64 through QEMU by image ID, then publishes that unchanged
  gated image behind `backend-production`. Release events only verify existing
  immutable tags. A Dockerless rootfs-layout smoke is included for local
  verification, GitHub Actions are commit-pinned, and publication refuses to
  overwrite an existing version or source tag.
- Assistant PCM ownership is retained outside Pipecat frames through the real
  0.0.97 chunker. Every reconstructed chunk is bound to its exact response
  generation and physical socket; ownership transitions drain queued/partial
  old audio before a replacement can receive output.
- OpenClaw secret-path URLs are redacted from standard and Loguru records, web
  search no longer logs answer content, and `announce_token` is password-masked.
- The rapid pilot remains unauthenticated plaintext and is supported only on a
  trusted isolated LAN; protocol nonces provide freshness, not authentication or
  encryption.

## 0.20.6 (fork)

- Enable Python safe-path mode and use a non-root working directory so NLTK can
  import standard-library modules after its upstream import-security update.

## 0.20.5 (fork)

- `max_context_messages` now retains complete user-led turns instead of slicing
  arbitrary local messages. Expired turns are deleted from OpenAI before the next
  response, with tool calls and matching outputs kept indivisible.
- Recent complete turns are silently replayed after the hourly Realtime reconnect
  without generating speech or rerunning tools. If replay cannot be proven safe,
  the replacement session starts with an empty conversation.
- Input transcription now runs with automatic language detection when context
  bounding is enabled. User transcripts remain only in the bounded in-memory
  window and are no longer written to the add-on log.
- Bounded history requires `semantic_vad`. Existing `server_vad` configurations
  remain startable by automatically disabling managed history with a warning.
- Tool calls now use generation-fenced scheduled, running, and result-drain
  barriers. A reconnect, replay, or explicit stop cannot dispatch an abandoned
  handler or emit an orphaned tool continuation.
- Explicit stop now removes the interrupted turn in place, quarantines every
  remaining item in that response, suppresses its audio and text, confirms its
  server-side deletion, and waits for response-cancel settlement before allowing
  the replacement response. The replacement wake and microphone audio are not
  discarded, including when a tool was pending.
- Generic Realtime receive-loop errors now enter explicit reconnect recovery;
  blank or failed managed transcripts and non-replayable terminal turns fail
  fresh instead of leaving local and server history divergent.
- Response IDs, cancel event IDs, input-clear acknowledgements, Pipecat function
  ownership, and assistant response-end processing are generation-fenced so a
  delayed old response cannot complete or contaminate a replacement turn.
- Started tool actions finish atomically while interrupted callbacks are
  swallowed. Tool execution is serialized, with bounded admission for a newer
  action instead of allowing contradictory mutations to interleave.
- Dangling server-VAD boundaries now fail closed through an immediate in-place
  Realtime reconnect. Old-session audio, items, transcripts, tools, and response
  completions remain suppressed until the replacement session is ready.

## 0.20.4 (fork)

- OpenAI reconnects now succeed only after the API acknowledges `session.updated`
  and the receive loop is alive. Failed attempts remain in recovery with capped
  exponential backoff instead of logging a false success and leaving Jarvis deaf.
- Recovery explicitly suppresses deferred response creation, so reconnecting
  cannot produce unsolicited speech.

## 0.20.3 (fork)

- Closing replies now answer the person naturally instead of describing the
  conversation mechanism. Voice PE close-stage acknowledgements have a
  three-second deadline with explicit transaction diagnostics, avoiding false
  failures when the device main loop responds just after the old one-second cap.

## 0.20.2 (fork)

- Fixed the dead-turn watchdog cancelling itself before its `idle` phase could
  reach the Voice PE, preventing a stalled response from leaving the device on
  the white thinking spinner indefinitely.

## 0.20.1 (fork)

- Standalone thanks after a completed action now closes the conversation. The
  redundant two-second post-tool cooldown was removed; generation-bound mixed
  tool cancellation remains authoritative.

## 0.20.0 (fork)

- Added graceful `end_conversation` handling: the model can mark a clearly
  finished exchange so the Voice PE plays the complete final reply and then
  skips only the next automatic follow-up window without disconnecting. Paired
  True Family Voice firmware `0.18.0` or newer is required; older firmware
  safely rejects the close because it cannot send the required acknowledgement.

## 0.19.0 (fork)

- Added the authoritative `turn_on_room_lights` tool with strict approved-room
  mappings, ordered mixed-Zigbee ON sequences, and non-retry failure reporting.

## 0.18.0 (fork)

- Added the strictly read-only `get_calendar_events` voice tool for 11 approved
  Home Assistant calendars, with explicit time bounds, a 31-day range cap,
  20-event output limit, minimal event fields, and structured non-retry errors.

## 0.16.7 (fork)

- **Wedge watchdog**: a half-open OpenAI socket (dies silently during an idle
  gap — no close frame, no error) used to swallow the next request entirely:
  audio streamed out, nothing came back, no reply. Now every wake arms a 12 s
  liveness check; if the server VAD shows no activity, the session reconnects
  in place (~3 s). A silent wake triggers a harmless idle-time reconnect.

## 0.16.6 (fork)

- **Fixed: direct `ask_openclaw` silently rebinding to the HA MCP path.**
  pipecat registers a handler for every MCP tool during session creation,
  which overwrote the native direct-path handler — resurrecting the 60-second
  MCP cap ("it failed" while the task actually succeeded). Native registration
  now happens after MCP registration and wins.
- **Announce endpoint repeat guard**: near-duplicate messages within 10
  minutes are accepted but not spoken (`duplicate_suppressed`), so an agent
  monitoring for a result can't re-announce the same news every poll cycle.

## 0.16.5 (fork)

- **Voice prints now build automatically** when enrollment completes — the
  coach confirms out loud, warns when the enrolled name isn't in
  `speaker_male_name`/`speaker_female_name` (recognition stays inactive until
  it is), and asks for a retry when there wasn't enough clear speech.
  Previously this required a manual `python3 -m app.build_voiceprint` step
  that was easy to miss, leaving enrollments silently ineffective.
- New `sensor.voicepe_<instance>_voice_prints`: enrolled prints, with an
  `active` attribute showing which are enrolled *and* configured.

## 0.16.4 (fork)

- **Cost observability**: every response's exact token usage (from the API's
  `response.done`) is logged with an estimated cost, and a
  `sensor.voicepe_<instance>_openai_cost_today` sensor tracks daily spend in
  Home Assistant. Rates auto-switch for mini models.
- Recommended default applied to our install: `max_output_tokens: 1200` —
  output audio is the dominant per-turn meter ($64/1M tokens, measured); a cap
  bounds runaway monologues without touching normal replies.

## 0.16.3 (fork)

- Documentation overhaul: marketing README, `docs/` (getting started,
  configuration reference, features, agent integration, FAQ); repository
  renamed to `voicepe-realtime` (old URLs redirect). `repository.json` now
  carries this project's identity (was still the upstream fork's).
- `enrollment_phrase` default is now "hey leonard" (matches the shipped
  default wake word); HA UI help text added for all fork options.

## 0.16.2 (fork)

- **Guaranteed report-back on long delegations**: ask_openclaw now sends the
  instance name as `room`; the bridge answers "still working" at 120s instead
  of killing the turn, and delivers the agent's eventual answer to that room's
  announce endpoint itself. Previously a >145s research task was reported as
  a failure by voice while the agent kept working with nowhere to deliver.

## 0.16.1 (fork)

- **`recall_memory` tool** (with `openclaw_url`): instant deterministic search
  of the agent's memory files via the bridge (`{"recall": query}` →
  `{"matches": [...]}`). Registered as the FIRST stop for personal/household
  recall; `ask_openclaw` becomes the deep fallback. Fixes recall being a
  40-80s agent turn that found or missed facts depending on phrasing.

## 0.16.0 (fork)

- **Announce endpoint** (`announce_port` + `announce_token` options): a LAN
  route back to the device for the household's external agent. POST
  `/announce {"message": "..."}` (bearer-authed) speaks the message through
  the device's guarded TTS lane — the same path timers use — so a delegated
  task ("research X") can report back by voice minutes later. Disabled unless
  both options are set; 503 when no device is connected.

## 0.15.1 (fork)

- **Direct OpenClaw escalation** (`openclaw_url` option): `ask_openclaw` now
  calls the bridge endpoint directly instead of going through HA's MCP server,
  whose hardcoded 60-second request timeout killed longer agent turns (deep
  memory recall, contact lookups). Direct calls get ~2.5 minutes. Unset, the
  MCP-script path is used unchanged. The speaker gate applies either way.
- (0.10–0.15.0 entries — speaker voice-prints, timers, enrollment v2, HA
  sensors, false-wake flagging, voice-instructed memory — are in git history.)

## 0.9.0 (fork)

- **Firmware-backed voice enrollment** (pairs with firmware commit 5095ed0+):
  the device enters a true enrollment mode — mic pinned open, wake/stop models
  disarmed, cyan breathing LED, 10-minute hard cap, center button as physical
  escape — while an automated audio coach (gpt-4o-mini-tts prompts, cached,
  pushed down the speaker lane on a fixed schedule) guides 25 varied wake-phrase
  repetitions plus 90 s of natural speech. Mic audio flows ONLY to the recorder
  during enrollment: OpenAI hears nothing, so no VAD commits, no forced
  responses, no cost, no conversation mechanics to fight. New options:
  `enrollment_phrase`, `enrollment_tts_voice`.

## 0.8.0 (fork)

- **Voice enrollment**: say "I want to teach you my voice" — the assistant runs
  a guided recording session (varied wake-phrase repetitions + natural speech)
  via the new `voice_enrollment` tool, capturing the raw device mic stream to
  `/share/voice-enrollment/<person>_<timestamp>.wav` (16 kHz mono, 15-minute
  safety cap, persists across rebuilds). One session yields wake-word training
  positives AND voice-print enrollment audio. Recordings are personal data and
  are not managed by the add-on beyond writing the file.

## 0.7.1 (fork)

- Speaker probe tuned for real device audio (live test found 3-7 voiced frames
  in actual speech vs 100+ on synthetic bench audio): YIN threshold 0.15 → 0.20
  with a moderate-periodicity argmin fallback, energy gate 0.15 → 0.08 of peak
  RMS, minimum voiced frames 12 → 8, capture window 2.5 s → 3.0 s. Synthetic
  bench unchanged (0% wrong on typical voices).
- Debug: when `enable_recording` is on, each probe capture is saved to
  `recordings/probe_*.wav` for offline threshold calibration.

## 0.7.0 (fork)

- **Speaker context v1**: optional voice-type (male/female) detection for a
  two-person household. On every wake the first ~2.5 s of command audio is
  classified by median pitch (pure numpy YIN, in-process, off the event loop;
  benched at 98.6% right / 0% wrong across 11 typical synthetic voices) and the
  verdict is injected into the Realtime session as a system item, so the
  assistant can address the speaker by name ("sir"/"ma'am") and hedge when
  uncertain. New options: `speaker_male_name`, `speaker_female_name` (both
  empty = feature off).
- **Speaker-gated tools**: `male_only_tools` (comma-separated tool names) are
  enforced below the model — gated tools return a polite refusal unless the
  last voice verdict is the male speaker. Fails closed on uncertain/stale
  verdicts. Convenience gating, not biometric auth.

## 0.6.0

> ⚠️ **This update has two parts — please update both:**
> 1. **This add-on** (the update you're installing now).
> 2. **The Voice PE firmware** — open **ESPHome Device Builder** and click **Update** (or **Install**) on your device.
>
> The device and the add-on use one shared protocol; updating only one half can cause odd behaviour.

A reliability and voice-control polish release.

**Stop word**

- **Saying "stop" now usually works on the first try.** The spoken "stop" could
  previously be answered by the assistant a moment later, so you sometimes had to
  repeat it; that follow-on reply is now cancelled, so a single "stop" is
  typically enough.
- **Saying "stop" during a web search returns the device to rest promptly** — the
  light ring no longer keeps showing the "replying" animation for several seconds.
- **Fewer accidental stops** on the assistant's own speech.
- The light ring briefly flashes **red** to confirm your "stop" was registered. *(firmware)*

**Reliability**

- **No more unresponsive sessions.** A silently dropped connection to OpenAI is
  now detected and repaired within seconds, instead of leaving the assistant deaf
  until a restart.
- **The roughly hourly reconnect now happens proactively during a quiet moment**,
  so it practically never interrupts a conversation.
- **Smart-home commands are no longer cancelled** if you keep talking while they run.
- The light can no longer get **stuck on "thinking"**, and long web searches get
  all the time they need.

**No more "answers out of nowhere"**

- The assistant no longer occasionally replies — or repeats its previous answer —
  right after the wake word when you said nothing.
- A sentence that got cut off is no longer answered minutes later on your next wake.

**Settings**

- New **"Wake mic delay"** setting: a short pause after the wake chime before the
  mic opens, so the chime can't be mistaken for speech (default 700 ms).
- The **"Follow-up mic delay"** default is now **700 ms**. Existing installs keep
  their saved value — raise yours if the assistant ever answers right after its
  own reply.

## 0.5.0

A big stable release: everything built and tested on the dev channel over the
past days. **Also update the Voice PE firmware** (v1.1.0 via ESPHome Builder) to
get the full effect of the "stop" improvements; the two halves
work best together.

- **"Stop" now works through the whole reply AND the after-reply listening
  window.** The device detects the word more reliably, and the bridge treats
  it as authoritative: in-flight audio is discarded and an answer OpenAI had
  already started for the stop word itself is cancelled on arrival — no more
  "Okay, I'll be quiet" replies to your "stop".
- **Fixed: an answer could cut off mid-sentence, after which the assistant
  went deaf** until the next reconnect. Harmless protocol races (e.g. your
  sentence being split into two turns by a pause) no longer kill the session.
- **Fixed an audio race that could inject noise/hiss into replies** (firmware,
  paired with this release).
- **Mute behaves properly now** (firmware): the ring goes dark with red
  markers by the microphones, and muting also ends an open listening window
  immediately — both from Home Assistant and with the physical side switch.
- **The LED Ring switch in Home Assistant works again** (firmware): entity off
  = device dark at rest; entity on = the gentle "ready" pulse.
- **Completely reworked Configuration tab**: options grouped logically
  (Basics → Model & voice → Conversation → Web search → Audio →
  Home Assistant → Advanced), every description rewritten in plain practical
  language, and a full Dutch translation included (shown automatically when
  your HA is set to Dutch). Confusing or broken switches were removed; rarely
  needed expert fields stay hidden until you need them.
- **The add-on now has its own icon.**
- Friendlier defaults for new installs: follow-up mic delay 200 ms and
  playback buffer 150 ms. **Existing installs keep their saved values** — if
  yours still say 0, consider setting 200/150 manually (Conversation / Audio
  groups) for fewer ghost triggers and less crackle.

### Heads-up: the firmware stub template was improved

The per-device stub in ESPHome Builder used to reference the firmware in a
form that lets ESPHome **cache the downloaded YAML for a day** — clicking
Update shortly after a release could then silently rebuild yesterday's code.
The stub templates in the firmware repo are fixed; existing users can apply
the same fix once by replacing **only the `packages:` block** in their
device's YAML in ESPHome Builder (everything else — your name, secrets,
`dashboard_import` — stays exactly the same):

```yaml
packages:
  realtime:
    url: https://github.com/TheOnlyHyland/True-Family-Voice-Firmware
    ref: "0.19.0"
    files:
      - home-assistant-voice.realtime.yaml
    refresh: 0s
```

Current templates for reference:
[esphome-builder.dhcp.yaml](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware/blob/0.19.0/esphome-builder.dhcp.yaml) ·
[esphome-builder.static-ip.yaml](https://github.com/TheOnlyHyland/True-Family-Voice-Firmware/blob/0.19.0/esphome-builder.static-ip.yaml)

## 0.4.26

- **Web search is now ON by default**, using **gpt-5.5** (the best-quality search
  model), so the assistant can look things up online — weather, news, facts — out
  of the box. **Existing installs keep their saved setting**: if you had it off,
  switch `enable_web_search` on (and set `web_search_model` to `gpt-5.5`) in the
  add-on Configuration. The cheaper mini/nano models stay available.

## 0.4.25

- **Fix:** the first thing you said in the few seconds right after an automatic
  reconnect (e.g. after the 60-minute session cap) could be ignored
  (`conversation_already_has_active_response`). The reconnected session no longer
  creates a duplicate response, so that turn answers normally.

## 0.4.24

- **Renamed** to **OpenAI Realtime 2 Voice Agent**.
- Rewrote the store/info description and added a full **Documentation** tab
  (install steps, OpenAI key, Home Assistant MCP setup, recommended settings, web
  search, credits). Removed stale text from the original upstream client.
- Default system prompt is now an English, voice-tuned prompt (silent tool calls,
  varied confirmations, language pinning). Your own saved prompt is not changed.
- Default `follow_up_open_delay_ms` and `playback_prebuffer_ms` set to `0` (raise
  them if the device hears its own tail or you hear crackle).

## 0.4.23

- **Fix:** the 60-minute session cap sometimes left the session dead until a
  restart. It now reconnects automatically in all cases (both the keepalive-drop
  and the `session_expired` forms).

## 0.4.22

- **New options:** voice **speed** (0.25–1.5), **max reply length**
  (`max_output_tokens`), and **input noise reduction** (off / near-field /
  far-field). All default to current behaviour.

## 0.4.21

- Model, voice, web-search-model and transcription-model options are now
  **dropdowns** with the known-good values, each with a **custom** entry if you
  need a value not in the list.

## 0.4.20

- **New:** optional **web search**. Turn on `enable_web_search` to let the
  assistant look things up online (weather, news, facts). Uses your OpenAI key;
  off by default. Model configurable via `web_search_model` (default gpt-5.4-mini).

## 0.4.19

- Clarified the MCP option help text for both the built-in HA MCP Server and the
  unofficial ha-mcp add-on.

## 0.4.18

- **Fix:** removed a meaningless filler reply ("I'm ready to continue…") that could
  appear on the first turn of a session.

## 0.4.17

- **Fix:** cap restored conversation history (`max_context_messages`, default 12) to
  bound per-turn token cost and avoid hitting OpenAI's rate limit.

## 0.4.16

- **Fix:** the device no longer gets stuck blinking "thinking" after a turn-ending
  error (e.g. a rate limit) — it returns to idle so you can retry.

## 0.4.14

- **New:** `playback_prebuffer_ms` jitter buffer to reduce occasional crackle at the
  start of replies.

## 0.4.12 – 0.4.13

- **Fix:** "say stop, then immediately ask again → silence". Disabled the broken
  server-side audio truncation that wedged the next turn.

## 0.4.9 – 0.4.11

- **New:** auto-reconnect the OpenAI Realtime session when its connection drops
  (keepalive timeout / 60-minute cap), instead of going dead until a restart.
  Refined so a normal device disconnect doesn't trigger an unnecessary reconnect.

## 0.4.6 – 0.4.8

- **New:** configurable post-reply **follow-up listening window** (answer back
  without re-saying the wake word) + its open-delay, and per-option help text in the
  UI.
- **New:** the assistant's and user's transcripts are logged to the add-on log
  (`🤖 assistant:` / `🗣️ user:`).

## 0.4.0 – 0.4.4

- **Fix:** resample the device's 16 kHz mic to the 24 kHz OpenAI requires (garbled
  speech), and drop empty audio chunks.
- **New:** device **"stop"** interrupt now actually cancels the reply and clears
  buffered audio.

## 0.3.x

- Switched the target to **gpt-realtime-2**, pinned pipecat-ai 0.0.97, and tuned
  turn detection (semantic VAD), phase delivery to the device, and the startup
  sequence to stop double-responses. Made the disconnect tool and transcription
  model configurable.

## Earlier

- Initial pipecat + WebSocket implementation (forked from
  [fjfricke/ha-openai-realtime](https://github.com/fjfricke/ha-openai-realtime)).
