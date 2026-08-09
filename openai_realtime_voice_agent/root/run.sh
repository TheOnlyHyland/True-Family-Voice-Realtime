#!/usr/bin/with-contenv bashio
set -e

# CI uses the image's real CMD and this exact script before publication. Keep
# the smoke ahead of bashio option reads so it needs no Supervisor runtime.
if [ "${TRUE_FAMILY_VOICE_STARTUP_SMOKE:-}" = "1" ]; then
    exec python -m app.main --startup-smoke
fi

# --- 🔑 Basics ---
OPENAI_API_KEY=$(bashio::config 'openai_api_key')
SPEAKER_MALE_NAME=$(bashio::config 'speaker_male_name')
TIMER_RING_ENTITY=$(bashio::config 'timer_ring_entity')
INSTANCE_NAME=$(bashio::config 'instance_name')
SPEAKER_FEMALE_NAME=$(bashio::config 'speaker_female_name')
MALE_ONLY_TOOLS=$(bashio::config 'male_only_tools')
INSTRUCTIONS=$(bashio::config 'instructions')
TRANSCRIPTION_LANGUAGE=$(bashio::config 'transcription_language')

# --- 🗣️ Model & voice ---
OPENAI_MODEL=$(bashio::config 'openai_model')
OPENAI_VOICE=$(bashio::config 'openai_voice')
OPENAI_SPEED=$(bashio::config 'openai_speed')
MAX_OUTPUT_TOKENS=$(bashio::config 'max_output_tokens')

# --- 💬 Conversation ---
FOLLOW_UP_LISTEN_SECONDS=$(bashio::config 'follow_up_listen_seconds')
FOLLOW_UP_OPEN_DELAY_MS=$(bashio::config 'follow_up_open_delay_ms')
WAKE_OPEN_DELAY_MS=$(bashio::config 'wake_open_delay_ms')
VAD_EAGERNESS=$(bashio::config 'vad_eagerness')
PHASE_IDLE_DEBOUNCE_MS=$(bashio::config 'phase_idle_debounce_ms')

# --- 🌐 Web search ---
ENABLE_WEB_SEARCH=$(bashio::config 'enable_web_search')
WEB_SEARCH_MODEL=$(bashio::config 'web_search_model')

# --- 🎚️ Audio ---
PLAYBACK_PREBUFFER_MS=$(bashio::config 'playback_prebuffer_ms')
NOISE_REDUCTION=$(bashio::config 'noise_reduction')

# --- 🏠 Home Assistant ---
HA_MCP_URL=$(bashio::config 'ha_mcp_url')
LONGLIVED_TOKEN=$(bashio::config 'longlived_token')
MCP_TOOL_ALLOWLIST=$(bashio::config 'mcp_tool_allowlist')
NEARBY_MEDIA_PLAYERS=$(bashio::config 'nearby_media_players')
NEARBY_MEDIA_POWER_ENTITY=""
if bashio::config.has_value 'nearby_media_power_entity'; then
    NEARBY_MEDIA_POWER_ENTITY=$(bashio::config 'nearby_media_power_entity')
fi
ENABLE_VOICE_MEMORY=$(bashio::config 'enable_voice_memory')
OPENCLAW_URL=$(bashio::config 'openclaw_url')
ANNOUNCE_PORT=$(bashio::config 'announce_port')
ANNOUNCE_TOKEN=$(bashio::config 'announce_token')

# --- ⚙️ Advanced ---
WEBSOCKET_PORT=$(bashio::config 'websocket_port')
SESSION_REUSE_TIMEOUT_SECONDS=$(bashio::config 'session_reuse_timeout_seconds')
MAX_CONTEXT_MESSAGES=$(bashio::config 'max_context_messages')
TRANSCRIPTION_MODEL=$(bashio::config 'transcription_model')

# --- 🔍 Debug ---
ENABLE_RECORDING=$(bashio::config 'enable_recording')

# Validate required configuration
if [ -z "$OPENAI_API_KEY" ]; then
    bashio::log.error "OPENAI_API_KEY is required but not set"
    exit 1
fi
if [ "$FOLLOW_UP_LISTEN_SECONDS" != "0" ]; then
    bashio::log.error "follow_up_listen_seconds must be 0 for the 0.22.4 rapid pilot"
    exit 1
fi
if [ -z "$NEARBY_MEDIA_PLAYERS" ]; then
    bashio::log.error "nearby_media_players must list media_player.living_room_tv and media_player.living_room_tv_audio for the 0.22.4 rapid pilot"
    exit 1
fi

# Export environment variables
export OPENAI_API_KEY
export SPEAKER_MALE_NAME
export TIMER_RING_ENTITY
export INSTANCE_NAME
export SPEAKER_FEMALE_NAME
export MALE_ONLY_TOOLS
export INSTRUCTIONS
export TRANSCRIPTION_LANGUAGE
export OPENAI_MODEL
export OPENAI_VOICE
export OPENAI_SPEED
export MAX_OUTPUT_TOKENS
export FOLLOW_UP_LISTEN_SECONDS
export FOLLOW_UP_OPEN_DELAY_MS
export WAKE_OPEN_DELAY_MS
export VAD_EAGERNESS
export PHASE_IDLE_DEBOUNCE_MS
export ENABLE_WEB_SEARCH
export WEB_SEARCH_MODEL
export PLAYBACK_PREBUFFER_MS
export NOISE_REDUCTION
export LONGLIVED_TOKEN
export MCP_TOOL_ALLOWLIST
export NEARBY_MEDIA_PLAYERS
export NEARBY_MEDIA_POWER_ENTITY
export ENABLE_VOICE_MEMORY
export OPENCLAW_URL
export ANNOUNCE_PORT
export ANNOUNCE_TOKEN
export WEBSOCKET_PORT
export SESSION_REUSE_TIMEOUT_SECONDS
export MAX_CONTEXT_MESSAGES
export TRANSCRIPTION_MODEL
export ENABLE_RECORDING

# The *_custom escape hatches (🗣️/🌐/⚙️) are optional WITHOUT defaults —
# bashio::config prints "null" for unset optionals, and main.py's
# _resolve_choice would treat that literal string as a real custom value.
# Only export when actually set.
if bashio::config.has_value 'openai_model_custom'; then
    OPENAI_MODEL_CUSTOM=$(bashio::config 'openai_model_custom')
    export OPENAI_MODEL_CUSTOM
fi
if bashio::config.has_value 'openai_voice_custom'; then
    OPENAI_VOICE_CUSTOM=$(bashio::config 'openai_voice_custom')
    export OPENAI_VOICE_CUSTOM
fi
if bashio::config.has_value 'web_search_model_custom'; then
    WEB_SEARCH_MODEL_CUSTOM=$(bashio::config 'web_search_model_custom')
    export WEB_SEARCH_MODEL_CUSTOM
fi
if bashio::config.has_value 'transcription_model_custom'; then
    TRANSCRIPTION_MODEL_CUSTOM=$(bashio::config 'transcription_model_custom')
    export TRANSCRIPTION_MODEL_CUSTOM
fi

# Legacy server_vad saved-config fields (⚙️ Advanced, optional WITHOUT defaults).
# bashio::config prints the string "null" for unset optional keys, which would
# crash main.py's float()/int() parsing — so only export when actually set.
# Unset = mandatory semantic_vad. A saved server_vad selection reaches main.py's
# explicit rapid-pilot startup error.
if bashio::config.has_value 'turn_detection_type'; then
    TURN_DETECTION_TYPE=$(bashio::config 'turn_detection_type')
    export TURN_DETECTION_TYPE
fi
if bashio::config.has_value 'vad_threshold'; then
    VAD_THRESHOLD=$(bashio::config 'vad_threshold')
    export VAD_THRESHOLD
fi
if bashio::config.has_value 'vad_prefix_padding_ms'; then
    VAD_PREFIX_PADDING_MS=$(bashio::config 'vad_prefix_padding_ms')
    export VAD_PREFIX_PADDING_MS
fi
if bashio::config.has_value 'vad_silence_duration_ms'; then
    VAD_SILENCE_DURATION_MS=$(bashio::config 'vad_silence_duration_ms')
    export VAD_SILENCE_DURATION_MS
fi

# The Voice PE's speaker output leaks into its microphone strongly enough to
# trigger Realtime server VAD even with far-field filtering and reduced volume.
# Keep automatic interruption off; local "Stop", the center button, and the
# post-reply follow-up window remain available.
export INTERRUPT_RESPONSE=false

# Removed options (v0.4.29) — no longer exported; main.py env defaults take
# over: backend-owned response creation, ENABLE_DISCONNECT_TOOL=false,
# DEVICE_INPUT_SAMPLE_RATE=16000.

# Export HA_MCP_URL if set (empty string means use default in main.py)
if [ -n "$HA_MCP_URL" ]; then
    export HA_MCP_URL
fi

# SUPERVISOR_TOKEN is automatically provided by Home Assistant when homeassistant_api: true

# Start the application
export PYTHONUNBUFFERED=1
exec python -m app.main
