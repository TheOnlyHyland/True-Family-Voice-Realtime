"""Production logging privacy and bounds tests."""

import os
import logging
import sys
import types
import unittest
from unittest.mock import Mock, patch

_loguru_stub = types.ModuleType("loguru")
setattr(_loguru_stub, "logger", Mock())
sys.modules.setdefault("loguru", _loguru_stub)

from app import logging_config


class LoggingConfigTests(unittest.TestCase):
    def test_sensitive_pipecat_payload_is_replaced_and_exception_removed(self):
        record = {
            "name": "pipecat.services.openai.realtime.llm",
            "message": 'function arguments: {"secret":"spoken value"}',
            "exception": object(),
        }

        self.assertTrue(logging_config.sanitize_loguru_record(record))
        self.assertEqual(
            record["message"],
            "Pipecat event redacted (sensitive payload)",
        )
        self.assertIsNone(record["exception"])
        self.assertNotIn("spoken value", record["message"])

    def test_normal_loguru_message_is_bounded(self):
        record = {
            "name": "pipecat.transport",
            "message": "x" * (logging_config.MAX_LOG_MESSAGE_CHARS + 100),
            "exception": None,
        }

        logging_config.sanitize_loguru_record(record)

        self.assertLessEqual(
            len(record["message"]),
            logging_config.MAX_LOG_MESSAGE_CHARS,
        )
        self.assertTrue(record["message"].endswith(" [truncated]"))

    def test_debug_request_is_clamped_and_default_sink_is_removed(self):
        fake_loguru = Mock()
        previous_configured = logging_config._configured
        logging_config._configured = False
        try:
            with (
                patch.object(logging_config, "_loguru_logger", fake_loguru),
                patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}),
            ):
                logging_config.configure_production_logging()
        finally:
            logging_config._configured = previous_configured

        fake_loguru.remove.assert_called_once_with()
        self.assertEqual(fake_loguru.add.call_args.kwargs["level"], "INFO")
        self.assertFalse(fake_loguru.add.call_args.kwargs["backtrace"])
        self.assertFalse(fake_loguru.add.call_args.kwargs["diagnose"])

    def test_openclaw_secret_url_is_removed_from_loguru_and_exceptions(self):
        secret_url = "https://agent.local/hooks/private-secret?token=also-secret"
        with patch.dict(os.environ, {"OPENCLAW_URL": secret_url}):
            record = {
                "name": "httpx",
                "message": f"POST {secret_url} failed",
                "exception": ValueError(secret_url),
            }
            logging_config.sanitize_loguru_record(record)
            formatted = logging_config._BoundedRedactingFormatter(
                "%(levelname)s %(message)s"
            ).format(
                logging.LogRecord(
                    "app.openclaw_tool",
                    logging.ERROR,
                    __file__,
                    1,
                    "bridge failed: %r",
                    (ValueError(secret_url),),
                    None,
                )
            )

        self.assertNotIn("private-secret", record["message"])
        self.assertNotIn("also-secret", record["message"])
        self.assertNotIn("private-secret", formatted)
        self.assertNotIn("also-secret", formatted)
        self.assertIn("<redacted-openclaw-url>", record["message"])
        self.assertIn("<redacted-openclaw-url>", formatted)

    def test_normalized_openclaw_path_query_and_fragment_are_redacted(self):
        secret_url = (
            "https://agent.local/hooks/private%2Fpath?token=also%2Fsecret"
            "#room%2Fkey"
        )
        rendered_variant = (
            "POST /hooks/private/path?token=also/secret#room/key failed"
        )

        with patch.dict(os.environ, {"OPENCLAW_URL": secret_url}):
            sanitized = logging_config._sanitize_message(rendered_variant)

        for secret in ("private/path", "also/secret", "room/key"):
            self.assertNotIn(secret, sanitized)
        self.assertIn("<redacted-openclaw-url>", sanitized)

    def test_all_known_configured_tokens_are_redacted(self):
        secrets = {
            "OPENAI_API_KEY": "openai-private-value",
            "SUPERVISOR_TOKEN": "supervisor-private-value",
            "LONGLIVED_TOKEN": "ha-private-value",
            "ANNOUNCE_TOKEN": "announce-private-value",
            "OPENCLAW_TOKEN": "openclaw-private-value",
        }
        rendered = " ".join(secrets.values())

        with patch.dict(os.environ, secrets, clear=True):
            sanitized = logging_config._sanitize_message(rendered)

        for value in secrets.values():
            self.assertNotIn(value, sanitized)
        self.assertEqual(sanitized.count("<redacted-credential>"), len(secrets))

    def test_mcp_exception_and_bearer_header_cannot_emit_tokens(self):
        supervisor = "mcp-supervisor-private-value"
        ha_token = "mcp-ha-private-value"
        formatter = logging_config._BoundedRedactingFormatter(
            "%(levelname)s %(message)s"
        )

        with patch.dict(
            os.environ,
            {
                "SUPERVISOR_TOKEN": supervisor,
                "LONGLIVED_TOKEN": ha_token,
            },
            clear=True,
        ):
            formatted = formatter.format(
                logging.LogRecord(
                    "app.mcp_service",
                    logging.ERROR,
                    __file__,
                    1,
                    "MCP failed with %r and Authorization: Bearer %s",
                    (ValueError(supervisor), ha_token),
                    None,
                )
            )

        self.assertNotIn(supervisor, formatted)
        self.assertNotIn(ha_token, formatted)
        self.assertIn("<redacted-credential>", formatted)


if __name__ == "__main__":
    unittest.main()
