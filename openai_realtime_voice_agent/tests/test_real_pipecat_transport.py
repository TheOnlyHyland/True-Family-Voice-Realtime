"""Run production-faithful transport tests in a fresh, un-stubbed process."""

import importlib.metadata
import subprocess
import sys
import unittest
from pathlib import Path


ADDON_ROOT = Path(__file__).resolve().parents[1]
PROBE = Path(__file__).with_name("real_pipecat_transport_probe.py")


class PinnedPipecatTransportTests(unittest.TestCase):
    def test_real_pipecat_0097_transport_contract(self):
        try:
            version = importlib.metadata.version("pipecat-ai")
        except importlib.metadata.PackageNotFoundError:
            self.skipTest("pipecat-ai is not installed in this test environment")
        self.assertEqual(version, "0.0.97")

        result = subprocess.run(
            [sys.executable, str(PROBE)],
            cwd=ADDON_ROOT,
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )
        self.assertNotIn(" | DEBUG | ", result.stderr)
        self.assertIn(" | INFO | pipecat:", result.stderr)


if __name__ == "__main__":
    unittest.main()
