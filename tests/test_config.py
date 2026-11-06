import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bunkrdl.config import default_home


class DefaultHomeTests(unittest.TestCase):
    def _home(self, tmp, env=None):
        with patch("pathlib.Path.home", return_value=Path(tmp)), \
                patch.dict(os.environ, {}, clear=False):
            os.environ.pop("BUNKRDL_HOME", None)
            if env:
                os.environ["BUNKRDL_HOME"] = env
            return default_home()

    def test_env_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(self._home(tmp, str(Path(tmp) / "x")), Path(tmp) / "x")

    def test_fresh_uses_new_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(self._home(tmp), Path(tmp) / ".bunkrdl")

    def test_legacy_kept_when_new_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            legacy = Path(tmp) / ".workhub" / "bunkrdl"
            legacy.mkdir(parents=True)
            (legacy / "state.db").write_bytes(b"x")
            self.assertEqual(self._home(tmp), legacy)
            self.assertEqual((legacy / "state.db").read_bytes(), b"x")

    def test_new_preferred_when_both_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / ".workhub" / "bunkrdl").mkdir(parents=True)
            (Path(tmp) / ".bunkrdl").mkdir()
            self.assertEqual(self._home(tmp), Path(tmp) / ".bunkrdl")


if __name__ == "__main__":
    unittest.main()
