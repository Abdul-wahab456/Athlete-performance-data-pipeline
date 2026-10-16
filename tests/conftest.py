import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import sql_helpers  # noqa: E402

sql_helpers.VERBOSE = False  # keep test output short
