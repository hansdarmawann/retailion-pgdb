from pathlib import Path
import logging
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from retailion.pipeline import run  # noqa: E402


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(ROOT / "data" / "Sample - Superstore.csv")

