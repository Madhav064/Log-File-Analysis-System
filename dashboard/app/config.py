"""Settings, from environment variables. DATA_DIR is the folder holding the exported serving JSON."""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


class Config:
    DATA_DIR = os.environ.get("DATA_DIR", str(REPO_ROOT / "data" / "serving"))
    GUIDE_PATH = os.environ.get("GUIDE_PATH", str(REPO_ROOT / "docs" / "dashboard-guide.md"))
