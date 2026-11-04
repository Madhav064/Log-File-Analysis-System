"""The ONLY module that reads the serving JSON (plan M8a). Everything else asks it for rows.

Each file is loaded once and cached. The cache is dropped when manifest.json changes (a new export), so a
re-run of `pipeline.py export` shows up without restarting the dashboard.
"""
import json
import threading
from pathlib import Path

MANIFEST = "manifest.json"
EXPORT_HINT = "run `python scripts/pipeline.py export` to produce the dashboard's data"


class DataMissing(Exception):
    """A serving file does not exist (yet). The API turns this into a 503 with a hint."""

    def __init__(self, name, reason="not exported"):
        super().__init__(f"{name}: {reason}; {EXPORT_HINT}")
        self.name = name


class Repository:
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self._lock = threading.Lock()
        self._stamp = None
        self._cache = {}

    def _check_fresh(self):
        """Drop the cache when the manifest was rewritten by a new export."""
        path = self.data_dir / MANIFEST
        stamp = path.stat().st_mtime_ns if path.exists() else None
        if stamp != self._stamp:
            self._cache, self._stamp = {}, stamp

    def _load(self, filename):
        with self._lock:
            self._check_fresh()
            if filename not in self._cache:
                path = self.data_dir / filename
                if not path.exists():
                    raise DataMissing(filename)
                self._cache[filename] = json.loads(path.read_text(encoding="utf-8"))
            return self._cache[filename]

    def manifest(self):
        return self._load(MANIFEST)

    def available(self, name):
        try:
            entry = self.manifest()["tables"].get(name)
        except DataMissing:
            return False
        return bool(entry) and not entry["missing"]

    def rows(self, name):
        """The table as a list of dicts, in file order."""
        entry = self.manifest()["tables"].get(name)
        if entry is None or entry["missing"]:
            raise DataMissing(name, "missing from the last export")
        data = self._load(entry["file"])
        return [dict(zip(data["columns"], row)) for row in data["rows"]]

    def columns(self, name):
        entry = self.manifest()["tables"].get(name)
        if entry is None:
            raise DataMissing(name, "not in the manifest")
        return entry["columns"]
