"""Make `import app` work everywhere: pytest 6 (the cluster image's) ignores the pythonpath option in pytest.ini.
The dashboard tests need Flask; where it is not installed (the Hadoop/Spark client image) they are skipped, and
they run in CI and in a dev environment with requirements/dev.txt."""
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
for path in (HERE.parent, HERE.parents[1] / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

if importlib.util.find_spec("flask") is None:
    collect_ignore_glob = ["test_*.py"]
    print("dashboard tests skipped: Flask is not installed here (pip install -r requirements/dashboard.txt)")
