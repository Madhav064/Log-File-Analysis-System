"""Rebuild dashboard/tests/data/: the real pipeline (ETL, the four analytics jobs, the export) run on the bundled
real NASA sample, data/sample/nasa_sample.log. The API tests read it, so they run anywhere without Spark.

Needs pyspark, so run it in the client container:

    docker compose -f infra/docker-compose.yml --profile lite run --rm --no-deps client \
        python dashboard/tests/make_test_data.py

Everything is written to a temp folder first, so data/serving/ (the real export) is never touched.
"""
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from loganalysis.analytics import anomalies, errors, top_n, traffic  # noqa: E402
from loganalysis.common.config import load_config  # noqa: E402
from loganalysis.common.spark import get_spark  # noqa: E402
from loganalysis.etl import parse_logs  # noqa: E402
from loganalysis.export import to_serving  # noqa: E402

OUT = Path(__file__).resolve().parent / "data"


def main():
    cfg = load_config("local")
    with tempfile.TemporaryDirectory() as tmp:
        for key in ("parsed", "rejected", "quality", "analytics", "mapreduce", "serving", "reports"):
            cfg["paths"][key] = str(Path(tmp) / key)
        spark = get_spark("make-test-data", cfg)
        try:
            parse_logs.run(spark, cfg)
            for job in (errors, traffic, top_n, anomalies):
                job.run(spark, cfg)
            to_serving.run(spark, cfg)
        finally:
            spark.stop()
        if OUT.exists():
            shutil.rmtree(OUT)
        shutil.copytree(Path(tmp) / "serving", OUT)
    print(f"wrote {OUT} ({len(list(OUT.glob('*.json')))} files)")


if __name__ == "__main__":
    main()
