"""The code-level contract (loganalysis.contracts) must agree with what other modules assume about it."""
import importlib.util

from loganalysis.common.config import REPO_ROOT, load_config
from loganalysis.contracts import ANOMALY_KINDS, TABLES, column_names, ddl

spec = importlib.util.spec_from_file_location("compare_engines", REPO_ROOT / "scripts" / "compare_engines.py")
compare_engines = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare_engines)


def test_every_table_has_a_known_layer_and_unique_columns():
    for name, table in TABLES.items():
        assert table.layer in ("analytics", "quality"), name
        names = column_names(name)
        assert len(names) == len(set(names)), name


def test_ddl_is_comma_separated_name_type_pairs():
    assert ddl("top_error_hosts") == "host string, errors bigint"


def test_compare_engines_reads_tables_and_columns_that_exist():
    for job, spec in compare_engines.JOBS.items():
        table = spec["table"]
        assert table in TABLES, job
        assert spec["value"] in column_names(table), job
        assert TABLES[table].built_by == spec["spark_module"], job


def test_engine_benchmark_schema_matches_what_compare_engines_writes():
    # compare_engines.py writes this table itself; keep the two definitions in step.
    source = (REPO_ROOT / "scripts" / "compare_engines.py").read_text()
    assert "job string, engine string, runtime_sec double, output_rows long, results_match boolean" in source
    assert column_names("engine_benchmark") == ("job", "engine", "runtime_sec", "output_rows", "results_match")


def test_config_has_every_setting_the_jobs_read():
    cfg = load_config("local")
    assert isinstance(cfg["top_n"], int) and isinstance(cfg["peaks_top_k"], int)
    assert {"z_threshold", "baseline_days", "min_baseline_points", "min_requests"} <= cfg["anomaly"].keys()
    assert {"parsed", "rejected", "quality", "analytics", "raw_glob"} <= cfg["paths"].keys()


def test_anomaly_kinds():
    assert ANOMALY_KINDS == ("spike", "drop", "gap")
