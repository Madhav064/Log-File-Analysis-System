import importlib.util

from loganalysis.common.config import REPO_ROOT

spec = importlib.util.spec_from_file_location("compare_engines", REPO_ROOT / "scripts" / "compare_engines.py")
ce = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ce)


def test_parse_mapreduce_keeps_order_and_ints():
    assert ce.parse_mapreduce("200\t10\n404\t3\n\n") == [("200", 10), ("404", 3)]
    assert ce.parse_mapreduce("/a b\t2\n") == [("/a b", 2)]  # keys may contain anything but the last tab


def test_identical_counts_match():
    assert ce.compare([("200", 5), ("404", 1)], [("404", 1), ("200", 5)], ranked=False)["match"]


def test_differences_are_reported():
    d = ce.compare([("200", 5), ("500", 1)], [("200", 6), ("404", 2)], ranked=False)
    assert not d["match"]
    assert d["only_spark"] == ["500"] and d["only_mr"] == ["404"] and d["different"] == [("200", 5, 6)]


def test_ranked_cuts_spark_to_the_same_n_and_breaks_ties_by_key():
    spark = [("/c", 1), ("/b", 5), ("/a", 5), ("/z", 1)]   # a full table, not just the top 2
    mr = [("/a", 5), ("/b", 5)]
    assert ce.compare(spark, mr, ranked=True)["match"]


def test_ranked_ties_in_any_order_still_match():
    # both sides are normalised to (count desc, key asc) before comparing
    assert ce.compare([("/a", 5), ("/b", 5)], [("/b", 5), ("/a", 5)], ranked=True)["match"]


def test_ranked_lists_with_different_members_do_not_match():
    assert not ce.compare([("/a", 5), ("/c", 4)], [("/a", 5), ("/b", 4)], ranked=True)["match"]


def test_spark_session_is_released_before_the_benchmark_runs(monkeypatch):
    """Regression: the comparison's Spark session held every core of the lite cluster's worker, so the first
    benchmark run (its own Spark application) waited for executors forever."""
    events = []

    class FakeFrame:
        def __init__(self, rows):
            self.write = self

        def mode(self, _):
            return self

        def parquet(self, path):
            events.append("write benchmark")

    class FakeSpark:
        def stop(self):
            events.append("stop session")

        def createDataFrame(self, rows, schema):
            return FakeFrame(rows)

    def fake_session(cfg):
        events.append("start session")
        return FakeSpark()

    monkeypatch.setattr(ce, "spark_session", fake_session)
    monkeypatch.setattr(ce, "read_spark", lambda spark, cfg, spec: [("k", 1)])
    monkeypatch.setattr(ce, "read_mapreduce", lambda cfg, job: "k\t1\n")
    monkeypatch.setattr(ce, "median_runtime", lambda cmd, runs: events.append("benchmark run") or 1.0)

    ce.main(["--env", "local", "--runs", "1"])

    first_run = events.index("benchmark run")
    assert events[:first_run] == ["start session", "stop session"]
    assert events[-3:] == ["start session", "write benchmark", "stop session"]
