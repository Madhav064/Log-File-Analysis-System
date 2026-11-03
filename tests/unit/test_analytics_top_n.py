import pytest

pytest.importorskip("pyspark")

from conftest import log_line  # noqa: E402

from loganalysis.analytics import top_n  # noqa: E402
from loganalysis.common.io import conform  # noqa: E402

LINES = [
    log_line(resource="/b.gif", host="x", nbytes=10),
    log_line(resource="/b.gif", host="x", nbytes=10),
    log_line(resource="/a.gif", host="y", nbytes=10),
    log_line(resource="/a.gif", host="y", nbytes=10),
    log_line(resource="/big.mpg", host="z", nbytes=1000),
    log_line(resource="/", host="z", nbytes=None),
]


@pytest.fixture
def parsed(make_parsed):
    return make_parsed(LINES)


def test_top_resources_by_hits_breaks_ties_by_resource_like_mapreduce(parsed):
    rows = top_n.top_resources_by_hits(parsed, 2).collect()
    assert sorted((r.resource, r.requests) for r in rows) == [("/a.gif", 2), ("/b.gif", 2)]


def test_top_resources_by_bytes(parsed):
    rows = top_n.top_resources_by_bytes(parsed, 1).collect()
    assert [(r.resource, r.bytes) for r in rows] == [("/big.mpg", 1000)]


def test_top_hosts_by_requests_with_bytes(parsed):
    rows = top_n.top_hosts(parsed, 3).collect()
    assert sorted((r.host, r.requests, r.bytes) for r in rows) == [("x", 2, 20), ("y", 2, 20), ("z", 2, 1000)]
    assert [r.host for r in top_n.top_hosts(parsed, 1).collect()] == ["x"]


def test_resource_type_breakdown(parsed):
    rows = {r.resource_type: r for r in top_n.resource_type_breakdown(parsed).collect()}
    assert {t: (r.requests, r.bytes) for t, r in rows.items()} == {
        "gif": (4, 40), "mpg": (1, 1000), "directory": (1, 0)}
    assert rows["gif"].pct == pytest.approx(400 / 6)


def test_compute_matches_the_contract_tables(parsed):
    for name, df in top_n.compute(parsed, {"top_n": 2}).items():
        conform(df, name)
