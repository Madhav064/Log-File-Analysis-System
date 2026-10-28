from datetime import timedelta
from pathlib import Path

import pytest

from loganalysis.parsing.parser import ParsedRecord, Rejected, parse_line

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "edge_cases.log"
LINES = FIXTURE.read_text(encoding="utf-8").split("\n")


def parse(n):
    """Parse line n (1-based) of the fixture."""
    return parse_line(LINES[n - 1])


def test_normal_line():
    r = parse(1)
    assert isinstance(r, ParsedRecord)
    assert (r.host, r.method, r.resource, r.protocol) == ("199.72.81.55", "GET", "/history/apollo/", "HTTP/1.0")
    assert (r.status, r.status_class, r.bytes, r.bytes_missing, r.is_error) == (200, "2xx", 6245, False, False)
    assert (str(r.log_date), r.log_hour, r.resource_type) == ("1995-07-01", 0, "directory")
    assert r.ts.utcoffset() == timedelta(hours=-4)


def test_query_string_is_stripped():
    assert parse(2).resource == "/shuttle/countdown/"


def test_missing_bytes_is_zero_with_flag():
    r = parse(3)
    assert (r.bytes, r.bytes_missing, r.status, r.resource_type) == (0, True, 304, "gif")


@pytest.mark.parametrize("n, resource, protocol", [
    (4, "/icons/back.xbm", None),                 # no protocol
    (5, "/history/history.html", "HTTP/1.0"),     # spaces inside the URL
    (6, "/", "HTTP/V1.0"),                        # odd protocol token is kept as-is
    (7, "/café/naïve.html", "HTTP/1.0"),          # non-ASCII
])
def test_messy_requests_are_kept(n, resource, protocol):
    r = parse(n)
    assert isinstance(r, ParsedRecord)
    assert (r.resource, r.protocol) == (resource, protocol)


def test_unusual_method_and_5xx():
    r = parse(8)
    assert (r.method, r.status_class, r.is_error) == ("FOO", "5xx", True)


@pytest.mark.parametrize("n, reason", [
    (9, "NO_MATCH"),         # empty line
    (10, "NO_MATCH"),        # truncated line
    (11, "BAD_TIMESTAMP"),   # 32 July
    (12, "BAD_TIMESTAMP"),   # 25:00
    (13, "BAD_TIMESTAMP"),   # unknown month
    (14, "BAD_STATUS"),      # 999
    (15, "BAD_REQUEST"),     # request is just "-"
    (16, "NO_MATCH"),        # bytes is not a number
])
def test_rejections_carry_a_reason(n, reason):
    assert parse(n) == Rejected(reason)


def test_never_raises_on_junk():
    for junk in ["", " ", "\x00\x01", '"', "a b c [d] e f g", "é" * 500]:
        assert isinstance(parse_line(junk), Rejected)
