from datetime import timedelta
from pathlib import Path

import pytest

from loganalysis.parsing.parser import ParsedRecord, Rejected, parse_line
from loganalysis.parsing.schema import PARSED_COLUMNS

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


@pytest.mark.parametrize("n, reason", [
    (17, "BAD_TIMESTAMP"),   # lower-case month: Spark's date parser would accept it, so the shape check must not
    (18, "BAD_TIMESTAMP"),   # 30 February
])
def test_timestamps_are_strict(n, reason):
    assert parse(n) == Rejected(reason)


def test_whitespace_classes_are_ascii_like_java():
    # A non-breaking space is not whitespace for Java's \s, so it is not for ours either (re.ASCII).
    r = parse(19)
    assert (r.method, r.resource, r.protocol) == ("GET /nbsp", "HTTP/1.0", None)


@pytest.mark.parametrize("n, resource, rtype", [
    (20, "", "none"),            # the target is only a query string
    (21, "/a/file.", ""),        # trailing dot: empty extension
    (22, "/a/B.GIF", "gif"),     # extensions are lower-cased
])
def test_resource_and_type_corner_cases(n, resource, rtype):
    r = parse(n)
    assert (r.resource, r.resource_type) == (resource, rtype)


def test_last_second_of_the_day_stays_on_that_day():
    r = parse(22)
    assert (str(r.log_date), r.log_hour, r.method) == ("1995-07-01", 23, "HEAD")


def test_crlf_line_ending_is_stripped():
    assert parse_line(LINES[0] + "\r\n") == parse(1)


def test_record_fields_are_the_contract_columns():
    assert ParsedRecord._fields == PARSED_COLUMNS


def test_never_raises_on_junk():
    for junk in ["", " ", "\x00\x01", '"', "a b c [d] e f g", "é" * 500]:
        assert isinstance(parse_line(junk), Rejected)
