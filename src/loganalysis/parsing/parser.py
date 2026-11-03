"""Pure-Python parser used by the MapReduce mappers and the tests (no Spark import).

`spark_parser.py` is the Spark twin of `parse_line`. Both use the patterns in `log_pattern.py`, run the checks
in the same order, and are held to identical output by tests/unit/test_parser_parity.py.
"""
import re
from collections import namedtuple
from datetime import datetime, timedelta, timezone

from .log_pattern import LOG_PATTERN, MONTHS, REQUEST_PATTERN, TIMESTAMP_PATTERN
from .schema import (
    BAD_REQUEST,
    BAD_STATUS,
    BAD_TIMESTAMP,
    ERROR_STATUS,
    MAX_STATUS,
    MIN_STATUS,
    NO_MATCH,
    PARSED_COLUMNS,
    status_class,
)

__all__ = ["BAD_REQUEST", "BAD_STATUS", "BAD_TIMESTAMP", "NO_MATCH", "ParsedRecord", "Rejected",
           "parse_line", "parse_timestamp", "resource_type"]

ParsedRecord = namedtuple("ParsedRecord", PARSED_COLUMNS)
Rejected = namedtuple("Rejected", "reason")

_LOG = re.compile(LOG_PATTERN, re.ASCII)
_REQ = re.compile(REQUEST_PATTERN, re.ASCII)
_TS = re.compile(TIMESTAMP_PATTERN, re.ASCII)
_MONTH_NUMBER = {name: i for i, name in enumerate(MONTHS, 1)}


def parse_timestamp(text):
    """'01/Jul/1995:00:00:01 -0400' -> timezone-aware datetime, or None. Does not depend on the locale."""
    m = _TS.match(text)
    if not m:
        return None
    day, month, year, hh, mm, ss, sign, off_h, off_m = m.groups()
    try:
        offset = timezone(timedelta(hours=int(off_h), minutes=int(off_m)) * (-1 if sign == "-" else 1))
        return datetime(int(year), _MONTH_NUMBER[month], int(day), int(hh), int(mm), int(ss), tzinfo=offset)
    except ValueError:  # 32/Jul, 25:00:00, an offset of 24 hours or more
        return None


def resource_type(resource):
    """'/a/b.GIF' -> 'gif', '/a/' -> 'directory', '/a/b' -> 'none'."""
    if resource.endswith("/"):
        return "directory"
    name = resource.rsplit("/", 1)[-1]
    return name.rsplit(".", 1)[-1].lower() if "." in name else "none"


def parse_line(line):
    """Return a ParsedRecord, or Rejected(reason). Never raises on bad input.

    log_date and log_hour come from the line's own clock (its UTC offset), which is the server's local time.
    """
    m = _LOG.match(line.rstrip("\r\n"))
    if not m:
        return Rejected(NO_MATCH)
    host, _ident, _user, raw_ts, request, raw_status, raw_bytes = m.groups()
    ts = parse_timestamp(raw_ts)
    if ts is None:
        return Rejected(BAD_TIMESTAMP)
    status = int(raw_status)
    if not MIN_STATUS <= status <= MAX_STATUS:
        return Rejected(BAD_STATUS)
    r = _REQ.match(request)
    if not r:
        return Rejected(BAD_REQUEST)
    method, target, protocol = r.groups()
    resource = target.split("?", 1)[0]
    return ParsedRecord(
        host=host, ts=ts, log_date=ts.date(), log_hour=ts.hour, method=method, resource=resource,
        protocol=protocol, status=status, status_class=status_class(status),
        bytes=0 if raw_bytes == "-" else int(raw_bytes), bytes_missing=raw_bytes == "-",
        is_error=status >= ERROR_STATUS, resource_type=resource_type(resource),
    )
