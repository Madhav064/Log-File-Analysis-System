"""Pure-Python parser used by the MapReduce mappers and the tests (no Spark import).

SEED for task B1: enough for the MapReduce jobs to run. B1 owns this module: schema.py (Spark
StructType), the Spark path and the Spark-vs-Python parity test still have to be built.
"""
import re
from collections import namedtuple
from datetime import datetime, timedelta, timezone

from .log_pattern import LOG_PATTERN, REQUEST_PATTERN

NO_MATCH, BAD_TIMESTAMP, BAD_STATUS, BAD_REQUEST = "NO_MATCH", "BAD_TIMESTAMP", "BAD_STATUS", "BAD_REQUEST"

ParsedRecord = namedtuple(
    "ParsedRecord",
    "host ts log_date log_hour method resource protocol status status_class bytes bytes_missing "
    "is_error resource_type",
)
Rejected = namedtuple("Rejected", "reason")

_LOG, _REQ = re.compile(LOG_PATTERN), re.compile(REQUEST_PATTERN)
_TS = re.compile(r"^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$")
_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def parse_timestamp(text):
    """'01/Jul/1995:00:00:01 -0400' -> aware datetime, or None. Does not depend on the locale."""
    m = _TS.match(text)
    if not m or m.group(2) not in _MONTHS:
        return None
    d, mon, y, hh, mm, ss, sign, oh, om = m.groups()
    try:
        offset = timezone(timedelta(hours=int(oh), minutes=int(om)) * (-1 if sign == "-" else 1))
        return datetime(int(y), _MONTHS[mon], int(d), int(hh), int(mm), int(ss), tzinfo=offset)
    except ValueError:  # e.g. 32/Jul or 25:00:00
        return None


def resource_type(resource):
    if resource.endswith("/"):
        return "directory"
    name = resource.rsplit("/", 1)[-1]
    return name.rsplit(".", 1)[-1].lower() if "." in name else "none"


def parse_line(line):
    """Return a ParsedRecord, or Rejected(reason). Never raises on bad input."""
    m = _LOG.match(line.rstrip("\r\n"))
    if not m:
        return Rejected(NO_MATCH)
    host, _ident, _user, raw_ts, request, raw_status, raw_bytes = m.groups()
    ts = parse_timestamp(raw_ts)
    if ts is None:
        return Rejected(BAD_TIMESTAMP)
    status = int(raw_status)
    if not 100 <= status <= 599:
        return Rejected(BAD_STATUS)
    r = _REQ.match(request)
    if not r:
        return Rejected(BAD_REQUEST)
    method, target, protocol = r.groups()
    resource = target.split("?", 1)[0]
    return ParsedRecord(
        host=host, ts=ts, log_date=ts.date(), log_hour=ts.hour, method=method, resource=resource,
        protocol=protocol, status=status, status_class=f"{status // 100}xx",
        bytes=0 if raw_bytes == "-" else int(raw_bytes), bytes_missing=raw_bytes == "-",
        is_error=status >= 400, resource_type=resource_type(resource),
    )
