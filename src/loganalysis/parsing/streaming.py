"""stdin -> parsed records, for Hadoop Streaming mappers. Pure Python, no Spark."""
import sys

from .parser import Rejected, parse_line

# Hadoop shows this counter on the YARN UI; local runs just print it to stderr.
MALFORMED_COUNTER = "reporter:counter:loganalysis,malformed_lines,1\n"


def parsed_records(stream=None):
    """Yield a ParsedRecord per good line and count the rest. Reads bytes and decodes with
    errors="replace" because the real data contains binary junk that strict UTF-8 would choke on."""
    for raw in stream or sys.stdin.buffer:
        record = parse_line(raw.decode("utf-8", errors="replace"))
        if isinstance(record, Rejected):
            sys.stderr.write(MALFORMED_COUNTER)
        else:
            yield record
