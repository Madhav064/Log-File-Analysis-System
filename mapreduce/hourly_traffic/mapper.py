#!/usr/bin/env python3
"""Emit `YYYY-MM-DDTHH<TAB>1`. The hour is the server's own clock, read straight from the log line."""
import sys

sys.path.insert(0, "loganalysis.zip")  # shipped by Hadoop Streaming (-files); harmless in local runs
from loganalysis.parsing.streaming import parsed_records  # noqa: E402

for r in parsed_records():
    print(f"{r.ts:%Y-%m-%dT%H}\t1")
