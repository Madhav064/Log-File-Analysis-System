#!/usr/bin/env python3
"""Emit `status<TAB>1` for every well-formed request."""
import sys

sys.path.insert(0, "loganalysis.zip")  # shipped by Hadoop Streaming (-files); harmless in local runs
from loganalysis.parsing.streaming import parsed_records  # noqa: E402

for r in parsed_records():
    print(f"{r.status}\t1")
