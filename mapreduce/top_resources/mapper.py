#!/usr/bin/env python3
"""Stage 1: emit `resource<TAB>1`. The shared sum_reducer.py totals them; top_n.py then picks the top N."""
import sys

sys.path.insert(0, "loganalysis.zip")  # shipped by Hadoop Streaming (-files); harmless in local runs
from loganalysis.parsing.streaming import parsed_records  # noqa: E402

for r in parsed_records():
    print(f"{r.resource}\t1")
