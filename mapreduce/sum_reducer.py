#!/usr/bin/env python3
"""Sum `key<TAB>count` lines (input arrives sorted by key). Used as reducer AND combiner by every counting job."""
import sys

key, total = None, 0
for line in sys.stdin:
    k, _, n = line.rstrip("\n").partition("\t")
    if k != key:
        if key is not None:
            print(f"{key}\t{total}")
        key, total = k, 0
    total += int(n)
if key is not None:
    print(f"{key}\t{total}")
