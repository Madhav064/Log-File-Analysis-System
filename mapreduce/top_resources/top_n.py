#!/usr/bin/env python3
"""Keep the N largest `key<TAB>count` lines. Stage 2 of top_resources, used twice:

  mapper  : each mapper emits only its local top N when its input ends
  reducer : ONE reducer merges every mapper's top N into the global top N

Order is count descending, then key ascending, so ties are deterministic and comparable with Spark.
Usage: top_n.py N
"""
import heapq
import sys

n = int(sys.argv[1])
rows = []
for line in sys.stdin:
    key, _, count = line.rstrip("\n").partition("\t")
    rows.append((int(count), key))
for count, key in heapq.nsmallest(n, rows, key=lambda row: (-row[0], row[1])):
    print(f"{key}\t{count}")
