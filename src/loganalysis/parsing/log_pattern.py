"""The one definition of a log line. Spark (regexp_extract) and the MapReduce mappers both use these.

Groups are numbered, not named: Python's (?P<name>...) is not valid Java regex syntax and Spark's
regexp_extract selects groups by number. Keep every pattern valid in both engines.
"""

# 1 host, 2 ident, 3 user, 4 timestamp, 5 request, 6 status, 7 bytes
LOG_PATTERN = r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(.*)" (\d{3}) (\d+|-)$'

# Lenient on purpose: ~0.1% of real lines have spaces in the URL, "HTTP/V1.0" or no protocol.
# 1 method, 2 resource (first token after the method), 3 protocol (optional)
REQUEST_PATTERN = r'^(\S+)\s+(\S+)(?:.*?\s(HTTP/\S+))?.*$'
