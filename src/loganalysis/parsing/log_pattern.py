"""The one definition of a log line. Spark (regexp_extract / rlike) and the MapReduce mappers both use these.

Rules that keep every pattern valid, and equivalent, in both engines:
- Groups are numbered, not named: Python's (?P<name>...) is not valid Java regex syntax and Spark's
  regexp_extract selects groups by number.
- Python compiles them with re.ASCII. Java's \\s, \\S and \\d are ASCII-only by default, Python's are
  Unicode-aware, so without the flag the engines would disagree on lines containing e.g. a non-breaking space.
"""

# 1 host, 2 ident, 3 user, 4 timestamp, 5 request, 6 status, 7 bytes
LOG_PATTERN = r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(.*)" (\d{3}) (\d+|-)$'

# Lenient on purpose: ~0.1% of real lines have spaces in the URL, "HTTP/V1.0" or no protocol.
# 1 method, 2 resource (first token after the method), 3 protocol (optional)
REQUEST_PATTERN = r'^(\S+)\s+(\S+)(?:.*?\s(HTTP/\S+))?.*$'

# The shape of the timestamp group. Month names are case-sensitive here because Spark's date parser is
# case-insensitive: checking the shape first makes both engines reject exactly the same strings.
# 1 day, 2 month, 3 year, 4 hour, 5 minute, 6 second, 7 offset sign, 8 offset hours, 9 offset minutes
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
TIMESTAMP_PATTERN = (r"^(\d{2})/(" + "|".join(MONTHS) + r")/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-])(\d{2})(\d{2})$")

# Spark datetime pattern for the same string (used with to_timestamp after the shape check above).
SPARK_TIMESTAMP_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z"
