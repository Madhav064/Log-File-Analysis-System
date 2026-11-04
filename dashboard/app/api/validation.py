"""Query-parameter checks for the API. A bad value raises BadParam, which the API returns as a 400."""
from datetime import date


class BadParam(ValueError):
    pass


def choice(args, name, allowed, default=None):
    value = args.get(name, default)
    if value is None or value == "":
        if default is None:
            return None
        value = default
    if value not in allowed:
        raise BadParam(f"{name} must be one of {', '.join(allowed)} (got {value!r})")
    return value


def integer(args, name, default, low, high):
    raw = args.get(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError:
        raise BadParam(f"{name} must be a whole number (got {raw!r})") from None
    if not low <= value <= high:
        raise BadParam(f"{name} must be between {low} and {high} (got {value})")
    return value


def day(args, name, first, last):
    """An ISO date (YYYY-MM-DD) inside [first, last], or None when not given."""
    raw = args.get(name)
    if raw in (None, ""):
        return None
    try:
        value = date.fromisoformat(raw)
    except ValueError:
        raise BadParam(f"{name} must be a date like 1995-07-13 (got {raw!r})") from None
    if not first <= value <= last:
        raise BadParam(f"{name} must be between {first} and {last}, the dates in the data (got {raw})")
    return value


def date_range(args, first, last):
    start, end = day(args, "from", first, last), day(args, "to", first, last)
    if start and end and start > end:
        raise BadParam("from must not be after to")
    return start or first, end or last
