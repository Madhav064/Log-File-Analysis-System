from datetime import date

import pytest
from app.api.validation import BadParam, choice, date_range, integer

FIRST, LAST = date(1995, 7, 1), date(1995, 8, 31)


def test_choice_accepts_allowed_values_and_defaults():
    assert choice({"g": "day"}, "g", ("hour", "day"), "hour") == "day"
    assert choice({}, "g", ("hour", "day"), "hour") == "hour"
    assert choice({"g": ""}, "g", ("hour", "day")) is None
    with pytest.raises(BadParam, match="one of hour, day"):
        choice({"g": "week"}, "g", ("hour", "day"), "hour")


def test_integer_bounds_and_garbage():
    assert integer({"n": "10"}, "n", 20, 1, 100) == 10
    assert integer({}, "n", 20, 1, 100) == 20
    for bad in ("0", "101", "ten", "1.5"):
        with pytest.raises(BadParam):
            integer({"n": bad}, "n", 20, 1, 100)


def test_date_range_defaults_to_the_whole_dataset_and_rejects_bad_ranges():
    assert date_range({}, FIRST, LAST) == (FIRST, LAST)
    assert date_range({"from": "1995-07-13", "to": "1995-07-13"}, FIRST, LAST) == (date(1995, 7, 13),) * 2
    for args in ({"from": "1995-06-30"}, {"to": "1995-09-01"}, {"from": "13/07/1995"},
                 {"from": "1995-08-01", "to": "1995-07-01"}):
        with pytest.raises(BadParam):
            date_range(args, FIRST, LAST)
