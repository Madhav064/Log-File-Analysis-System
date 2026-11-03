"""The Spark parser (regexp_extract) and the Python parser (re) must give every line the same verdict and the
same fields: this is what lets the Spark and MapReduce results be compared at all (plan M3)."""
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from loganalysis.common.config import REPO_ROOT  # noqa: E402
from loganalysis.parsing.parser import Rejected, parse_line  # noqa: E402
from loganalysis.parsing.schema import PARSED_COLUMNS, REJECTED_COLUMNS, parsed_struct  # noqa: E402
from loganalysis.parsing.spark_parser import annotate, parsed_records, rejected_records  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "edge_cases.log"
SAMPLE = REPO_ROOT / "data" / "sample" / "nasa_sample.log"
EXTRA = [
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /crlf HTTP/1.0" 200 6245\r',   # CR left by a CRLF file
    "�� binary junk decoded with errors=replace",
]


def all_lines():
    return (FIXTURE.read_text("utf-8").split("\n") + SAMPLE.read_text("utf-8").split("\n") + EXTRA)


def python_view(line):
    r = parse_line(line)
    if isinstance(r, Rejected):
        return ("REJECTED", r.reason)
    return tuple(int(r.ts.timestamp()) if col == "ts" else getattr(r, col) for col in PARSED_COLUMNS)


def spark_view(row):
    if row.reason is not None:
        return ("REJECTED", row.reason)
    return tuple(row.ts_epoch if col == "ts" else row[col] for col in PARSED_COLUMNS)


@pytest.fixture(scope="module")
def annotated(spark):
    lines = all_lines()
    df = spark.createDataFrame(list(enumerate(lines)), "line_id int, value string")
    return lines, annotate(df, source_col=None, keep=("line_id",))


def test_every_line_gets_the_same_verdict_and_fields(annotated):
    lines, df = annotated
    rows = df.selectExpr("*", "CAST(ts AS LONG) AS ts_epoch").collect()
    spark_by_id = {r.line_id: spark_view(r) for r in rows}
    assert len(spark_by_id) == len(lines)
    mismatches = [(i, lines[i], python_view(lines[i]), spark_by_id[i])
                  for i in range(len(lines)) if python_view(lines[i]) != spark_by_id[i]]
    assert not mismatches, "\n".join(map(str, mismatches[:10]))


def test_both_outcomes_are_exercised(annotated):
    lines, _ = annotated
    verdicts = [python_view(line)[0] == "REJECTED" for line in lines]
    assert sum(verdicts) >= 10 and verdicts.count(False) >= 2000


def test_parsed_records_have_the_contract_schema(annotated):
    _, df = annotated
    parsed = parsed_records(df)
    assert [(f.name, f.dataType) for f in parsed.schema.fields] == \
           [(f.name, f.dataType) for f in parsed_struct().fields]


def test_rejected_records_keep_the_raw_line_and_reason(annotated):
    lines, df = annotated
    rejected = rejected_records(df)
    assert tuple(rejected.columns) == REJECTED_COLUMNS
    expected = sorted((line.rstrip("\r\n"), parse_line(line).reason)
                      for line in lines if isinstance(parse_line(line), Rejected))
    assert sorted((r.raw_line, r.reason) for r in rejected.collect()) == expected
