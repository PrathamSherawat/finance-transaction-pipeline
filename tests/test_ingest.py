import pytest
from pyspark.sql.types import StringType

from finance_pipeline.generate_data import (
    COLUMNS,
    GeneratorConfig,
    generate_rows,
    write_csv,
)
from finance_pipeline.ingest import RAW_SCHEMA, check_header, read_raw_transactions


def test_raw_schema_is_all_strings_in_columns_order():
    assert RAW_SCHEMA.fieldNames() == list(COLUMNS)
    assert all(isinstance(f.dataType, StringType) for f in RAW_SCHEMA.fields)


def test_spark_session_starts(spark):
    assert spark.range(3).count() == 3


def test_spark_reads_csv_with_raw_schema(spark, tmp_path):
    path = tmp_path / "t.csv"
    write_csv(generate_rows(GeneratorConfig(num_rows=100, seed=1)), path)
    df = spark.read.csv(str(path), header=True, schema=RAW_SCHEMA)
    assert df.count() == 100
    assert df.columns == list(COLUMNS)
    assert all(dtype == "string" for _, dtype in df.dtypes)


HEADER = ",".join(COLUMNS)
GOOD_ROW = "TXN-000001,ACC-0001,2025-01-01T00:00:00,CREDIT,10.00,INR,POSTED,2025-01-01T00:00:01"


def test_check_header_accepts_generated_file(tmp_path):
    p = tmp_path / "t.csv"
    write_csv(generate_rows(GeneratorConfig(num_rows=20)), p)
    check_header(p)


def test_check_header_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        check_header(tmp_path / "nope.csv")


def test_check_header_empty_file_raises(tmp_path):
    p = tmp_path / "t.csv"
    p.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        check_header(p)


def test_check_header_reordered_columns_raises(tmp_path):
    p = tmp_path / "t.csv"
    swapped = list(COLUMNS)
    swapped[0], swapped[1] = swapped[1], swapped[0]
    p.write_text(",".join(swapped) + "\n" + GOOD_ROW + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="order"):
        check_header(p)


def test_check_header_missing_column_raises(tmp_path):
    p = tmp_path / "t.csv"
    p.write_text(",".join(COLUMNS[:-1]) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing"):
        check_header(p)


def test_check_header_extra_column_raises(tmp_path):
    p = tmp_path / "t.csv"
    p.write_text(HEADER + ",extra\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unexpected"):
        check_header(p)


def test_read_raw_returns_all_rows_as_strings(spark, tmp_path):
    p = tmp_path / "t.csv"
    write_csv(generate_rows(GeneratorConfig(num_rows=100, seed=1)), p)
    df = read_raw_transactions(spark, p)
    assert df.count() == 100
    assert df.columns == list(COLUMNS)
    assert all(dtype == "string" for _, dtype in df.dtypes)


def test_read_raw_rejects_bad_header_before_reading(spark, tmp_path):
    p = tmp_path / "t.csv"
    p.write_text(HEADER + ",extra\n" + GOOD_ROW + ",x\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unexpected"):
        read_raw_transactions(spark, p)


def test_read_raw_values_survive_untouched_including_malformed(spark, tmp_path):
    rows = generate_rows(GeneratorConfig(num_rows=1000, seed=3))
    p = tmp_path / "t.csv"
    write_csv(rows, p)
    got = sorted(
        tuple("" if v is None else v for v in r)
        for r in read_raw_transactions(spark, p).collect()
    )
    expected = sorted(tuple(r[c] for c in COLUMNS) for r in rows)
    assert got == expected


def test_read_raw_empty_field_arrives_as_null_not_empty_string(spark, tmp_path):
    p = tmp_path / "t.csv"
    p.write_text(HEADER + "\n" + "," + GOOD_ROW.split(",", 1)[1] + "\n", encoding="utf-8")
    row = read_raw_transactions(spark, p).collect()[0]
    assert row["transaction_id"] is None
    assert row["account_id"] == "ACC-0001"


def test_read_raw_wrong_field_count_fails_fast(spark, tmp_path):
    p = tmp_path / "t.csv"
    p.write_text(HEADER + "\nTXN-000001,ACC-0001,2025-01-01T00:00:00\n", encoding="utf-8")
    df = read_raw_transactions(spark, p)  # lazy: no error yet
    # count() needs no column values, so Spark prunes columns and never parses
    # the row. Only an action that reads the values triggers FAILFAST.
    with pytest.raises(Exception):  # Spark wraps this in a Py4J/Spark exception
        df.collect()


def test_timestamp_policy_is_corrected_and_bad_values_become_null(spark):
    assert spark.conf.get("spark.sql.legacy.timeParserPolicy") == "CORRECTED"
    fmt = "yyyy-MM-dd'T'HH:mm:ss"
    bad = ["2025-02-30T00:00:00", "2025-13-45T99:99:99", "not-a-timestamp", "",
           " 2025-01-01T00:00:00", "2025-01-01T00:00:00Z", "2025-1-1T00:00:00"]
    for v in bad:
        got = spark.sql(f"SELECT to_timestamp('{v}', \"{fmt}\") AS p").collect()[0]["p"]
        assert got is None, v
    ok = spark.sql(f"SELECT to_timestamp('2025-01-01T00:00:00', \"{fmt}\") AS p").collect()[0]["p"]
    assert ok is not None


def test_null_in_is_null_not_false(spark):
    row = spark.sql("SELECT NULL IN ('CREDIT','DEBIT') AS a, NOT (NULL IN ('CREDIT','DEBIT')) AS b").collect()[0]
    assert row["a"] is None and row["b"] is None
