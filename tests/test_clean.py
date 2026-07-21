"""Tests for the OHLCV cleaning stage."""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_jobs.clean import TIMESTAMP_FORMAT, clean

RAW_SCHEMA = StructType(
    [
        StructField("row_index", LongType(), True),
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("barCount", LongType(), True),
        StructField("average", DoubleType(), True),
    ]
)

T1 = "20080103  09:30:00"
T2 = "20080103  09:31:00"
T3 = "20080103  09:32:00"

GOOD_ROW = (1, T2, 147.0, 148.0, 146.0, 147.5, 1000, 10, 147.2)


@pytest.fixture
def raw_df(spark):
    return spark.createDataFrame(
        [Row(*GOOD_ROW)],
        schema=RAW_SCHEMA,
    )


def test_duplicate_rows_removed(spark):
    df = spark.createDataFrame([GOOD_ROW, GOOD_ROW], schema=RAW_SCHEMA)
    result = clean(df)
    assert result.count() == 1


def test_timestamp_column_is_timestamp_type(raw_df):
    result = clean(raw_df)
    assert dict(result.dtypes)["date"] == "timestamp"


def test_timestamp_parsed_correctly(raw_df):
    result = clean(raw_df)
    ts = result.collect()[0]["date"]
    assert ts.year == 2008
    assert ts.month == 1
    assert ts.day == 3
    assert ts.hour == 9
    assert ts.minute == 31


def test_null_close_row_removed(spark):
    rows = [
        (1, T1, 147.0, 148.0, 146.0, None, 1000, 10, 147.2),
        (2, T2, 147.0, 148.0, 146.0, 147.5, 1000, 10, 147.2),
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    result = clean(df)
    assert result.count() == 1


def test_null_open_row_removed(spark):
    rows = [
        (1, T1, None, 148.0, 146.0, 147.5, 1000, 10, 147.2),
        (2, T2, 147.0, 148.0, 146.0, 147.5, 1000, 10, 147.2),
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    result = clean(df)
    assert result.count() == 1


def test_null_volume_row_removed(spark):
    rows = [
        (1, T1, 147.0, 148.0, 146.0, 147.5, None, 10, 147.2),
        (2, T2, 147.0, 148.0, 146.0, 147.5, 1000, 10, 147.2),
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    result = clean(df)
    assert result.count() == 1


def test_rows_sorted_chronologically(spark):
    rows = [
        (3, T3, 149.0, 150.0, 148.0, 149.5, 900, 9, 149.1),
        (1, T1, 147.0, 148.0, 146.0, 147.5, 1000, 10, 147.2),
        (2, T2, 148.0, 149.0, 147.0, 148.5, 1100, 11, 148.3),
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    result = clean(df)
    timestamps = [row["date"] for row in result.collect()]
    assert timestamps == sorted(timestamps)


def test_clean_preserves_valid_rows(spark):
    rows = [
        (1, T1, 147.0, 148.0, 146.0, 147.5, 1000, 10, 147.2),
        (2, T2, 148.0, 149.0, 147.0, 148.5, 1100, 11, 148.3),
    ]
    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    result = clean(df)
    assert result.count() == 2
