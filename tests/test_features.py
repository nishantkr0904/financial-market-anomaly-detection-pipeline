"""Tests for the deterministic feature engineering stage."""

from datetime import datetime

import pytest
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

from analytics.features import add_candle_body, add_price_range, add_simple_return

FEATURE_SCHEMA = StructType(
    [
        StructField("date", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
    ]
)

T1 = datetime(2020, 1, 2, 9, 30)
T2 = datetime(2020, 1, 2, 9, 31)
T3 = datetime(2020, 1, 2, 9, 32)


def _row(ts, open_, high, low, close):
    return (ts, open_, high, low, close)


def _rows_by_time(df, column):
    return [
        (row["date"], row[column])
        for row in df.orderBy("date").collect()
    ]


# --- simple_return -----------------------------------------------------------


def test_simple_return_first_row_is_null(spark):
    df = spark.createDataFrame([_row(T1, 100.0, 101.0, 99.0, 100.0)], FEATURE_SCHEMA)
    result = _rows_by_time(add_simple_return(df), "simple_return")
    assert result[0][1] is None


def test_simple_return_positive_move(spark):
    # (110 - 100) / 100 == 0.10
    rows = [
        _row(T1, 100.0, 100.0, 100.0, 100.0),
        _row(T2, 100.0, 100.0, 100.0, 110.0),
    ]
    df = spark.createDataFrame(rows, FEATURE_SCHEMA)
    result = _rows_by_time(add_simple_return(df), "simple_return")
    assert result[1][1] == pytest.approx(0.10)


def test_simple_return_negative_move(spark):
    # (90 - 100) / 100 == -0.10
    rows = [
        _row(T1, 100.0, 100.0, 100.0, 100.0),
        _row(T2, 100.0, 100.0, 100.0, 90.0),
    ]
    df = spark.createDataFrame(rows, FEATURE_SCHEMA)
    result = _rows_by_time(add_simple_return(df), "simple_return")
    assert result[1][1] == pytest.approx(-0.10)


def test_simple_return_zero_movement(spark):
    # (100 - 100) / 100 == 0.0
    rows = [
        _row(T1, 100.0, 100.0, 100.0, 100.0),
        _row(T2, 100.0, 100.0, 100.0, 100.0),
    ]
    df = spark.createDataFrame(rows, FEATURE_SCHEMA)
    result = _rows_by_time(add_simple_return(df), "simple_return")
    assert result[1][1] == pytest.approx(0.0)


def test_simple_return_uses_prior_row_by_time_order(spark):
    # Rows are inserted out of chronological order; the lag() must respect
    # date ordering, not insertion order.
    rows = [
        _row(T3, 100.0, 100.0, 100.0, 121.0),  # +10% vs. T2
        _row(T1, 100.0, 100.0, 100.0, 100.0),
        _row(T2, 100.0, 100.0, 100.0, 110.0),  # +10% vs. T1
    ]
    df = spark.createDataFrame(rows, FEATURE_SCHEMA)
    result = _rows_by_time(add_simple_return(df), "simple_return")
    assert result[0][1] is None
    assert result[1][1] == pytest.approx(0.10)
    assert result[2][1] == pytest.approx(0.10)


# --- price_range -------------------------------------------------------------


def test_price_range_positive_spread(spark):
    # high - low == 105 - 95 == 10
    df = spark.createDataFrame(
        [_row(T1, 100.0, 105.0, 95.0, 102.0)], FEATURE_SCHEMA
    )
    result = _rows_by_time(add_price_range(df), "price_range")
    assert result[0][1] == pytest.approx(10.0)


def test_price_range_zero_when_high_equals_low(spark):
    # A frozen bar: high == low, so range == 0
    df = spark.createDataFrame(
        [_row(T1, 100.0, 100.0, 100.0, 100.0)], FEATURE_SCHEMA
    )
    result = _rows_by_time(add_price_range(df), "price_range")
    assert result[0][1] == pytest.approx(0.0)


# --- candle_body -------------------------------------------------------------


def test_candle_body_bullish_is_positive(spark):
    # close > open => positive body: 102 - 100 == 2
    df = spark.createDataFrame(
        [_row(T1, 100.0, 105.0, 95.0, 102.0)], FEATURE_SCHEMA
    )
    result = _rows_by_time(add_candle_body(df), "candle_body")
    assert result[0][1] == pytest.approx(2.0)


def test_candle_body_bearish_is_negative(spark):
    # close < open => negative body: 98 - 100 == -2
    df = spark.createDataFrame(
        [_row(T1, 100.0, 101.0, 97.0, 98.0)], FEATURE_SCHEMA
    )
    result = _rows_by_time(add_candle_body(df), "candle_body")
    assert result[0][1] == pytest.approx(-2.0)


def test_candle_body_zero_when_close_equals_open(spark):
    # A doji: close == open, so body == 0
    df = spark.createDataFrame(
        [_row(T1, 100.0, 101.0, 99.0, 100.0)], FEATURE_SCHEMA
    )
    result = _rows_by_time(add_candle_body(df), "candle_body")
    assert result[0][1] == pytest.approx(0.0)
