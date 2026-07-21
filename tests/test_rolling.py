"""Tests for the rolling statistics stage."""

from datetime import datetime, timedelta
from statistics import mean, stdev

import pytest
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

from analytics.rolling import (
    _trailing_window,
    add_rolling_mean,
    add_rolling_std,
    add_rolling_stats,
)

ROLLING_SCHEMA = StructType(
    [
        StructField("date", TimestampType(), True),
        StructField("close", DoubleType(), True),
    ]
)

BASE = datetime(2020, 1, 2, 9, 30)


def _series_df(spark, values):
    """Deterministic time-ordered DataFrame with one bar per minute."""
    rows = [(BASE + timedelta(minutes=i), v) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, ROLLING_SCHEMA)


def _column_in_order(df, column):
    return [row[column] for row in df.orderBy("date").collect()]


# --- rolling mean ------------------------------------------------------------


def test_rolling_mean_first_row_equals_value(spark):
    # Trailing window includes only the current row => mean == the value itself.
    df = _series_df(spark, [10.0, 20.0, 30.0])
    result = _column_in_order(add_rolling_mean(df, "close", window=3), "close_rolling_mean_3")
    assert result[0] == pytest.approx(10.0)


def test_rolling_mean_partial_window(spark):
    # Row 2 has only 2 prior observations => mean of first two values.
    df = _series_df(spark, [10.0, 20.0, 30.0])
    result = _column_in_order(add_rolling_mean(df, "close", window=3), "close_rolling_mean_3")
    assert result[1] == pytest.approx(mean([10.0, 20.0]))


def test_rolling_mean_full_window(spark):
    # Row 3 has all 3 observations => mean of the full window.
    df = _series_df(spark, [10.0, 20.0, 30.0])
    result = _column_in_order(add_rolling_mean(df, "close", window=3), "close_rolling_mean_3")
    assert result[2] == pytest.approx(mean([10.0, 20.0, 30.0]))


def test_rolling_mean_window_slides(spark):
    # Row 4 drops row 1 out of the window: mean == mean([20, 30, 40]).
    df = _series_df(spark, [10.0, 20.0, 30.0, 40.0])
    result = _column_in_order(add_rolling_mean(df, "close", window=3), "close_rolling_mean_3")
    assert result[3] == pytest.approx(mean([20.0, 30.0, 40.0]))


# --- rolling std -------------------------------------------------------------


def test_rolling_std_first_row_is_null(spark):
    # Sample std of a single value is undefined => Spark returns null.
    df = _series_df(spark, [10.0, 20.0, 30.0])
    result = _column_in_order(add_rolling_std(df, "close", window=3), "close_rolling_std_3")
    assert result[0] is None


def test_rolling_std_full_window(spark):
    # Sample std (n-1 denominator) matches statistics.stdev.
    values = [10.0, 20.0, 30.0]
    df = _series_df(spark, values)
    result = _column_in_order(add_rolling_std(df, "close", window=3), "close_rolling_std_3")
    assert result[2] == pytest.approx(stdev(values))


def test_rolling_std_zero_on_constant_window(spark):
    # A perfectly flat window has zero deviation.
    df = _series_df(spark, [50.0, 50.0, 50.0])
    result = _column_in_order(add_rolling_std(df, "close", window=3), "close_rolling_std_3")
    assert result[2] == pytest.approx(0.0)


# --- add_rolling_stats + null handling ---------------------------------------


def test_add_rolling_stats_adds_both_columns(spark):
    df = _series_df(spark, [10.0, 20.0, 30.0])
    result = add_rolling_stats(df, "close", window=3)
    assert "close_rolling_mean_3" in result.columns
    assert "close_rolling_std_3" in result.columns


def test_rolling_mean_skips_null_values(spark):
    # Spark's avg() ignores nulls: mean over window with a null should be the
    # mean of the non-null values.
    df = _series_df(spark, [10.0, None, 30.0])
    result = _column_in_order(add_rolling_mean(df, "close", window=3), "close_rolling_mean_3")
    assert result[2] == pytest.approx(mean([10.0, 30.0]))


# --- window size guard -------------------------------------------------------


def test_trailing_window_rejects_size_one():
    with pytest.raises(ValueError):
        _trailing_window(1)
