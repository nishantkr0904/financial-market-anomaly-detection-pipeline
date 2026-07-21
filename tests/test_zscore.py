"""Tests for the rolling z-score anomaly detector."""

from datetime import datetime, timedelta

import pytest
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

from analytics.zscore import (
    add_rolling_zscore,
    add_zscore_anomaly_flag,
    detect_zscore_anomalies,
)

# The detector consumes precomputed rolling stats, so the test schema mirrors
# what analytics.rolling would have produced upstream. Building the stats
# columns explicitly here keeps every test's expected z-score hand-verifiable.
ZSCORE_SCHEMA = StructType(
    [
        StructField("date", TimestampType(), True),
        StructField("close", DoubleType(), True),
        StructField("close_rolling_mean_3", DoubleType(), True),
        StructField("close_rolling_std_3", DoubleType(), True),
    ]
)

BASE = datetime(2020, 1, 2, 9, 30)


def _zscore_df(spark, rows):
    """Build a z-score input DataFrame; ``rows`` is a list of (close, mean, std)."""
    materialized = [
        (BASE + timedelta(minutes=i), close, mean, std)
        for i, (close, mean, std) in enumerate(rows)
    ]
    return spark.createDataFrame(materialized, ZSCORE_SCHEMA)


def _rows(df):
    return df.orderBy("date").collect()


# --- z-score arithmetic ------------------------------------------------------


def test_zscore_matches_formula(spark):
    # z = (close - mean) / std => (110 - 100) / 5 == 2.0
    df = _zscore_df(spark, [(110.0, 100.0, 5.0)])
    result = _rows(add_rolling_zscore(df, "close", window=3))
    assert result[0]["close_zscore_3"] == pytest.approx(2.0)


def test_zscore_zero_when_value_equals_mean(spark):
    # z = (100 - 100) / 5 == 0
    df = _zscore_df(spark, [(100.0, 100.0, 5.0)])
    result = _rows(add_rolling_zscore(df, "close", window=3))
    assert result[0]["close_zscore_3"] == pytest.approx(0.0)


def test_zscore_null_when_std_is_null(spark):
    # Start-of-series row: std is null => z-score must be null.
    df = _zscore_df(spark, [(110.0, 100.0, None)])
    result = _rows(add_rolling_zscore(df, "close", window=3))
    assert result[0]["close_zscore_3"] is None


def test_zscore_null_when_std_is_zero(spark):
    # A perfectly flat window has std == 0. In production the SparkSession
    # runs with spark.sql.ansi.enabled=false so the (x - mean) / 0 division
    # yields null rather than raising — the module docstring's stated contract.
    df = _zscore_df(spark, [(101.0, 100.0, 0.0)])
    result = _rows(add_rolling_zscore(df, "close", window=3))
    assert result[0]["close_zscore_3"] is None


# --- threshold flag ----------------------------------------------------------


def test_anomaly_flag_true_when_zscore_exceeds_threshold(spark):
    # |(120 - 100) / 5| = 4.0 > 3.0
    df = _zscore_df(spark, [(120.0, 100.0, 5.0)])
    result = _rows(detect_zscore_anomalies(df, "close", window=3, threshold=3.0))
    assert result[0]["close_zscore_anomaly_3"] is True


def test_anomaly_flag_true_for_negative_extreme(spark):
    # |(80 - 100) / 5| = 4.0 > 3.0; the flag uses abs(), not the signed z.
    df = _zscore_df(spark, [(80.0, 100.0, 5.0)])
    result = _rows(detect_zscore_anomalies(df, "close", window=3, threshold=3.0))
    assert result[0]["close_zscore_anomaly_3"] is True


def test_anomaly_flag_false_within_threshold(spark):
    # |(110 - 100) / 5| = 2.0 < 3.0
    df = _zscore_df(spark, [(110.0, 100.0, 5.0)])
    result = _rows(detect_zscore_anomalies(df, "close", window=3, threshold=3.0))
    assert result[0]["close_zscore_anomaly_3"] is False


def test_anomaly_flag_false_at_boundary(spark):
    # |z| == threshold is NOT an anomaly: the check is strict > threshold.
    df = _zscore_df(spark, [(115.0, 100.0, 5.0)])  # z == 3.0 exactly
    result = _rows(detect_zscore_anomalies(df, "close", window=3, threshold=3.0))
    assert result[0]["close_zscore_anomaly_3"] is False


def test_anomaly_flag_null_when_zscore_null(spark):
    # Undefined z-score must not be flagged as an anomaly. Spark's abs(null) > k
    # evaluates to null, which is falsy in Python's `is True` check.
    df = _zscore_df(spark, [(110.0, 100.0, None)])
    result = _rows(add_zscore_anomaly_flag(df.transform(
        lambda d: add_rolling_zscore(d, "close", window=3)
    ), "close", window=3, threshold=3.0))
    assert result[0]["close_zscore_anomaly_3"] is None
