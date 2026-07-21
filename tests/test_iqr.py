"""Tests for the IQR anomaly detector."""

from datetime import datetime, timedelta

import pytest
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

from analytics.iqr import (
    add_iqr_anomaly_flag,
    compute_iqr_bounds,
    detect_iqr_anomalies,
)

IQR_SCHEMA = StructType(
    [
        StructField("date", TimestampType(), True),
        StructField("close", DoubleType(), True),
    ]
)

BASE = datetime(2020, 1, 2, 9, 30)


def _series_df(spark, values):
    """Deterministic time-ordered DataFrame with one bar per minute."""
    rows = [(BASE + timedelta(minutes=i), v) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, IQR_SCHEMA)


def _flags(df):
    return [row["close_iqr_anomaly"] for row in df.orderBy("date").collect()]


# --- quartile computation ----------------------------------------------------


def test_quartiles_produce_expected_bounds(spark):
    # For 1..9: Q1=3, Q3=7, IQR=4, k=1.5 => lower=-3, upper=13.
    values = [float(v) for v in range(1, 10)]
    df = _series_df(spark, values)
    lower, upper = compute_iqr_bounds(df, "close", k=1.5, relative_error=0.0)
    assert lower == pytest.approx(-3.0)
    assert upper == pytest.approx(13.0)


def test_iqr_is_zero_when_all_values_equal(spark):
    # Degenerate distribution: Q1 == Q3 => IQR == 0 => lower == upper == the value.
    df = _series_df(spark, [50.0] * 5)
    lower, upper = compute_iqr_bounds(df, "close", k=1.5, relative_error=0.0)
    assert lower == pytest.approx(50.0)
    assert upper == pytest.approx(50.0)


def test_custom_k_widens_the_fence(spark):
    # k=3 doubles the additive term vs k=1.5 => bounds spread further out.
    values = [float(v) for v in range(1, 10)]
    df = _series_df(spark, values)
    lower_15, upper_15 = compute_iqr_bounds(df, "close", k=1.5, relative_error=0.0)
    lower_30, upper_30 = compute_iqr_bounds(df, "close", k=3.0, relative_error=0.0)
    assert lower_30 < lower_15
    assert upper_30 > upper_15


# --- flag semantics on precomputed bounds ------------------------------------


def test_value_above_upper_is_flagged(spark):
    df = _series_df(spark, [100.0])
    flagged = _flags(add_iqr_anomaly_flag(df, "close", lower=-3.0, upper=13.0))
    assert flagged[0] is True


def test_value_below_lower_is_flagged(spark):
    df = _series_df(spark, [-100.0])
    flagged = _flags(add_iqr_anomaly_flag(df, "close", lower=-3.0, upper=13.0))
    assert flagged[0] is True


def test_value_inside_fence_is_not_flagged(spark):
    df = _series_df(spark, [5.0])
    flagged = _flags(add_iqr_anomaly_flag(df, "close", lower=-3.0, upper=13.0))
    assert flagged[0] is False


def test_value_at_upper_boundary_is_not_flagged(spark):
    # Flag uses strict inequality: value > upper (not >=). value == upper stays inside.
    df = _series_df(spark, [13.0])
    flagged = _flags(add_iqr_anomaly_flag(df, "close", lower=-3.0, upper=13.0))
    assert flagged[0] is False


def test_value_at_lower_boundary_is_not_flagged(spark):
    df = _series_df(spark, [-3.0])
    flagged = _flags(add_iqr_anomaly_flag(df, "close", lower=-3.0, upper=13.0))
    assert flagged[0] is False


# --- end-to-end detector -----------------------------------------------------


def test_detect_iqr_anomalies_flags_outlier(spark):
    # 1..9 defines the fence; 100 is a clear upper-tail outlier.
    values = [float(v) for v in range(1, 10)] + [100.0]
    df = _series_df(spark, values)
    result = detect_iqr_anomalies(df, "close", k=1.5, relative_error=0.0)
    flagged = _flags(result)
    assert flagged[-1] is True
    assert all(f is False for f in flagged[:-1])
