"""Rolling statistical features over a time-ordered OHLCV Spark DataFrame.

Adds trailing rolling mean and rolling sample standard deviation for a chosen
column. No anomaly logic — those consume these statistics downstream.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, stddev
from pyspark.sql.window import Window, WindowSpec

logger = logging.getLogger(__name__)

TIMESTAMP_COLUMN = "date"


def _trailing_window(size: int) -> WindowSpec:
    """Time-ordered window covering the current row and the prior ``size - 1``."""
    if size < 2:
        raise ValueError(f"window size must be >= 2, got {size}")
    return Window.orderBy(TIMESTAMP_COLUMN).rowsBetween(-(size - 1), 0)


def add_rolling_mean(df: DataFrame, column: str, window: int) -> DataFrame:
    """Add ``{column}_rolling_mean_{window}`` computed over a trailing window."""
    return df.withColumn(
        f"{column}_rolling_mean_{window}",
        avg(column).over(_trailing_window(window)),
    )


def add_rolling_std(df: DataFrame, column: str, window: int) -> DataFrame:
    """Add ``{column}_rolling_std_{window}`` (sample std dev) over a trailing window."""
    return df.withColumn(
        f"{column}_rolling_std_{window}",
        stddev(column).over(_trailing_window(window)),
    )


def add_rolling_stats(df: DataFrame, column: str, window: int) -> DataFrame:
    """Add both rolling mean and rolling std for ``column`` over ``window`` rows."""
    logger.info("Adding rolling mean and std for %s over %d-row window", column, window)
    df = add_rolling_mean(df, column, window)
    df = add_rolling_std(df, column, window)
    return df
