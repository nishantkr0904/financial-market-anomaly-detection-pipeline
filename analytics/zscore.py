"""Rolling Z-score anomaly detection.

Given a column and its precomputed rolling mean/std (from ``analytics.rolling``),
add a z-score column and a boolean anomaly flag.

Formula
-------
    z_t = (x_t - rolling_mean_t) / rolling_std_t
    anomaly_t = |z_t| > threshold

The z-score is null wherever ``rolling_std`` is null (start of series) or zero
(constant window). Both are safe: comparison against a null threshold yields
null, which is not a true anomaly.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = 3.0


def add_rolling_zscore(df: DataFrame, column: str, window: int) -> DataFrame:
    """Add ``{column}_zscore_{window}`` from precomputed rolling stats."""
    mean_col = f"{column}_rolling_mean_{window}"
    std_col = f"{column}_rolling_std_{window}"
    zscore_col = f"{column}_zscore_{window}"
    return df.withColumn(zscore_col, (col(column) - col(mean_col)) / col(std_col))


def add_zscore_anomaly_flag(
    df: DataFrame,
    column: str,
    window: int,
    threshold: float = DEFAULT_THRESHOLD,
) -> DataFrame:
    """Add ``{column}_zscore_anomaly_{window}`` = |z| > threshold."""
    zscore_col = f"{column}_zscore_{window}"
    flag_col = f"{column}_zscore_anomaly_{window}"
    return df.withColumn(flag_col, spark_abs(col(zscore_col)) > threshold)


def detect_zscore_anomalies(
    df: DataFrame,
    column: str,
    window: int,
    threshold: float = DEFAULT_THRESHOLD,
) -> DataFrame:
    """Add rolling z-score and anomaly flag for ``column`` at ``window``.

    Requires that ``{column}_rolling_mean_{window}`` and
    ``{column}_rolling_std_{window}`` already exist on ``df``.
    """
    logger.info(
        "Z-score anomaly detection on %s (window=%d, threshold=%.2f)",
        column,
        window,
        threshold,
    )
    df = add_rolling_zscore(df, column, window)
    df = add_zscore_anomaly_flag(df, column, window, threshold)
    return df
