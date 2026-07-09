"""IQR-based anomaly detection over a Spark DataFrame.

Formula
-------
    Q1  = 25th percentile of the column
    Q3  = 75th percentile of the column
    IQR = Q3 - Q1
    lower_bound = Q1 - k * IQR
    upper_bound = Q3 + k * IQR
    anomaly     = value < lower_bound OR value > upper_bound

Tukey's default fence is ``k = 1.5``. Quantiles are computed with
``approxQuantile`` — a single-pass, distributed algorithm whose accuracy is
governed by ``relative_error``.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

DEFAULT_K = 1.5
DEFAULT_RELATIVE_ERROR = 0.01


def compute_iqr_bounds(
    df: DataFrame,
    column: str,
    k: float = DEFAULT_K,
    relative_error: float = DEFAULT_RELATIVE_ERROR,
) -> tuple[float, float]:
    """Return ``(lower_bound, upper_bound)`` for ``column`` using Tukey's IQR rule."""
    q1, q3 = df.approxQuantile(column, [0.25, 0.75], relative_error)
    iqr = q3 - q1
    lower = q1 - k * iqr
    upper = q3 + k * iqr
    logger.info(
        "IQR bounds for %s: Q1=%.6f Q3=%.6f IQR=%.6f lower=%.6f upper=%.6f (k=%.2f)",
        column,
        q1,
        q3,
        iqr,
        lower,
        upper,
        k,
    )
    return lower, upper


def add_iqr_anomaly_flag(
    df: DataFrame, column: str, lower: float, upper: float
) -> DataFrame:
    """Add ``{column}_iqr_anomaly`` = value is outside [lower, upper]."""
    flag_col = f"{column}_iqr_anomaly"
    return df.withColumn(flag_col, (col(column) < lower) | (col(column) > upper))


def detect_iqr_anomalies(
    df: DataFrame,
    column: str,
    k: float = DEFAULT_K,
    relative_error: float = DEFAULT_RELATIVE_ERROR,
) -> DataFrame:
    """Compute IQR bounds for ``column`` and add the anomaly flag column."""
    lower, upper = compute_iqr_bounds(df, column, k, relative_error)
    return add_iqr_anomaly_flag(df, column, lower, upper)
