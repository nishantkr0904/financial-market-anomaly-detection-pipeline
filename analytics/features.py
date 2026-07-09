"""Deterministic financial features derived from cleaned OHLCV data.

Each feature is a single, column-wise transformation — no windows, no
aggregations, no anomaly detection. All operations are lazy Spark expressions.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

TIMESTAMP_COLUMN = "date"


def add_simple_return(df: DataFrame) -> DataFrame:
    """Add ``simple_return`` = (close_t - close_{t-1}) / close_{t-1}.

    Uses a time-ordered window over the whole DataFrame. The first row has a
    null return by construction (no prior close).
    """
    prev_close = lag("close").over(Window.orderBy(TIMESTAMP_COLUMN))
    return df.withColumn("simple_return", (col("close") - prev_close) / prev_close)


def add_price_range(df: DataFrame) -> DataFrame:
    """Add ``price_range`` = high - low for the bar."""
    return df.withColumn("price_range", col("high") - col("low"))


def add_candle_body(df: DataFrame) -> DataFrame:
    """Add ``candle_body`` = close - open (signed; positive = bullish bar)."""
    return df.withColumn("candle_body", col("close") - col("open"))


def add_features(df: DataFrame) -> DataFrame:
    """Add all deterministic features to the DataFrame and return it."""
    logger.info("Adding features: simple_return, price_range, candle_body")
    return add_candle_body(add_price_range(add_simple_return(df)))
