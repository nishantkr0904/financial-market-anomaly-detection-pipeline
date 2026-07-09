"""Explicit Spark schema for the raw 1-minute SPY OHLCV dataset.

Source file: data/raw/1_min_SPY_2008-2021.csv

Columns (in order):
    date      — timestamp string formatted "YYYYMMDD  HH:MM:SS"
                (double-space separator; parsed downstream, kept as string here)
    open      — opening price of the 1-minute bar
    high      — highest price during the bar
    low       — lowest price during the bar
    close     — closing price of the bar
    volume    — shares traded during the bar
    barCount  — number of ticks aggregated into the bar
    average   — volume-weighted average price of the bar

An explicit schema is preferred over inference: it avoids a full-file scan on
read, guarantees stable column types across runs, and fails fast if the source
file drifts from the expected contract.
"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

OHLCV_SCHEMA: StructType = StructType(
    [
        StructField("date", StringType(), nullable=False),
        StructField("open", DoubleType(), nullable=False),
        StructField("high", DoubleType(), nullable=False),
        StructField("low", DoubleType(), nullable=False),
        StructField("close", DoubleType(), nullable=False),
        StructField("volume", LongType(), nullable=False),
        StructField("barCount", LongType(), nullable=False),
        StructField("average", DoubleType(), nullable=False),
    ]
)
