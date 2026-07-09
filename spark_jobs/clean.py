"""Deterministic cleaning of the validated OHLCV Spark DataFrame.

Applies the four required operations as a single fluent Spark plan:
dedupe -> parse timestamp -> drop rows with null OHLCV -> sort by time.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp

from spark_jobs.schema import OHLCV_SCHEMA

logger = logging.getLogger(__name__)

TIMESTAMP_COLUMN = "date"
# Source uses a double space between the date and time components.
TIMESTAMP_FORMAT = "yyyyMMdd  HH:mm:ss"


def clean(df: DataFrame) -> DataFrame:
    """Return a cleaned DataFrame: deduped, timestamp-parsed, non-null, sorted."""
    required = [f.name for f in OHLCV_SCHEMA.fields]
    logger.info(
        "Building clean plan: dropDuplicates -> to_timestamp(%s) -> dropna(%s) -> orderBy(%s)",
        TIMESTAMP_COLUMN,
        required,
        TIMESTAMP_COLUMN,
    )
    return (
        df.dropDuplicates()
        .withColumn(TIMESTAMP_COLUMN, to_timestamp(TIMESTAMP_COLUMN, TIMESTAMP_FORMAT))
        .dropna(subset=required)
        .orderBy(TIMESTAMP_COLUMN)
    )
