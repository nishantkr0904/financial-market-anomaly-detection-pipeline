"""Ingest raw OHLCV CSV files into a Spark DataFrame.

Reads from data/raw using the project's SparkSession and schema. No cleaning,
parsing, or transformation is done here — those belong to downstream stages.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from spark_jobs.schema import OHLCV_SCHEMA
from spark_jobs.session import get_spark_session

logger = logging.getLogger(__name__)


def read_ohlcv_csv(
    path: Path | str,
    spark: SparkSession | None = None,
) -> DataFrame:
    """Read an OHLCV CSV (or directory of CSVs) into a Spark DataFrame.

    Args:
        path: File or directory under data/raw. Accepts str or Path.
        spark: An existing SparkSession. If None, one is created from
            configs/spark.yaml via ``get_spark_session``.

    Returns:
        A Spark DataFrame conforming to ``OHLCV_SCHEMA``.

    Raises:
        FileNotFoundError: The given path does not exist on the local
            filesystem. Raised early so we don't surface an opaque Spark
            error deep inside the JVM.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"CSV path not found: {path}")

    spark = spark or get_spark_session()

    logger.info("Reading OHLCV CSV from %s", path)
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(OHLCV_SCHEMA)
        .csv(str(path))
    )
    logger.info(
        "Loaded DataFrame: columns=%s partitions=%d",
        df.columns,
        df.rdd.getNumPartitions(),
    )
    return df
