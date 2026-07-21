"""End-to-end orchestration of the anomaly detection pipeline.

Wires the existing stage modules in order:

    ingest -> validate -> clean -> features -> rolling stats
    -> z-score detection -> IQR detection
    -> Parquet (data/processed/) -> pandas -> BigQuery

No new business logic. All transforms live in their own modules; this script
only chains them together and handles the Spark -> pandas edge.
"""

import sys
from pathlib import Path

# Direct invocation (``python scripts/run_pipeline.py``) puts scripts/ on
# sys.path, not the project root. Prepend the root so absolute imports like
# ``from analytics.features import ...`` resolve. Idempotent under pytest and
# ``python -m`` because both already have the root on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import logging  # noqa: E402

import pandas as pd  # noqa: E402
from pyspark.sql.functions import lit  # noqa: E402

from analytics.features import add_features  # noqa: E402
from analytics.iqr import detect_iqr_anomalies  # noqa: E402
from analytics.rolling import add_rolling_stats  # noqa: E402
from analytics.zscore import detect_zscore_anomalies  # noqa: E402
from bigquery.loader import load_dataframe  # noqa: E402
from spark_jobs.clean import clean  # noqa: E402
from spark_jobs.ingest import read_ohlcv_csv  # noqa: E402
from spark_jobs.session import get_spark_session  # noqa: E402
from spark_jobs.validator import (  # noqa: E402
    validate_file_exists,
    validate_not_empty,
    validate_numeric_columns,
    validate_required_columns,
    validate_schema,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
RAW_CSV = ROOT / "data" / "raw" / "1_min_SPY_2008-2021.csv"
PROCESSED_DIR = ROOT / "data" / "processed" / "ohlcv_anomalies"

SYMBOL = "SPY"
TARGET_COLUMN = "close"
ROLLING_WINDOW = 20
ZSCORE_THRESHOLD = 3.0
IQR_K = 1.5
BQ_TABLE = "ohlcv_anomalies"


def run_pipeline() -> None:
    """Execute the full pipeline: CSV -> Spark -> Parquet -> BigQuery."""
    logger.info("Pipeline start")

    spark = get_spark_session()

    logger.info("Ingest: reading %s", RAW_CSV)
    validate_file_exists(RAW_CSV)
    df = read_ohlcv_csv(RAW_CSV, spark=spark)

    logger.info("Validate: schema, required columns, non-empty, numeric types")
    validate_schema(df)
    validate_required_columns(df)
    validate_not_empty(df)
    validate_numeric_columns(df)

    logger.info("Clean: dedupe -> parse timestamp -> drop null OHLCV -> sort")
    df = clean(df)

    logger.info("Features: simple_return, price_range, candle_body")
    df = add_features(df)

    logger.info(
        "Rolling stats: column=%s window=%d", TARGET_COLUMN, ROLLING_WINDOW
    )
    df = add_rolling_stats(df, TARGET_COLUMN, ROLLING_WINDOW)

    logger.info("Z-score detection: threshold=%.2f", ZSCORE_THRESHOLD)
    df = detect_zscore_anomalies(
        df, TARGET_COLUMN, ROLLING_WINDOW, ZSCORE_THRESHOLD
    )

    logger.info("IQR detection: k=%.2f", IQR_K)
    df = detect_iqr_anomalies(df, TARGET_COLUMN, k=IQR_K)

    # Attach the symbol tag so both Parquet and BigQuery carry it. queries.py
    # and loader.py's clustering key both assume this column exists.
    df = df.withColumn("symbol", lit(SYMBOL))

    logger.info("Writing Parquet to %s", PROCESSED_DIR)
    df.write.mode("overwrite").parquet(str(PROCESSED_DIR))

    # Convert to pandas only at the pipeline edge by re-reading the Parquet we
    # just wrote. Avoids re-executing the whole Spark DAG for toPandas().
    logger.info("Materializing pandas frame from %s", PROCESSED_DIR)
    pdf = pd.read_parquet(PROCESSED_DIR)

    logger.info("Loading BigQuery: table=%s rows=%d", BQ_TABLE, len(pdf))
    load_dataframe(pdf, table=BQ_TABLE, mode="overwrite")

    logger.info(
        "Pipeline complete: rows=%d parquet=%s bq_table=%s",
        len(pdf),
        PROCESSED_DIR,
        BQ_TABLE,
    )

    spark.stop()


def main() -> None:
    """CLI entry point: configure root logging and run the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_pipeline()


if __name__ == "__main__":
    main()
