"""Load processed pandas DataFrames into BigQuery with cost-aware storage.

Creates the target dataset on demand and appends or overwrites the destination
table using the ``google-cloud-bigquery`` SDK. The input DataFrame must already
be materialized in pandas — e.g. ``spark_df.toPandas()`` at the pipeline edge.

Storage layout
--------------
Tables are created with DAY partitioning on the ``date`` column and clustered
by ``symbol``. Both are configured on the first load via ``LoadJobConfig`` and
are idempotent on subsequent loads.

Why DATE partitioning
    BigQuery bills on bytes scanned. Partitioning splits the table into
    per-day physical units; a query that filters ``WHERE date BETWEEN ...``
    scans only the matching partitions instead of the full table. Every
    time-series query in this project — rolling windows, daily aggregates,
    dashboard tiles — filters by date, so partition pruning maps directly
    onto the workload.

Why DAY granularity
    The raw data is 1-minute bars and the analytics roll up to trading days.
    A DAY partition is the natural join between storage and query.

Why SYMBOL clustering
    Clustering physically co-locates rows that share the cluster key inside
    each partition. Predicates like ``WHERE symbol = 'SPY'`` and ``GROUP BY
    symbol`` skip most row groups without decoding them. Symbol has moderate
    cardinality (hundreds of tickers) — the sweet spot for BigQuery
    clustering. Clustering has no storage cost; only a slightly heavier load.

Why partition expiration is opt-in
    The default is no expiration — this is a historical research dataset and
    losing older partitions defeats the purpose. Callers running sandbox or
    staging loads can pass ``partition_expiration_days`` to cap storage.

Query hygiene not enforced here but recommended
    - Select named columns, not ``SELECT *`` — BigQuery is columnar; unused
      columns are still billed.
    - Every ad-hoc query should include a ``date`` filter; without one,
      partition pruning cannot help.
    - Materialize expensive aggregates into a smaller downstream table rather
      than re-scanning the raw table from every dashboard.
"""

import logging
import os
from collections.abc import Sequence
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from bigquery.client import get_bigquery_client

logger = logging.getLogger(__name__)

DATASET_ENV = "BIGQUERY_DATASET"

WriteMode = Literal["append", "overwrite"]

DEFAULT_PARTITION_FIELD = "date"
DEFAULT_CLUSTERING_FIELDS: tuple[str, ...] = ("symbol",)
MS_PER_DAY = 24 * 60 * 60 * 1000


def load_dataframe(
    df: pd.DataFrame,
    table: str,
    mode: WriteMode = "append",
    client: bigquery.Client | None = None,
    partition_field: str | None = DEFAULT_PARTITION_FIELD,
    clustering_fields: Sequence[str] | None = DEFAULT_CLUSTERING_FIELDS,
    partition_expiration_days: int | None = None,
) -> None:
    """Load ``df`` into ``{project}.{dataset}.{table}`` with storage layout.

    On first load the table is created with DAY partitioning on
    ``partition_field`` and clustered by ``clustering_fields``. Both settings
    are re-sent on every load; BigQuery ignores them once the table exists,
    so the call remains idempotent.

    Args:
        df: The DataFrame to load.
        table: Unqualified table name inside the configured dataset.
        mode: ``"append"`` (default) or ``"overwrite"``.
        client: Existing BigQuery client; a fresh one is built if omitted.
        partition_field: DATE/TIMESTAMP column to partition by, or ``None``
            to disable partitioning (not recommended for time-series data).
        clustering_fields: Columns to cluster on, or ``None`` / empty to
            disable clustering.
        partition_expiration_days: Per-partition TTL in days. Applied only
            on table creation; leave ``None`` to retain partitions forever.
    """
    client = client or get_bigquery_client()

    dataset_id = os.getenv(DATASET_ENV)
    if not dataset_id:
        raise RuntimeError(f"{DATASET_ENV} is not set")

    logger.info("Ensuring dataset %s.%s exists", client.project, dataset_id)
    client.create_dataset(dataset_id, exists_ok=True)

    disposition = "WRITE_TRUNCATE" if mode == "overwrite" else "WRITE_APPEND"
    table_ref = f"{client.project}.{dataset_id}.{table}"

    job_config = bigquery.LoadJobConfig(write_disposition=disposition)
    if partition_field:
        expiration_ms = (
            partition_expiration_days * MS_PER_DAY
            if partition_expiration_days
            else None
        )
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
            expiration_ms=expiration_ms,
        )
    if clustering_fields:
        job_config.clustering_fields = list(clustering_fields)

    logger.info(
        "Loading %d rows into %s (mode=%s partition=%s cluster=%s ttl_days=%s)",
        len(df),
        table_ref,
        mode,
        partition_field,
        list(clustering_fields) if clustering_fields else None,
        partition_expiration_days,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    logger.info(
        "Load complete: table=%s output_rows=%s output_bytes=%s",
        table_ref,
        job.output_rows,
        job.output_bytes,
    )
