"""Load processed pandas DataFrames into BigQuery.

Creates the target dataset on demand and appends or overwrites the destination
table using the ``google-cloud-bigquery`` SDK. The input DataFrame must already
be materialized in pandas — e.g. ``spark_df.toPandas()`` at the pipeline edge.
"""

import logging
import os
from typing import Literal

import pandas as pd
from google.cloud import bigquery

from bigquery.client import get_bigquery_client

logger = logging.getLogger(__name__)

DATASET_ENV = "BIGQUERY_DATASET"

WriteMode = Literal["append", "overwrite"]


def load_dataframe(
    df: pd.DataFrame,
    table: str,
    mode: WriteMode = "append",
    client: bigquery.Client | None = None,
) -> None:
    """Load ``df`` into ``{project}.{dataset}.{table}``.

    The dataset is created if it does not exist; the table is created by
    BigQuery on the first load using the pandas dtypes as the schema. Blocks
    until the load job completes and logs its output statistics.
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

    logger.info("Loading %d rows into %s (mode=%s)", len(df), table_ref, mode)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    logger.info(
        "Load complete: table=%s output_rows=%s output_bytes=%s",
        table_ref,
        job.output_rows,
        job.output_bytes,
    )
