"""Google BigQuery client factory.

Reads project configuration from environment variables (loaded from ``.env``
if present) and returns a configured ``google.cloud.bigquery.Client``.

Authentication uses Application Default Credentials — the SDK resolves them
from ``GOOGLE_APPLICATION_CREDENTIALS`` without an explicit hand-off here.
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ENV = "BIGQUERY_PROJECT"
CREDENTIALS_ENV = "GOOGLE_APPLICATION_CREDENTIALS"


def validate_bigquery_config() -> None:
    """Verify the required BigQuery environment variables are set and usable.

    Loads ``.env`` into the process environment. Raises ``RuntimeError`` for a
    missing variable and ``FileNotFoundError`` for a credentials path that does
    not exist on disk.
    """
    load_dotenv()

    if not os.getenv(PROJECT_ENV):
        raise RuntimeError(f"{PROJECT_ENV} is not set")

    credentials = os.getenv(CREDENTIALS_ENV)
    if not credentials:
        raise RuntimeError(f"{CREDENTIALS_ENV} is not set")
    if not Path(credentials).exists():
        raise FileNotFoundError(f"Credentials file not found: {credentials}")


def get_bigquery_client() -> bigquery.Client:
    """Return a configured BigQuery ``Client`` bound to ``BIGQUERY_PROJECT``."""
    validate_bigquery_config()
    project = os.environ[PROJECT_ENV]
    logger.info("Creating BigQuery client for project=%s", project)
    return bigquery.Client(project=project)
