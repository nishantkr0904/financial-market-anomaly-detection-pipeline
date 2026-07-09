"""Validation checks for the raw OHLCV dataset.

Each function performs a single independent check and raises a clear exception
on failure. No mutation, no cleaning — validation only.
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType, StructType

from spark_jobs.schema import OHLCV_SCHEMA


def validate_file_exists(path: Path | str) -> None:
    """Raise FileNotFoundError if the given path does not exist."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")


def validate_schema(df: DataFrame, expected: StructType = OHLCV_SCHEMA) -> None:
    """Raise ValueError if the DataFrame schema does not match expected."""
    if df.schema != expected:
        raise ValueError(
            "Schema mismatch.\n"
            f"Expected: {expected.simpleString()}\n"
            f"Got:      {df.schema.simpleString()}"
        )


def validate_required_columns(
    df: DataFrame, required: list[str] | None = None
) -> None:
    """Raise ValueError if any required column name is absent from the DataFrame."""
    required = required or [f.name for f in OHLCV_SCHEMA.fields]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_not_empty(df: DataFrame) -> None:
    """Raise ValueError if the DataFrame contains zero rows.

    Uses ``limit(1).count()`` to short-circuit — no full scan of the input.
    """
    if df.limit(1).count() == 0:
        raise ValueError("DataFrame is empty.")


def validate_numeric_columns(df: DataFrame) -> None:
    """Raise ValueError if a column expected to be numeric is not.

    Expectations are derived from ``OHLCV_SCHEMA`` — any field whose declared
    type is a subclass of ``NumericType`` must also be numeric in ``df``.
    """
    expected_numeric = {
        f.name for f in OHLCV_SCHEMA.fields if isinstance(f.dataType, NumericType)
    }
    actual_types = {f.name: f.dataType for f in df.schema.fields}
    wrong = [
        c
        for c in expected_numeric
        if c in actual_types and not isinstance(actual_types[c], NumericType)
    ]
    if wrong:
        raise ValueError(f"Non-numeric columns where numeric expected: {wrong}")
