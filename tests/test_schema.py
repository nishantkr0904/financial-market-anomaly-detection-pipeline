"""Tests for the OHLCV ingest schema contract."""

import pytest
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.schema import OHLCV_SCHEMA

EXPECTED_COLUMN_ORDER = [
    "row_index",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "barCount",
    "average",
]

EXPECTED_TYPES = {
    "row_index": LongType(),
    "date": StringType(),
    "open": DoubleType(),
    "high": DoubleType(),
    "low": DoubleType(),
    "close": DoubleType(),
    "volume": LongType(),
    "barCount": LongType(),
    "average": DoubleType(),
}


def test_all_expected_columns_present():
    assert set(OHLCV_SCHEMA.fieldNames()) == set(EXPECTED_COLUMN_ORDER)


def test_column_ordering_is_stable():
    assert OHLCV_SCHEMA.fieldNames() == EXPECTED_COLUMN_ORDER


@pytest.mark.parametrize("column, expected_type", list(EXPECTED_TYPES.items()))
def test_column_has_expected_type(column, expected_type):
    assert OHLCV_SCHEMA[column].dataType == expected_type


def test_schema_rejects_wrong_type():
    wrong = StructType(
        [
            StructField(name, LongType() if name == "close" else field.dataType,
                        nullable=field.nullable)
            for name, field in zip(OHLCV_SCHEMA.fieldNames(), OHLCV_SCHEMA.fields)
        ]
    )
    assert wrong != OHLCV_SCHEMA


def test_schema_rejects_extra_column():
    extended = StructType(list(OHLCV_SCHEMA.fields) +
                          [StructField("extra", StringType(), True)])
    assert extended != OHLCV_SCHEMA
