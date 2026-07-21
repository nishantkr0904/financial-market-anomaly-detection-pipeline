"""Smoke test for the end-to-end pipeline orchestration in scripts/run_pipeline.py.

Only I/O boundaries are mocked. Every Spark transform in ``analytics/`` and
``spark_jobs/`` runs unmodified.

Mocks in play
-------------
* ``read_ohlcv_csv``       -> returns a tiny in-memory DataFrame, no real CSV
* ``validate_file_exists`` -> no-op, the CSV path is not on disk in tests
* ``DataFrame.write``      -> captures the Spark DataFrame; no Parquet is
                              written to disk (avoids Hadoop/Java FS quirks)
* ``pd.read_parquet``      -> returns ``captured_spark_df.toPandas()`` so the
                              pipeline's pandas boundary still runs
* ``load_dataframe``       -> records the call, no BigQuery traffic
* ``get_spark_session``    -> returns the session-scoped test fixture
* ``spark.stop``           -> no-op, the shared session survives the test

The test therefore validates the *orchestration* — stage order, output schema,
row-count preservation, and the BigQuery boundary — without touching the
filesystem or the network.
"""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame as _AbstractDataFrame
from pyspark.sql.classic.dataframe import DataFrame as _ConcreteDataFrame

from scripts import run_pipeline as pipeline
from spark_jobs.schema import OHLCV_SCHEMA

EXPECTED_OUTPUT_COLUMNS = {
    "row_index",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "barCount",
    "average",
    "simple_return",
    "price_range",
    "candle_body",
    "close_rolling_mean_20",
    "close_rolling_std_20",
    "close_zscore_20",
    "close_zscore_anomaly_20",
    "close_iqr_anomaly",
    "symbol",
}

REQUIRED_ANOMALY_COLUMNS = {"close_zscore_anomaly_20", "close_iqr_anomaly"}

EXPECTED_STAGE_ORDER = [
    "read_ohlcv_csv",
    "clean",
    "add_features",
    "add_rolling_stats",
    "detect_zscore_anomalies",
    "detect_iqr_anomalies",
    "load_dataframe",
]

SEED_ROWS = 30


def _synthetic_ohlcv_rows(n):
    """n deterministic OHLCV rows matching OHLCV_SCHEMA — unique, non-null, sorted."""
    base = datetime(2020, 1, 2, 9, 30)
    rows = []
    for i in range(n):
        ts = base + timedelta(minutes=i)
        date_str = ts.strftime("%Y%m%d  %H:%M:%S")
        close = 100.0 + i * 0.5
        rows.append(
            (
                i,               # row_index
                date_str,        # date (string; cleaned downstream)
                close - 0.2,     # open
                close + 0.3,     # high
                close - 0.4,     # low
                close,           # close
                1000 + i,        # volume
                10,              # barCount
                close - 0.1,     # average
            )
        )
    return rows


@pytest.fixture(scope="module")
def pipeline_run(spark, tmp_path_factory):
    """Run the pipeline once with all I/O boundaries mocked. Shared across tests."""
    seed_df = spark.createDataFrame(_synthetic_ohlcv_rows(SEED_ROWS), OHLCV_SCHEMA)
    seed_row_count = seed_df.count()

    # Point PROCESSED_DIR at an isolated tmp directory as a defence in depth:
    # even if a monkeypatch ever misses, no leftover Parquet in the real
    # data/processed/ can silently satisfy a read.
    processed_dir = tmp_path_factory.mktemp("pipeline_smoke") / "ohlcv_anomalies"

    call_log: list[str] = []
    load_calls: list[dict] = []
    captured: dict = {}

    class _CaptureWriter:
        """Fluent stand-in for Spark's DataFrameWriter — records instead of writing."""

        def __init__(self, df):
            self._df = df

        def mode(self, _mode):
            return self

        def parquet(self, _path):
            captured["spark_df"] = self._df

    def fake_read_ohlcv_csv(path, spark=None):
        call_log.append("read_ohlcv_csv")
        return seed_df

    def fake_read_parquet(_path):
        # Materialize the captured Spark DataFrame as pandas, exactly as the
        # real pipeline does after its Parquet write.
        return captured["spark_df"].toPandas()

    def fake_load_dataframe(df, table, mode="append", **_kwargs):
        call_log.append("load_dataframe")
        load_calls.append({"table": table, "mode": mode, "df": df, "rows": len(df)})

    def _record(name, original):
        def wrapper(*args, **kwargs):
            call_log.append(name)
            return original(*args, **kwargs)

        return wrapper

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(pipeline, "PROCESSED_DIR", processed_dir)
        mp.setattr(pipeline, "get_spark_session", lambda: spark)
        mp.setattr(pipeline, "validate_file_exists", lambda _p: None)
        mp.setattr(pipeline, "read_ohlcv_csv", fake_read_ohlcv_csv)
        mp.setattr(pipeline, "load_dataframe", fake_load_dataframe)
        mp.setattr(pipeline.pd, "read_parquet", fake_read_parquet)
        # Patch ``DataFrame.write`` at the class level so any Spark DataFrame's
        # ``.write.mode(...).parquet(...)`` chain routes to the in-memory
        # capture instead of the local filesystem. PySpark 4 resolves ``write``
        # on the concrete ``classic.dataframe.DataFrame`` subclass, so we patch
        # both the abstract public class and the concrete one.
        capture_property = property(lambda self: _CaptureWriter(self))
        mp.setattr(_AbstractDataFrame, "write", capture_property)
        mp.setattr(_ConcreteDataFrame, "write", capture_property)
        mp.setattr(spark, "stop", lambda: None)

        for name in (
            "clean",
            "add_features",
            "add_rolling_stats",
            "detect_zscore_anomalies",
            "detect_iqr_anomalies",
        ):
            mp.setattr(pipeline, name, _record(name, getattr(pipeline, name)))

        pipeline.run_pipeline()

    return {
        "call_log": call_log,
        "load_calls": load_calls,
        "seed_row_count": seed_row_count,
        "output_df": load_calls[0]["df"],
        "captured_spark_df": captured["spark_df"],
    }


# --- orchestration order -----------------------------------------------------


def test_stages_execute_in_expected_order(pipeline_run):
    assert pipeline_run["call_log"] == EXPECTED_STAGE_ORDER


# --- output schema -----------------------------------------------------------


def test_output_contains_all_expected_columns(pipeline_run):
    assert set(pipeline_run["output_df"].columns) == EXPECTED_OUTPUT_COLUMNS


def test_output_contains_required_anomaly_columns(pipeline_run):
    output_columns = set(pipeline_run["output_df"].columns)
    assert REQUIRED_ANOMALY_COLUMNS.issubset(output_columns)


def test_output_carries_symbol_column(pipeline_run):
    # scripts/run_pipeline.py attaches a constant "symbol" after IQR detection.
    assert (pipeline_run["output_df"]["symbol"] == "SPY").all()


# --- row-count preservation --------------------------------------------------


def test_output_row_count_matches_input(pipeline_run):
    # Seed rows are unique, non-null, and time-sorted — clean() should not drop
    # any of them. Every downstream stage is a withColumn (no filter), so the
    # final row count must equal the input row count.
    assert len(pipeline_run["output_df"]) == pipeline_run["seed_row_count"]


# --- BigQuery boundary -------------------------------------------------------


def test_bigquery_load_invoked_exactly_once(pipeline_run):
    assert len(pipeline_run["load_calls"]) == 1


def test_bigquery_load_target_and_mode(pipeline_run):
    call = pipeline_run["load_calls"][0]
    assert call["table"] == pipeline.BQ_TABLE
    assert call["mode"] == "overwrite"


def test_bigquery_load_row_count_matches_output(pipeline_run):
    call = pipeline_run["load_calls"][0]
    assert call["rows"] == len(pipeline_run["output_df"])
