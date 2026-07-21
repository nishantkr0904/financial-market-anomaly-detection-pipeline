import sys
from pathlib import Path

# Ensure the project root is importable regardless of how pytest is invoked.
# tests/ has no __init__.py, and bare `pytest` does not add the project root
# to sys.path — only the test file's directory. Insert the repository root
# (parent of this tests/ directory) so `import spark_jobs` resolves.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # Match configs/spark.yaml so tests exercise the same arithmetic
        # semantics as production (e.g., divide-by-zero => null, not raise).
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
