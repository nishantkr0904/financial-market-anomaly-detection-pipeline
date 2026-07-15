"""Orchestrate the analytical dashboard for the processed OHLCV dataset.

Reads the anomaly-enriched Parquet dataset once, validates the schema
contract advertised by ``plots.py``, runs every registered plot function,
and writes a ``dashboard_summary.json`` capturing the run.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from visualizations.plots import PLOT_FUNCTIONS, REQUIRED_COLUMNS

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET_PATH = PROJECT_ROOT / "data" / "processed" / "ohlcv_anomalies"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"
SUMMARY_FILE = "dashboard_summary.json"


def _validate_columns(df: pd.DataFrame) -> None:
    """Raise ``ValueError`` listing every required column missing from ``df``."""
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(
            f"Processed dataset is missing required columns: {missing}. "
            f"Present columns: {list(df.columns)}"
        )


def render_dashboard(parquet_path: Path, output_dir: Path) -> dict:
    """Render every dashboard plot and write ``dashboard_summary.json``.

    Args:
        parquet_path: Directory or file path of the processed Parquet dataset.
        output_dir: Directory where PNGs and the summary file are written.

    Returns:
        The summary dict that was persisted to ``dashboard_summary.json``.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Processed dataset not found: {parquet_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()

    logger.info("Reading processed dataset from %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    _validate_columns(df)
    logger.info("Loaded %d rows; generating %d plots", len(df), len(PLOT_FUNCTIONS))

    generated: list[str] = []
    for plot_fn in PLOT_FUNCTIONS:
        logger.info("Rendering %s", plot_fn.__name__)
        generated.append(plot_fn(df, output_dir))

    summary = {
        "generation_timestamp": datetime.now(timezone.utc).isoformat(),
        "processed_row_count": int(len(df)),
        "generated_plots": generated,
        "execution_seconds": round(time.perf_counter() - start, 3),
    }
    summary_path = output_dir / SUMMARY_FILE
    summary_path.write_text(json.dumps(summary, indent=2))
    logger.info("Dashboard complete: %s", summary_path)

    return summary


def main() -> None:
    """CLI entry point using the pipeline's default Parquet and output paths."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    render_dashboard(DEFAULT_PARQUET_PATH, DEFAULT_OUTPUT_DIR)


if __name__ == "__main__":
    main()
