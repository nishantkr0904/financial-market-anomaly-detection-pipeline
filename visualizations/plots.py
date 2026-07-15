"""Standalone analytical plots for the anomaly-enriched OHLCV dataset.

Each plotting function accepts the pre-loaded DataFrame and the output
directory, writes a single PNG, and returns the file name it wrote. The
dashboard module is responsible for reading the Parquet dataset once,
validating columns, and orchestrating the calls.

Style is applied once at module load: seaborn ``whitegrid``, 300 DPI, and
consistent figure sizes for time-series vs. aggregation plots.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid")

DPI = 300
FIGSIZE_WIDE = (14, 6)
FIGSIZE_SQUARE = (10, 8)

TIMESTAMP_COLUMN = "date"
CLOSE_COLUMN = "close"
VOLUME_COLUMN = "volume"
ZSCORE_COLUMN = "close_zscore_20"
ROLLING_MEAN_COLUMN = "close_rolling_mean_20"
ROLLING_STD_COLUMN = "close_rolling_std_20"
ZSCORE_ANOMALY_COLUMN = "close_zscore_anomaly_20"
IQR_ANOMALY_COLUMN = "close_iqr_anomaly"

REQUIRED_COLUMNS = [
    TIMESTAMP_COLUMN,
    "open",
    "high",
    "low",
    CLOSE_COLUMN,
    VOLUME_COLUMN,
    "average",
    "simple_return",
    "price_range",
    "candle_body",
    ROLLING_MEAN_COLUMN,
    ROLLING_STD_COLUMN,
    ZSCORE_COLUMN,
    ZSCORE_ANOMALY_COLUMN,
    IQR_ANOMALY_COLUMN,
]

CORRELATION_FEATURES = [
    "open",
    "high",
    "low",
    CLOSE_COLUMN,
    VOLUME_COLUMN,
    "average",
    "simple_return",
    "price_range",
    "candle_body",
    ROLLING_MEAN_COLUMN,
    ROLLING_STD_COLUMN,
    ZSCORE_COLUMN,
]

CORRELATION_SAMPLE_SIZE = 100_000
CORRELATION_SAMPLE_SEED = 42

ZSCORE_THRESHOLD = 3.0


def _save_figure(fig: plt.Figure, output_dir: Path, filename: str) -> str:
    """Apply ``tight_layout``, save at 300 DPI, close the figure."""
    fig.tight_layout()
    fig.savefig(output_dir / filename, dpi=DPI)
    plt.close(fig)
    return filename


def _daily_series(df: pd.DataFrame, column: str) -> pd.Series:
    """Downsample a column to daily mean, indexed by trading day."""
    return (
        df[[TIMESTAMP_COLUMN, column]]
        .set_index(TIMESTAMP_COLUMN)[column]
        .resample("1D")
        .mean()
        .dropna()
    )


def plot_price_trend(df: pd.DataFrame, output_dir: Path) -> str:
    """Daily close price with z-score and IQR anomaly markers overlaid.

    Close is downsampled to daily mean for readability; anomalies are drawn
    from the full-resolution DataFrame so the exact flagged bars remain
    visible on top of the smoothed trend line.
    """
    daily_close = _daily_series(df, CLOSE_COLUMN)

    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    ax.plot(daily_close.index, daily_close.values, color="steelblue",
            linewidth=1.0, label="Daily mean close")

    zscore_anomalies = df.loc[df[ZSCORE_ANOMALY_COLUMN] == True]  # noqa: E712
    if not zscore_anomalies.empty:
        ax.scatter(zscore_anomalies[TIMESTAMP_COLUMN], zscore_anomalies[CLOSE_COLUMN],
                   s=8, color="crimson", alpha=0.5,
                   label=f"Z-score anomalies ({len(zscore_anomalies):,})")

    iqr_anomalies = df.loc[df[IQR_ANOMALY_COLUMN] == True]  # noqa: E712
    if not iqr_anomalies.empty:
        ax.scatter(iqr_anomalies[TIMESTAMP_COLUMN], iqr_anomalies[CLOSE_COLUMN],
                   s=8, color="darkorange", alpha=0.5,
                   label=f"IQR anomalies ({len(iqr_anomalies):,})")

    ax.set_title("SPY Close Price with Detected Anomalies (2008-2021)")
    ax.set_xlabel("Trading day")
    ax.set_ylabel("Close price (USD)")
    ax.legend(loc="upper left")
    return _save_figure(fig, output_dir, "price_trend_with_anomalies.png")


def plot_rolling_zscore(df: pd.DataFrame, output_dir: Path) -> str:
    """Daily-mean rolling z-score with dashed threshold lines at ±3."""
    daily_z = _daily_series(df, ZSCORE_COLUMN)

    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    ax.plot(daily_z.index, daily_z.values, color="steelblue", linewidth=0.9)
    ax.axhline(ZSCORE_THRESHOLD, color="crimson", linestyle="--", linewidth=1,
               label=f"±{ZSCORE_THRESHOLD:.0f} threshold")
    ax.axhline(-ZSCORE_THRESHOLD, color="crimson", linestyle="--", linewidth=1)
    ax.axhline(0, color="grey", linewidth=0.5)

    ax.set_title("Rolling Z-Score of Close Price (Window = 20 bars)")
    ax.set_xlabel("Trading day")
    ax.set_ylabel("Z-score")
    ax.legend(loc="upper left")
    return _save_figure(fig, output_dir, "rolling_zscore.png")


def plot_rolling_stats(df: pd.DataFrame, output_dir: Path) -> str:
    """Daily-mean rolling mean and rolling standard deviation, stacked."""
    daily_mean = _daily_series(df, ROLLING_MEAN_COLUMN)
    daily_std = _daily_series(df, ROLLING_STD_COLUMN)

    fig, (ax_mean, ax_std) = plt.subplots(2, 1, figsize=FIGSIZE_WIDE, sharex=True)
    ax_mean.plot(daily_mean.index, daily_mean.values, color="steelblue", linewidth=1.0)
    ax_mean.set_title("Rolling Mean and Rolling Std of Close Price (Window = 20 bars)")
    ax_mean.set_ylabel("Rolling mean (USD)")

    ax_std.plot(daily_std.index, daily_std.values, color="darkorange", linewidth=1.0)
    ax_std.set_xlabel("Trading day")
    ax_std.set_ylabel("Rolling std (USD)")
    return _save_figure(fig, output_dir, "rolling_mean_std.png")


def plot_iqr_anomaly_distribution(df: pd.DataFrame, output_dir: Path) -> str:
    """Overlaid histograms of close price for IQR-normal vs. IQR-anomaly rows."""
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    normal = df.loc[df[IQR_ANOMALY_COLUMN] == False, CLOSE_COLUMN]  # noqa: E712
    anomaly = df.loc[df[IQR_ANOMALY_COLUMN] == True, CLOSE_COLUMN]  # noqa: E712

    ax.hist(normal, bins=80, color="steelblue", alpha=0.7, label=f"Normal ({len(normal):,})")
    if not anomaly.empty:
        ax.hist(anomaly, bins=80, color="crimson", alpha=0.7,
                label=f"IQR anomaly ({len(anomaly):,})")
    else:
        ax.text(0.98, 0.95, "No IQR anomalies detected",
                transform=ax.transAxes, ha="right", va="top",
                fontsize=11, color="crimson",
                bbox=dict(facecolor="white", edgecolor="crimson", boxstyle="round"))

    ax.set_title("Close Price Distribution by IQR Anomaly Flag")
    ax.set_xlabel("Close price (USD)")
    ax.set_ylabel("Frequency")
    ax.legend(loc="upper right")
    return _save_figure(fig, output_dir, "iqr_anomaly_distribution.png")


def plot_volume_distribution(df: pd.DataFrame, output_dir: Path) -> str:
    """Histogram of per-minute trading volume on a log-scaled x axis."""
    volume = df.loc[df[VOLUME_COLUMN] > 0, VOLUME_COLUMN]

    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    log_bins = np.logspace(np.log10(volume.min()), np.log10(volume.max()), 80)
    ax.hist(volume, bins=log_bins, color="steelblue", alpha=0.85)
    ax.set_xscale("log")
    ax.set_title("Per-Minute Trading Volume Distribution")
    ax.set_xlabel("Volume (log scale)")
    ax.set_ylabel("Frequency")
    return _save_figure(fig, output_dir, "volume_distribution.png")


def plot_correlation_heatmap(df: pd.DataFrame, output_dir: Path) -> str:
    """Pearson correlation heatmap of the numerical feature set.

    Uses a deterministic 100k-row sample (``random_state=42``) instead of
    the full 2M-row dataset. Correlation over the sample is statistically
    indistinguishable from the full population while keeping this plot
    fast and reproducible across runs.
    """
    sample_size = min(CORRELATION_SAMPLE_SIZE, len(df))
    sample = df[CORRELATION_FEATURES].sample(
        n=sample_size, random_state=CORRELATION_SAMPLE_SEED
    )
    corr = sample.corr()

    fig, ax = plt.subplots(figsize=FIGSIZE_SQUARE)
    sns.heatmap(
        corr, annot=True, fmt=".2f", cmap="coolwarm", vmin=-1, vmax=1,
        square=True, cbar_kws={"shrink": 0.8}, ax=ax,
    )
    ax.set_title(
        f"Feature Correlation Heatmap (sample n={sample_size:,}, seed={CORRELATION_SAMPLE_SEED})"
    )
    return _save_figure(fig, output_dir, "correlation_heatmap.png")


def _anomaly_counts_by_period(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Group flagged anomalies by the given pandas offset alias."""
    period = df[TIMESTAMP_COLUMN].dt.to_period(freq).dt.to_timestamp()
    zscore_flags = df[ZSCORE_ANOMALY_COLUMN] == True  # noqa: E712
    iqr_flags = df[IQR_ANOMALY_COLUMN] == True  # noqa: E712
    return pd.DataFrame({
        "zscore": zscore_flags.groupby(period).sum(),
        "iqr": iqr_flags.groupby(period).sum(),
    }).fillna(0)


def plot_daily_anomaly_frequency(df: pd.DataFrame, output_dir: Path) -> str:
    """Daily count of z-score and IQR anomalies, drawn as stacked bars."""
    counts = _anomaly_counts_by_period(df, "D")
    counts = counts.loc[(counts["zscore"] + counts["iqr"]) > 0]

    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    if counts.empty:
        ax.text(0.5, 0.5, "No anomalies detected", transform=ax.transAxes,
                ha="center", va="center", fontsize=12)
    else:
        ax.bar(counts.index, counts["zscore"], color="crimson",
               label="Z-score anomalies", width=1.0)
        ax.bar(counts.index, counts["iqr"], bottom=counts["zscore"],
               color="darkorange", label="IQR anomalies", width=1.0)
        ax.legend(loc="upper left")

    ax.set_title("Daily Anomaly Frequency")
    ax.set_xlabel("Trading day")
    ax.set_ylabel("Anomaly count")
    return _save_figure(fig, output_dir, "daily_anomaly_frequency.png")


def plot_monthly_anomaly_frequency(df: pd.DataFrame, output_dir: Path) -> str:
    """Monthly count of z-score and IQR anomalies, drawn as stacked bars."""
    counts = _anomaly_counts_by_period(df, "M")

    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    if counts.empty or (counts.sum().sum() == 0):
        ax.text(0.5, 0.5, "No anomalies detected", transform=ax.transAxes,
                ha="center", va="center", fontsize=12)
    else:
        ax.bar(counts.index, counts["zscore"], color="crimson",
               label="Z-score anomalies", width=25)
        ax.bar(counts.index, counts["iqr"], bottom=counts["zscore"],
               color="darkorange", label="IQR anomalies", width=25)
        ax.legend(loc="upper left")

    ax.set_title("Monthly Anomaly Frequency")
    ax.set_xlabel("Month")
    ax.set_ylabel("Anomaly count")
    return _save_figure(fig, output_dir, "monthly_anomaly_frequency.png")


def plot_top_anomaly_days(df: pd.DataFrame, output_dir: Path, top_n: int = 20) -> str:
    """Horizontal bar chart of the ``top_n`` trading days ranked by anomaly count."""
    day = df[TIMESTAMP_COLUMN].dt.date
    zscore_flags = df[ZSCORE_ANOMALY_COLUMN] == True  # noqa: E712
    iqr_flags = df[IQR_ANOMALY_COLUMN] == True  # noqa: E712
    combined = (zscore_flags | iqr_flags).groupby(day).sum()
    top = combined.sort_values(ascending=False).head(top_n)
    top = top.loc[top > 0].sort_values()

    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    if top.empty:
        ax.text(0.5, 0.5, "No anomalies detected", transform=ax.transAxes,
                ha="center", va="center", fontsize=12)
    else:
        positions = np.arange(len(top))
        labels = [d.isoformat() for d in top.index]
        ax.barh(positions, top.values, color="crimson")
        ax.set_yticks(positions)
        ax.set_yticklabels(labels)
        for y, value in enumerate(top.values):
            ax.text(value, y, f" {int(value)}", va="center", fontsize=9)

    ax.set_title(f"Top {top_n} Trading Days by Anomaly Count")
    ax.set_xlabel("Anomaly count")
    ax.set_ylabel("Trading day")
    return _save_figure(fig, output_dir, "top_anomaly_days.png")


PLOT_FUNCTIONS = (
    plot_price_trend,
    plot_rolling_zscore,
    plot_rolling_stats,
    plot_iqr_anomaly_distribution,
    plot_volume_distribution,
    plot_correlation_heatmap,
    plot_daily_anomaly_frequency,
    plot_monthly_anomaly_frequency,
    plot_top_anomaly_days,
)
