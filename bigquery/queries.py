"""Reusable analytical SQL against the BigQuery table loaded by ``loader.py``.

Each function returns a SQL string ready to pass to ``client.query(...)``.
Nothing here executes SQL, opens a client, or configures storage layout.

Expected schema
---------------
The target table is the anomaly-enriched OHLCV table produced by the pipeline
and includes at least these columns:

    date            TIMESTAMP  -- 1-minute bar timestamp
    symbol          STRING     -- ticker (e.g. "SPY")
    volume          INTEGER    -- shares traded in the bar
    simple_return   FLOAT64    -- from analytics.features
    <anomaly_col>   BOOLEAN    -- e.g. close_zscore_anomaly_20, close_iqr_anomaly

Interpolation
-------------
Table and column identifiers are interpolated with f-strings — BigQuery does
not accept bind parameters for identifiers. Numeric arguments are coerced
with ``int(...)`` before interpolation as a small defence against typos and
accidental floats. These queries are called by trusted internal code, not
user input.
"""


def average_daily_return(table: str) -> str:
    """Average of ``simple_return`` per (trading day, symbol).

    Excludes rows with a null return (the first bar of each series, where no
    prior close exists).
    """
    return f"""
        SELECT
            DATE(date) AS trading_day,
            symbol,
            AVG(simple_return) AS avg_return
        FROM `{table}`
        WHERE simple_return IS NOT NULL
        GROUP BY trading_day, symbol
        ORDER BY trading_day, symbol
    """


def highest_volatility_symbols(table: str, limit: int = 10) -> str:
    """Top ``limit`` symbols ranked by sample std dev of ``simple_return``.

    Volatility here is the realised return volatility over the full loaded
    history; it is *not* an annualised figure.
    """
    return f"""
        SELECT
            symbol,
            STDDEV(simple_return) AS return_volatility,
            COUNT(*) AS observations
        FROM `{table}`
        WHERE simple_return IS NOT NULL
        GROUP BY symbol
        ORDER BY return_volatility DESC
        LIMIT {int(limit)}
    """


def top_anomaly_days(table: str, anomaly_column: str, limit: int = 10) -> str:
    """Top ``limit`` (trading day, symbol) pairs by count of flagged anomalies.

    ``anomaly_column`` is the name of a BOOLEAN column produced by either the
    z-score or IQR detector (e.g. ``close_zscore_anomaly_20``).
    """
    return f"""
        SELECT
            DATE(date) AS trading_day,
            symbol,
            COUNTIF({anomaly_column}) AS anomaly_count
        FROM `{table}`
        GROUP BY trading_day, symbol
        HAVING anomaly_count > 0
        ORDER BY anomaly_count DESC
        LIMIT {int(limit)}
    """


def daily_trading_volume(table: str) -> str:
    """Total ``volume`` aggregated per (trading day, symbol)."""
    return f"""
        SELECT
            DATE(date) AS trading_day,
            symbol,
            SUM(volume) AS total_volume
        FROM `{table}`
        GROUP BY trading_day, symbol
        ORDER BY trading_day, symbol
    """


def rolling_anomaly_counts(
    table: str, anomaly_column: str, window_days: int = 7
) -> str:
    """Daily anomaly count plus its trailing ``window_days``-day sum per symbol.

    The rolling sum uses a row-based window (``ROWS BETWEEN N PRECEDING AND
    CURRENT ROW``), which assumes one row per (trading day, symbol) — the
    exact shape the inner CTE produces.
    """
    return f"""
        WITH daily AS (
            SELECT
                DATE(date) AS trading_day,
                symbol,
                COUNTIF({anomaly_column}) AS anomaly_count
            FROM `{table}`
            GROUP BY trading_day, symbol
        )
        SELECT
            trading_day,
            symbol,
            anomaly_count,
            SUM(anomaly_count) OVER (
                PARTITION BY symbol
                ORDER BY trading_day
                ROWS BETWEEN {int(window_days) - 1} PRECEDING AND CURRENT ROW
            ) AS rolling_anomaly_count
        FROM daily
        ORDER BY symbol, trading_day
    """
