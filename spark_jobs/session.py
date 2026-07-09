"""Reusable SparkSession factory driven by configs/spark.yaml."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "spark.yaml"

DEFAULTS: dict[str, Any] = {
    "app_name": "FinancialPipeline",
    "master": "local[*]",
    "config": {},
}


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load Spark settings from a YAML file, falling back to defaults."""
    if not path.exists():
        logger.warning("Spark config %s not found; using defaults.", path)
        return DEFAULTS
    with path.open("r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f) or {}
    return {**DEFAULTS, **loaded}


def get_spark_session(config_path: Path = CONFIG_PATH) -> SparkSession:
    """Build (or return the existing) SparkSession from YAML config."""
    cfg = load_config(config_path)
    builder = SparkSession.builder.appName(cfg["app_name"]).master(cfg["master"])
    for key, value in cfg.get("config", {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    logger.info("SparkSession ready: app=%s master=%s", cfg["app_name"], cfg["master"])
    return spark
