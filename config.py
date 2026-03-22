from __future__ import annotations

import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
BASE_URL = "https://energy-poverty.ec.europa.eu/discover-community/epah-atlas"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "data" / "atlas.duckdb"
LOG_DIR = PROJECT_ROOT / "logs"
MAX_AGE_DAYS = 7
REQUEST_DELAY = 1.5
REQUEST_TIMEOUT = 15
REQUEST_RETRIES = 3
USER_AGENT = "atlas-etl/0.1 (+https://energy-poverty.ec.europa.eu/discover-community/epah-atlas)"
NAV_PATH = "/"
DETAIL_LINK_SELECTOR = "a"
DETAIL_FIELD_SELECTOR = "[data-field]"


def ensure_runtime_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def setup_logging(name: str) -> logging.Logger:
    ensure_runtime_dirs()
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger
