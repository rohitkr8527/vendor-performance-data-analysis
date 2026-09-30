"""
Shared configuration for database connection and logging.

The project uses SQLite (``notebooks/inventory.db``) as its analytical database.
All scripts and notebooks read from / write to this single file.
"""

import logging
from pathlib import Path
from sqlalchemy import create_engine

# ── Paths ───────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
DB_PATH = PROJECT_ROOT / "notebooks" / "inventory.db"


def get_engine():
    """Create and return a SQLAlchemy engine for the SQLite database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{DB_PATH}")


def setup_logging(
    name: str,
    *,
    log_file: str | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Return a named logger with console + optional file handlers.

    Parameters
    ----------
    name : str
        Logger name (typically ``__name__`` of the calling module).
    log_file : str | None
        If provided, logs are also written to ``logs/<log_file>``.
    level : int
        Logging level (default ``INFO``).
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console handler
    if not logger.handlers:
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(fmt)
        logger.addHandler(console)

        # File handler
        if log_file:
            LOG_DIR.mkdir(exist_ok=True)
            fh = logging.FileHandler(LOG_DIR / log_file)
            fh.setLevel(level)
            fh.setFormatter(fmt)
            logger.addHandler(fh)

    return logger
