import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Optional

from pythonjsonlogger.json import JsonFormatter

from src.auth.context import _current_user_id
from src.request_context import get_request_id


class ContextFilter(logging.Filter):
    """Stamp every record with the current request_id and user_id.

    Reads the per-request contextvars so application *and* CRUD logs carry
    request correlation without each call site passing it explicitly. Both
    fields are always present (``None`` when outside a request) to keep the
    JSON shape stable for aggregation.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        record.user_id = _current_user_id.get()
        return True


def _build_formatter() -> JsonFormatter:
    """JSON formatter used for every handler (JSON everywhere, incl. dev)."""
    return JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={
            "asctime": "timestamp",
            "levelname": "level",
            "name": "logger",
        },
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def setup_logging(
    app_log_level: Optional[str] = None,
    third_party_log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    Set up logging configuration for the Pocket Watcher API.

    Emits structured JSON on every handler. Each record carries the current
    request_id and user_id via ``ContextFilter``.

    Args:
        app_log_level: Log level for application logs (default: INFO)
        third_party_log_level: Log level for third-party libraries (default: WARNING)
        log_file: Optional log file path. If None, logs only to console
        max_file_size: Maximum size of log file before rotation (bytes)
        backup_count: Number of backup log files to keep

    Returns:
        Logger instance for the application
    """
    # Get log levels from environment variables or use defaults
    app_log_level = app_log_level or os.getenv("APP_LOG_LEVEL", "INFO")
    third_party_log_level = third_party_log_level or os.getenv("THIRD_PARTY_LOG_LEVEL", "WARNING")
    log_file = log_file or os.getenv("LOG_FILE")

    # Convert string levels to logging constants
    app_level = getattr(logging, app_log_level.upper(), logging.INFO)
    third_party_level = getattr(logging, third_party_log_level.upper(), logging.WARNING)

    # Create application logger
    app_logger = logging.getLogger("pocket_watcher")
    app_logger.setLevel(app_level)

    # Clear any existing handlers to avoid duplicates
    app_logger.handlers.clear()
    app_logger.filters.clear()

    formatter = _build_formatter()
    context_filter = ContextFilter()

    # Console handler. The context filter lives on the handler (not the
    # logger) so it also stamps records propagated up from child loggers
    # like ``pocket_watcher.crud.*`` — logger-level filters would not.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(app_level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(context_filter)
    app_logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count
        )
        file_handler.setLevel(app_level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(context_filter)
        app_logger.addHandler(file_handler)

    # Configure third-party loggers
    third_party_loggers = [
        "sqlalchemy.engine",
        "sqlalchemy.engine.Engine",
        "sqlalchemy.dialects",
        "sqlalchemy.pool",
        "sqlalchemy.pool.impl",
        "sqlalchemy.pool.Pool",
        "sqlalchemy.orm",
        "alembic",
        "pdfplumber",
        "uvicorn.access",
        "uvicorn.error",
        "httpx",
        "boto3",
        "botocore"
    ]

    for logger_name in third_party_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(third_party_level)

    # Prevent duplicate logs by not propagating to root logger
    app_logger.propagate = False

    return app_logger


def get_logger(name: str = "pocket_watcher") -> logging.Logger:
    """
    Get a logger instance for the specified module.

    Args:
        name: Logger name (typically module name)

    Returns:
        Logger instance
    """
    if name == "pocket_watcher" or name.startswith("pocket_watcher."):
        return logging.getLogger(name)
    else:
        return logging.getLogger(f"pocket_watcher.{name}")
