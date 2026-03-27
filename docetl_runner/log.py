"""Logging configuration with Rich handler."""

import logging

from rich.logging import RichHandler


def setup_logging(*, verbose: bool = False) -> logging.Logger:
    """Configure and return the application logger.

    Args:
        verbose: If True, set level to DEBUG; otherwise INFO.

    Returns:
        Configured root logger for the ``docetl_runner`` namespace.
    """
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
        force=True,
    )

    logger = logging.getLogger("docetl_runner")
    logger.setLevel(level)
    return logger
