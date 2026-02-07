"""
Centralized logging for the Restaurant Review Pipeline.
Uses loguru for structured, colorized logging.
"""

import sys
from loguru import logger

# Remove default handler
logger.remove()

# Console handler with colorized output
logger.add(
    sys.stdout,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    ),
    level="INFO",
    colorize=True,
)

# File handler for persistent logs
logger.add(
    "logs/pipeline_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
)


def get_logger(name: str):
    """Get a named logger instance."""
    return logger.bind(name=name)
