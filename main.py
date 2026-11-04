"""logos entry point: loads config, starts the core server."""

from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

from core.config import ConfigError, load_config
from core.server import run

LOG_DIR = Path(__file__).parent / "logs"
LOG_FILE = LOG_DIR / "logos.log"


def _setup_logging() -> None:
    """Configure loguru: terminal sink + detailed rotating file sink.

    The file sink is the incident trail: after a "subtitles stopped"
    event mid-live, logs/logos.log shows the pipeline state (stage
    monitor, watchdogs) around the moment things went quiet.
    `enqueue=True` makes the file sink thread-safe and non-blocking
    (the PortAudio thread also logs).
    """
    logger.remove()
    # Terminal sink.
    logger.add(
        sys.stderr,
        level="INFO",
        format=(
            "<green>{time:HH:mm:ss}</green> "
            "<level>{level: <7}</level> <level>{message}</level>"
        ),
    )
    # File sink.
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.add(
        LOG_FILE,
        level="DEBUG",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        rotation="50 MB",
        retention=5,
        encoding="utf-8",
        enqueue=True,
    )


def main() -> int:
    _setup_logging()

    try:
        config = load_config()
    except ConfigError as e:
        logger.error("Configuration error: {}", e)
        return 1

    try:
        run(config)
    except KeyboardInterrupt:
        logger.info("Interrupted (Ctrl-C), shutting down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
