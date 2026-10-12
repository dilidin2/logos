"""logos entry point: loads config, starts the core server."""

from __future__ import annotations

import sys

from loguru import logger

from core.config import ConfigError, load_config
from core.server import run


def main() -> int:
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:HH:mm:ss}</green> <level>{level: <7}</level> <level>{message}</level>")

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
