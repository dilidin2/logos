"""Configuration loading and validation for logos.

The single source of runtime behavior is config/logos_config.yaml.
Missing file or invalid values fail fast at startup with readable
messages (spec section 8).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from loguru import logger

CONFIG_PATH = Path("config/logos_config.yaml")
EXAMPLE_PATH = Path("config/logos_config.yaml.example")

VALID_CHUNK_SIZES_MS = (80, 160, 320, 560, 1120)
VALID_LANGUAGES = ("en", "it")


class ConfigError(Exception):
    """Raised when the config file is missing or contains invalid values."""


def _require(mapping: dict[str, Any], key: str, section: str) -> Any:
    """Return mapping[key] or raise a ConfigError naming the section."""
    if key not in mapping:
        raise ConfigError(f"Missing required key '{section}.{key}'.")
    return mapping[key]


def _validate_asr(config: dict[str, Any]) -> None:
    section = _require(config, "asr", "root")
    model_path = Path(_require(section, "model_path", "asr"))
    if not model_path.exists():
        raise ConfigError(
            f"asr.model_path does not exist: {model_path}. "
            "Download the model into models/ first."
        )
    language = _require(section, "language", "asr")
    if language not in VALID_LANGUAGES:
        raise ConfigError(
            f"asr.language must be one of {VALID_LANGUAGES}, got '{language}'."
        )
    chunk_ms = _require(section, "chunk_size_ms", "asr")
    if chunk_ms not in VALID_CHUNK_SIZES_MS:
        raise ConfigError(
            f"asr.chunk_size_ms must be one of {VALID_CHUNK_SIZES_MS}, "
            f"got {chunk_ms}."
        )


def _validate_audio(config: dict[str, Any]) -> None:
    section = _require(config, "audio", "root")
    rate = _require(section, "sample_rate", "audio")
    if not isinstance(rate, int) or rate != 16000:
        raise ConfigError(
            f"audio.sample_rate must be 16000 (model input rate), got {rate}."
        )


def _validate_overlay(config: dict[str, Any]) -> None:
    section = _require(config, "overlay", "root")
    port = _require(section, "port", "overlay")
    if not isinstance(port, int) or not (1 <= port <= 65535):
        raise ConfigError(f"overlay.port must be 1-65535, got {port}.")
    window = _require(section, "window_size", "overlay")
    if not isinstance(window, int) or window < 1:
        raise ConfigError(f"overlay.window_size must be a positive int, got {window}.")
    dim_step = _require(section, "dim_step", "overlay")
    if not (0 <= dim_step <= 1):
        raise ConfigError(f"overlay.dim_step must be in [0, 1], got {dim_step}.")


def _validate_keywords(config: dict[str, Any]) -> None:
    section = _require(config, "keywords", "root")
    cooldown = _require(section, "cooldown_seconds", "keywords")
    if not isinstance(cooldown, (int, float)) or cooldown < 0:
        raise ConfigError(
            f"keywords.cooldown_seconds must be a non-negative number, got {cooldown}."
        )
    triggers = _require(section, "triggers", "keywords")
    if not isinstance(triggers, list):
        raise ConfigError("keywords.triggers must be a list.")
    for i, trigger in enumerate(triggers):
        if not isinstance(trigger, dict):
            raise ConfigError(f"keywords.triggers[{i}] must be a mapping.")
        name = trigger.get("trigger")
        if not name or not isinstance(name, str):
            raise ConfigError(f"keywords.triggers[{i}].trigger must be a non-empty string.")
        action = trigger.get("action")
        if not action or "." not in action:
            raise ConfigError(
                f"keywords.triggers[{i}].action must be 'plugin.action', "
                f"got {action!r}."
            )


def _validate_plugins(config: dict[str, Any]) -> None:
    section = config.get("plugins", {})
    if not isinstance(section, dict):
        raise ConfigError("plugins must be a mapping.")
    for name, plugin_cfg in section.items():
        if not isinstance(plugin_cfg, dict):
            raise ConfigError(f"plugins.{name} must be a mapping.")
        if "enabled" in plugin_cfg and not isinstance(plugin_cfg["enabled"], bool):
            raise ConfigError(f"plugins.{name}.enabled must be a boolean.")


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load and validate the YAML config, failing fast on any problem."""
    if not path.exists():
        raise ConfigError(
            f"Config file not found: {path}. "
            f"Copy {EXAMPLE_PATH} to {path} and edit it."
        )
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if not isinstance(config, dict):
        raise ConfigError(f"{path} must contain a YAML mapping at the top level.")

    _validate_asr(config)
    _validate_audio(config)
    _validate_overlay(config)
    _validate_keywords(config)
    _validate_plugins(config)

    logger.info("Config loaded from {}", path)
    return config
