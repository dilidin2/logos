"""Plugin manager: load, validate, dispatch (spec section 7).

Loads the plugins enabled in config, runs setup + connect at startup,
validates that every keyword action maps to a registered plugin action
(warn + skip otherwise), and dispatches `execute` off the main loop.
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from plugins.base import Plugin
from plugins.obs_plugin import ObsPlugin

# name in config -> plugin class
PLUGIN_REGISTRY: dict[str, type[Plugin]] = {
    "obs": ObsPlugin,
}


class PluginManager:
    """Owns the lifecycle and dispatch of all loaded plugins."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._plugins: dict[str, Plugin] = {}

    @property
    def plugins(self) -> dict[str, Plugin]:
        return dict(self._plugins)

    def registered_actions(self) -> set[str]:
        """All 'plugin.action' strings the plugins provide."""
        actions: set[str] = set()
        for name, plugin in self._plugins.items():
            for action in plugin.actions():
                actions.add(f"{name}.{action}")
        return actions

    async def start(self) -> None:
        plugins_cfg = self._config.get("plugins", {})
        for name, plugin_cfg in plugins_cfg.items():
            if not isinstance(plugin_cfg, dict) or not plugin_cfg.get("enabled", False):
                continue
            plugin_cls = PLUGIN_REGISTRY.get(name)
            if plugin_cls is None:
                logger.warning("Unknown plugin {!r} in config, skipping", name)
                continue
            plugin = plugin_cls()
            plugin.setup(plugin_cfg)
            await plugin.connect()
            self._plugins[name] = plugin
            logger.info("Plugin {!r} loaded ({} action(s))",
                        name, len(plugin.actions()))
        self._validate_keyword_actions()

    def _validate_keyword_actions(self) -> None:
        valid = self.registered_actions()
        for trigger in self._config["keywords"]["triggers"]:
            action = trigger["action"]
            if action not in valid:
                logger.warning(
                    "Trigger {!r} maps to unknown action {}; it will be skipped",
                    trigger["trigger"], action,
                )

    async def stop(self) -> None:
        for name, plugin in self._plugins.items():
            try:
                await plugin.shutdown()
            except Exception as e:
                logger.error("Plugin {} shutdown failed: {}", name, e)
        self._plugins.clear()

    async def dispatch(self, plugin_name: str, action: str, args: dict[str, Any]) -> None:
        """Run a plugin action. Raises for unknown plugin/action so the
        keyword engine can log it."""
        plugin = self._plugins.get(plugin_name)
        if plugin is None:
            raise RuntimeError(f"plugin {plugin_name!r} is not loaded")
        if action not in plugin.actions():
            raise RuntimeError(f"plugin {plugin_name!r} has no action {action!r}")
        await plugin.execute(action, args)
