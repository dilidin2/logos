"""Keyword engine: trigger matching, cooldown, dispatch.

Watches the finalized rolling window text and fires configured
triggers (spec section 5). Matching is case-insensitive whole-phrase
substring over space-normalized text of finalized words only. Each
trigger has an independent cooldown. Dispatch is fire-and-forget: a
failing plugin action is logged, never raised.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from .wordstream import WordStream


def _normalize(text: str) -> str:
    return " ".join(text.split()).lower()


class KeywordEngine:
    """Matches configured triggers against the finalized word window."""

    POLL_INTERVAL_S = 0.2

    def __init__(
        self,
        config: dict[str, Any],
        words: WordStream,
        dispatch: Callable[[str, str, dict[str, Any]], Awaitable[None]],
        broadcast: Callable[[dict[str, Any]], None],
    ) -> None:
        self._words = words
        self._dispatch = dispatch
        self._broadcast = broadcast
        self._cooldown_s = float(config["keywords"]["cooldown_seconds"])
        self._show_triggers = bool(config["overlay"]["show_keyword_triggers"])
        self._triggers = [
            {
                "trigger": _normalize(t["trigger"]),
                "raw": t["trigger"],
                "action": t["action"],
                "args": t.get("args") or {},
            }
            for t in config["keywords"]["triggers"]
        ]
        self._last_fired: dict[str, float] = {}
        self._last_text = ""
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="keyword-engine")
        logger.info("Keyword engine started ({} trigger(s))", len(self._triggers))

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _run(self) -> None:
        while True:
            text = self._words.finalized_window_text
            if text != self._last_text:
                self._last_text = text
                self._check(text)
            await asyncio.sleep(self.POLL_INTERVAL_S)

    def _check(self, text: str) -> None:
        normalized = _normalize(text)
        if not normalized:
            return
        now = time.monotonic()
        for trigger in self._triggers:
            if trigger["trigger"] not in normalized:
                continue
            last = self._last_fired.get(trigger["trigger"], 0.0)
            if now - last < self._cooldown_s:
                continue
            self._last_fired[trigger["trigger"]] = now
            plugin, _, action = trigger["action"].partition(".")
            logger.info(
                "Trigger {!r} fired -> {}{}", trigger["raw"], trigger["action"],
                f" {trigger['args']}" if trigger["args"] else "",
            )
            if self._show_triggers:
                self._broadcast({
                    "type": "trigger",
                    "trigger": trigger["raw"],
                    "action": trigger["action"],
                })
            asyncio.create_task(self._safe_dispatch(plugin, action, trigger["args"]))

    async def _safe_dispatch(
        self, plugin: str, action: str, args: dict[str, Any]
    ) -> None:
        try:
            await self._dispatch(plugin, action, args)
        except Exception as e:
            logger.error("Dispatch {}.{} failed: {}", plugin, action, e)
