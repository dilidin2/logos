"""Plugin base class (spec section 7)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class Plugin:
    """Base class for in-process plugins.

    A plugin registers named async actions with the core; the core calls
    `execute` when a keyword trigger maps to one of them.
    """

    name: str = ""

    def setup(self, config: dict[str, Any]) -> None:
        """Store plugin configuration (called once at startup)."""

    async def connect(self) -> None:
        """Establish connections (called once at startup). Must return
        promptly; background reconnect logic belongs here."""

    async def shutdown(self) -> None:
        """Release resources (called at core shutdown)."""

    def actions(self) -> dict[str, Callable]:
        """Map of action name -> async callable (for validation)."""
        return {}

    async def execute(self, action: str, args: dict[str, Any]) -> None:
        """Run a registered action. Must not raise for expected failures
        (e.g. unknown scene names): log and return."""
        raise NotImplementedError
