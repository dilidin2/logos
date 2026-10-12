"""OBS plugin: obs-websocket 5.x actions.

Connects to OBS in the background with retry/backoff (OBS may not be
running at startup). All actions log errors instead of raising for
expected failures (unknown scene/source names) (spec section 7.1).
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from ..base import Plugin

RECONNECT_BACKOFF_S = (1.0, 2.0, 5.0, 10.0, 30.0)


class ObsPlugin(Plugin):
    """OBS actions over obs-websocket 5.x."""

    name = "obs"

    def __init__(self) -> None:
        self._client: Any = None
        self._host = "127.0.0.1"
        self._port = 4455
        self._password = ""
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def setup(self, config: dict[str, Any]) -> None:
        import logging

        # The library logs full tracebacks for expected connection
        # failures; the plugin logs those itself, more concisely.
        logging.getLogger("obsws_python").setLevel(logging.CRITICAL)
        self._host = config.get("host", self._host)
        self._port = int(config.get("port", self._port))
        self._password = config.get("password", "")

    @property
    def connected(self) -> bool:
        return self._client is not None

    async def connect(self) -> None:
        """Start the background connect/reconnect loop (returns promptly)."""
        self._stop.clear()
        self._task = asyncio.create_task(self._connect_loop(), name="obs-connect")

    async def _connect_loop(self) -> None:
        backoff = 0
        while not self._stop.is_set():
            try:
                client = await asyncio.to_thread(self._create_client)
                self._client = client
                backoff = 0
                logger.info("OBS connected ({}:{})", self._host, self._port)
                # Detect silent disconnects: any request will fail.
                await asyncio.to_thread(
                    client.req, "GetVersion", None
                )
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._client = None
                delay = RECONNECT_BACKOFF_S[
                    min(backoff, len(RECONNECT_BACKOFF_S) - 1)
                ]
                logger.warning("OBS not reachable ({}); retrying in {:.0f}s", e, delay)
                backoff += 1
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    return
                except asyncio.TimeoutError:
                    pass

    def _create_client(self) -> Any:
        from obsws_python.baseclient import ObsClient

        client = ObsClient(
            host=self._host, port=self._port, password=self._password
        )
        client.authenticate()
        return client

    async def shutdown(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            self._task = None
        self._client = None
        logger.info("OBS plugin shut down")

    def actions(self) -> dict[str, Any]:
        return {
            "change_scene": self._change_scene,
            "record_clip": self._record_clip,
            "toggle_source": self._toggle_source,
        }

    async def execute(self, action: str, args: dict[str, Any]) -> None:
        handler = self.actions().get(action)
        if handler is None:
            logger.error("OBS plugin: unknown action {!r}", action)
            return
        if self._client is None:
            logger.error("OBS plugin: not connected, action {!r} skipped", action)
            return
        try:
            await asyncio.to_thread(handler, args)
        except Exception as e:
            # Expected failures (unknown scene/source) land here.
            logger.error("OBS action {} failed: {}", action, e)

    # -- action handlers (run in a worker thread) -------------------------

    def _change_scene(self, args: dict[str, Any]) -> None:
        scene = args.get("scene")
        if not scene:
            raise ValueError("change_scene requires 'scene'")
        resp = self._client.req("SetCurrentScene", {"sceneName": scene})
        result = resp.get("result", "success")
        if result not in ("success", ""):
            raise RuntimeError(f"SetCurrentScene failed: {result}")
        logger.info("OBS scene changed to {}", scene)

    def _record_clip(self, args: dict[str, Any]) -> None:
        status = self._client.req("GetRecordClipStatus", None)
        active = bool(status.get("responseData", {}).get("active", False))
        self._client.req("SetRecordClipActive", {"active": not active})
        logger.info("OBS clip recording {}", "started" if not active else "stopped")

    def _toggle_source(self, args: dict[str, Any]) -> None:
        source = args.get("source")
        if not source:
            raise ValueError("toggle_source requires 'source'")
        current = self._client.req("GetCurrentScene", None)
        scene_name = current.get("responseData", {}).get("sceneName")
        if not scene_name:
            raise RuntimeError("OBS has no current scene")
        items = self._client.req(
            "GetSceneItemList", {"sceneName": scene_name}
        ).get("responseData", {}).get("sceneItems", [])
        for item in items:
            if item.get("sourceName") == source:
                enabled = not item.get("sceneItemEnabled", True)
                self._client.req(
                    "SetSceneItemEnabled",
                    {
                        "sceneName": scene_name,
                        "sceneItemId": item["sceneItemId"],
                        "sceneItemEnabled": enabled,
                    },
                )
                logger.info(
                    "OBS source {} {}", source, "shown" if enabled else "hidden"
                )
                return
        raise RuntimeError(f"source {source!r} not found in scene {scene_name!r}")
