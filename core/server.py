"""FastAPI application: overlay page, WebSocket events, health.

Owns the background pipeline (audio capture, ASR loop, keyword engine,
plugins) via the app lifespan. Binds to 127.0.0.1 only (spec section 9).
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse, Response
from loguru import logger

OVERLAY_DIR = Path(__file__).parent / "overlay"

from .audio import AudioCapture
from .asr import ASREngine
from .keywords import KeywordEngine
from .plugins import PluginManager
from .wordstream import WordEvent, WordStream


class Core:
    """Holds pipeline state shared between the app and its background tasks."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.audio = AudioCapture()
        self.clients: set[WebSocket] = set()
        self.words = WordStream(
            window_size=config["overlay"]["window_size"],
            on_word=self._on_word,
        )
        self.asr = ASREngine(config, self.words)
        self.asr.set_audio_queue(self.audio.queue)
        self.plugins = PluginManager(config)
        self.keywords = KeywordEngine(
            config, self.words, dispatch=self.plugins.dispatch,
            broadcast=self.broadcast,
        )
        self.words_total = 0

    def _on_word(self, event: WordEvent) -> None:
        if event.is_final:
            self.words_total += 1
        self.broadcast({
            "type": "word", "text": event.text,
            "index": event.index, "is_final": event.is_final,
        })

    def broadcast(self, payload: dict[str, Any]) -> None:
        """Push a word/trigger event to all connected overlay clients."""
        data = json.dumps(payload)
        for client in list(self.clients):
            asyncio.create_task(self._safe_send(client, data))

    @staticmethod
    async def _safe_send(client: WebSocket, payload: str) -> None:
        try:
            await client.send_text(payload)
        except Exception:
            pass  # client disconnect is handled by its receive loop

    def health_payload(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "asr_loaded": self.asr.loaded,
            "audio": {
                "captured_seconds": round(self.audio.captured_seconds, 1),
                "queue_depth": self.audio.queue.qsize(),
                "dropped_frames": self.audio.dropped_frames,
            },
            "words_total": self.words.total_words,
            "window": self.words.window_text,
            "plugins": {
                name: getattr(plugin, "connected", True)
                for name, plugin in self.plugins.plugins.items()
            },
        }


def create_app(core: Core) -> FastAPI:
    """Build the FastAPI app wired to the given core instance."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("logos starting (host {}:{})",
                    core.config["overlay"]["host"], core.config["overlay"]["port"])
        await core.audio.start()
        await core.asr.start()
        await core.plugins.start()
        await core.keywords.start()
        yield
        await core.keywords.stop()
        await core.asr.stop()
        await core.plugins.stop()
        core.audio.stop()
        logger.info("logos shutdown complete")

    app = FastAPI(title="logos", lifespan=lifespan)

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(core.health_payload())

    @app.get("/", response_class=Response)
    async def index() -> Response:
        return Response(
            (OVERLAY_DIR / "index.html").read_text(encoding="utf-8"),
            media_type="text/html",
        )

    @app.get("/overlay.css", response_class=Response)
    async def overlay_css() -> Response:
        return Response(
            (OVERLAY_DIR / "overlay.css").read_text(encoding="utf-8"),
            media_type="text/css",
        )

    @app.get("/overlay.js", response_class=Response)
    async def overlay_js() -> Response:
        config_json = json.dumps(core.config["overlay"])
        js = (
            (OVERLAY_DIR / "overlay.js")
            .read_text(encoding="utf-8")
            .replace("/*__CONFIG__*/{}", config_json)
        )
        return Response(js, media_type="application/javascript")

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket) -> None:
        """Push word/trigger events to the overlay."""
        await ws.accept()
        core.clients.add(ws)
        try:
            # Keep the socket alive; sends are driven by core.broadcast.
            while True:
                await ws.receive_text()
        except Exception:
            pass
        finally:
            core.clients.discard(ws)

    return app


def run(config: dict[str, Any]) -> None:
    """Start the core server with uvicorn (blocking)."""
    import uvicorn

    core = Core(config)
    uvicorn.run(
        create_app(core),
        host=config["overlay"]["host"],
        port=config["overlay"]["port"],
        log_level="warning",
    )
