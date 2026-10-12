"""FastAPI application: overlay page, WebSocket events, health.

Owns the background pipeline (audio capture, ASR loop, keyword engine,
plugins) via the app lifespan. Binds to 127.0.0.1 only (spec section 9).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
from loguru import logger

from .audio import AudioCapture


class Core:
    """Holds pipeline state shared between the app and its background tasks."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.audio = AudioCapture()
        self.asr_loaded = False
        self.words_total = 0

    def health_payload(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "asr_loaded": self.asr_loaded,
            "audio": {
                "captured_seconds": round(self.audio.captured_seconds, 1),
                "queue_depth": self.audio.queue.qsize(),
                "dropped_frames": self.audio.dropped_frames,
            },
            "words_total": self.words_total,
        }


def create_app(core: Core) -> FastAPI:
    """Build the FastAPI app wired to the given core instance."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("logos starting (host {}:{})",
                    core.config["overlay"]["host"], core.config["overlay"]["port"])
        await core.audio.start()
        # Phase 3+ background tasks (ASR loop, keyword engine, plugins)
        # will be started here.
        yield
        core.audio.stop()
        logger.info("logos shutdown complete")

    app = FastAPI(title="logos", lifespan=lifespan)

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(core.health_payload())

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket) -> None:
        """Push word/trigger events to the overlay (wired in Phase 3+)."""
        await ws.accept()
        await ws.close()

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
