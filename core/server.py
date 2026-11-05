"""FastAPI application: overlay page, WebSocket events, health.

Owns the background pipeline (audio capture, ASR loop, keyword engine,
plugins) via the app lifespan. Binds to 127.0.0.1 only (spec section 9).
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse, Response
from loguru import logger

OVERLAY_DIR = Path(__file__).parent / "overlay"

# App-level liveness probe: sent to every client so the overlay's stale
# watchdog (overlay.js) can detect silently dead connections.
HEARTBEAT_INTERVAL_S = 15

# Interval of the pipeline stage monitor: logs the state of every
# stage so a "subtitles stopped" incident shows exactly where and when.
PIPELINE_LOG_INTERVAL_S = 15.0

# No-words watchdog thresholds.
NO_WORD_WARN_S = 30.0
NO_WORD_LOG_INTERVAL_S = 60.0
NEVER_WORD_WARMUP_S = 90.0


async def _heartbeat(core: "Core") -> None:
    """Periodically ping all overlay clients."""
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_S)
        core.broadcast({"type": "ping"})


async def _pipeline_monitor(core: "Core") -> None:
    """Log the state of every pipeline stage on an interval.

    This is the incident trail: after a "subtitles stopped" event the
    file log shows which stage went quiet (audio frames, ASR process,
    words) and when — audio last_frame age, queue depth, dropped
    frames, ASR pid/alive, words total and last_word age.
    """
    loop = asyncio.get_running_loop()
    start = loop.time()
    last_no_word_log = 0.0
    while True:
        await asyncio.sleep(PIPELINE_LOG_INTERVAL_S)
        audio = core.audio
        last_frame_age = (
            time.monotonic() - audio.last_frame_at
            if audio.last_frame_at is not None else None
        )
        last_word_age = (
            time.monotonic() - core.last_word_at
            if core.last_word_at is not None else None
        )
        last_speech_age = (
            time.monotonic() - core.asr.last_speech_at
            if core.asr.last_speech_at is not None else None
        )
        logger.info(
            "pipeline | audio: captured={:.1f}s last_frame={} queue={} "
            "dropped={} overflows={} | asr: pid={} alive={} | words: "
            "total={} last_word={} | speech: last_speech={} recoveries={} "
            "| clients={}",
            audio.captured_seconds,
            f"{last_frame_age:.1f}s" if last_frame_age is not None else "n/a",
            audio.queue.qsize(),
            audio.dropped_frames,
            audio.overflow_count,
            core.asr.server_pid,
            core.asr.server_alive,
            core.words.total_words,
            f"{last_word_age:.1f}s" if last_word_age is not None else "n/a",
            f"{last_speech_age:.1f}s" if last_speech_age is not None else "n/a",
            core.asr.recovery_count,
            len(core.clients),
        )
        now = time.monotonic()
        # No-words watchdog: flag a silent pipeline (throttled).
        if (last_word_age is not None and
                last_word_age > NO_WORD_WARN_S and
                now - last_no_word_log > NO_WORD_LOG_INTERVAL_S):
            logger.warning(
                "No words for {:.0f}s — mic silence or pipeline stall; "
                "check audio/asr stages in the pipeline line",
                last_word_age,
            )
            last_no_word_log = now
        elif (last_word_age is None and
              now - start > NEVER_WORD_WARMUP_S and
              now - last_no_word_log > NO_WORD_LOG_INTERVAL_S):
            logger.warning(
                "Pipeline running for {:.0f}s but no words ever — check "
                "mic/ASR", now - start,
            )
            last_no_word_log = now


async def _loop_lag_monitor() -> None:
    """Detect event-loop stalls.

    If a 1 s sleep takes much longer than 1 s, something blocked the
    loop — the classic cause of "everything froze" (broadcasts, WS
    sends and heartbeats all stop). Logged so the stall is visible in
    the file log.
    """
    loop = asyncio.get_running_loop()
    while True:
        start = loop.time()
        await asyncio.sleep(1.0)
        elapsed = loop.time() - start
        if elapsed - 1.0 > 2.0:
            logger.warning("Event loop stalled for {:.1f}s", elapsed)

from .audio import AudioCapture
from .asr import ASREngine
from .keywords import KeywordEngine
from .plugins import PluginManager
from .wordstream import WordEvent, WordStream


class Core:
    """Holds pipeline state shared between the app and its background tasks."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.started_at = time.monotonic()
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
        # Monotonic time of the last word event (None until the first);
        # used by the pipeline monitor's no-words watchdog.
        self.last_word_at: float | None = None

    def _on_word(self, event: WordEvent) -> None:
        self.last_word_at = time.monotonic()
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
            "asr_process": {
                "pid": self.asr.server_pid,
                "alive": self.asr.server_alive,
            },
            "audio": {
                "captured_seconds": round(self.audio.captured_seconds, 1),
                "queue_depth": self.audio.queue.qsize(),
                "dropped_frames": self.audio.dropped_frames,
            },
            "words_total": self.words.total_words,
            "words_per_sec": round(
                self.words.total_words / max(time.monotonic() - self.started_at, 1.0),
                2,
            ),
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
        heartbeat = asyncio.create_task(_heartbeat(core))
        pipeline_monitor = asyncio.create_task(_pipeline_monitor(core))
        loop_monitor = asyncio.create_task(_loop_lag_monitor())
        await core.audio.start()
        await core.asr.start()
        await core.plugins.start()
        await core.keywords.start()
        yield
        pipeline_monitor.cancel()
        loop_monitor.cancel()
        heartbeat.cancel()
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
        logger.info("Overlay client connected ({} total)", len(core.clients))
        try:
            # Keep the socket alive; sends are driven by core.broadcast.
            while True:
                await ws.receive_text()
        except Exception:
            pass
        finally:
            core.clients.discard(ws)
            logger.info("Overlay client disconnected ({} total)",
                        len(core.clients))

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
        # TCP-level keepalive: uvicorn pings the WS transport so
        # half-dead connections are torn down on both sides.
        ws_ping_interval=20,
        ws_ping_timeout=10,
    )
