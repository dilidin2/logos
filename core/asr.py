"""ASR engine: nemo-speech streaming wrapper.

Runs `nemo-speech serve` as a child process and streams audio frames
from the capture queue to its realtime WebSocket. Token deltas are
accumulated and split into words, which are pushed into the WordStream
as interim events; utterance `completed` events finalize them
(spec section 2, docs/research.md).
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import aiohttp
import websockets
from loguru import logger

from .wordstream import WordStream

LANGUAGE_MAP = {"en": "en-US", "it": "it-IT"}
RECONNECT_BACKOFF_S = (1.0, 2.0, 5.0, 10.0)

# Max time a single ws.send() may block before we consider the server
# hung. A healthy server drains 32 KB/s of audio; if it stops reading
# (app-level hang), the kernel TCP buffer fills in seconds and the send
# blocks forever. The timeout kills the session so it reconnects.
SEND_TIMEOUT_S = 10.0

# Max time for the session handshake (session.created / session.updated).
HANDSHAKE_TIMEOUT_S = 15.0

# Server stdout/stderr go here so a crash leaves a diagnosable trace
# (append mode: previous runs' output is kept).
SERVER_LOG_PATH = Path("logs") / "nemo-speech.log"

# Consecutive server respawns allowed before giving up (a crash loop
# means a model/config problem, not a random crash).
MAX_SERVER_RESTARTS = 5


class _RestartLimitReached(RuntimeError):
    """Raised when the server crash-loop cap is hit; the ASR loop stops."""


def _find_nemo_speech() -> str:
    """Locate the nemo-speech binary (PATH or default install dir)."""
    path = shutil.which("nemo-speech")
    if path:
        return path
    default = Path.home() / ".local/share/nemo-speech/bin/nemo-speech"
    if default.exists():
        return str(default)
    raise FileNotFoundError(
        "nemo-speech binary not found. Install it with the official script: "
        "curl -fsSL https://github.com/NVIDIA/NeMo-Speech.cpp/raw/main/scripts/install.sh | sh"
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ASREngine:
    """Owns the nemo-speech server process and the realtime WS session."""

    def __init__(self, config: dict, word_stream: WordStream) -> None:
        self._config = config
        self._words = word_stream
        self._asr_cfg = config["asr"]
        self._port: int | None = None
        self._proc: subprocess.Popen | None = None
        self._proc_log: Any = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._restarts = 0
        self._partial = ""
        self._pending_punct = ""
        self._open_indices: list[int] = []
        self.loaded = False

    async def start(self) -> None:
        """Start the nemo-speech server and the streaming loop."""
        self._port = _free_port()
        self._spawn_server()
        await self._wait_ready()
        self._task = asyncio.create_task(self._run(), name="asr-loop")
        logger.info("ASR engine started (nemo-speech on port {})", self._port)

    @property
    def server_pid(self) -> int | None:
        """PID of the nemo-speech server process, if any."""
        return self._proc.pid if self._proc is not None else None

    @property
    def server_alive(self) -> bool:
        """Whether the nemo-speech server process is currently alive."""
        return self._proc is not None and self._proc.poll() is None

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
        if self._proc is not None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            self._proc = None
        if self._proc_log is not None:
            self._proc_log.close()
            self._proc_log = None
        logger.info("ASR engine stopped")

    def _spawn_server(self) -> None:
        binary = _find_nemo_speech()
        chunk_size_s = self._asr_cfg["chunk_size_ms"] / 1000
        SERVER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._proc_log = open(SERVER_LOG_PATH, "a", encoding="utf-8")
        self._proc_log.write(
            f"\n=== nemo-speech spawn {time.strftime('%Y-%m-%d %H:%M:%S')} "
            f"port {self._port} ===\n"
        )
        self._proc_log.flush()
        self._proc = subprocess.Popen(
            [
                binary, "serve",
                "--host", "127.0.0.1",
                "--port", str(self._port),
                "--no-ui",
                "--asr-model", self._asr_cfg["model_path"],
                "--asr.streaming.chunk_size", str(chunk_size_s),
                # Mid-stream end-of-utterance: emit completed events on
                # silence so words get finalized without a manual commit.
                "--asr.endpointing.enable",
            ],
            stdout=self._proc_log,
            stderr=self._proc_log,
        )
        logger.info("nemo-speech server spawned (pid {})", self._proc.pid)

    async def _wait_ready(self, timeout: float = 60.0) -> None:
        url = f"http://127.0.0.1:{self._port}/health"
        async with aiohttp.ClientSession() as session:
            deadline = asyncio.get_running_loop().time() + timeout
            while asyncio.get_running_loop().time() < deadline:
                if self._proc is not None and self._proc.poll() is not None:
                    raise RuntimeError(
                        f"nemo-speech server exited early (code {self._proc.returncode})"
                    )
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as r:
                        if r.status == 200:
                            self.loaded = True
                            return
                except aiohttp.ClientError:
                    pass
                await asyncio.sleep(0.5)
        raise TimeoutError(f"nemo-speech server did not become ready at {url}")

    async def _ensure_server(self) -> None:
        """Ensure the nemo-speech server process is alive.

        Respawns it (and re-waits for readiness) if it died, counting
        consecutive respawns: a crash loop gives up after
        `MAX_SERVER_RESTARTS` so a broken model/config fails loudly
        instead of restarting forever.
        """
        proc = self._proc
        if proc is not None and proc.poll() is None:
            return
        if proc is not None:
            logger.error(
                "nemo-speech server died (exit code {}); see {} for details",
                proc.returncode, SERVER_LOG_PATH,
            )
        if self._restarts >= MAX_SERVER_RESTARTS:
            raise _RestartLimitReached(
                f"nemo-speech server restarted {self._restarts} times in a "
                "row without a live session; check {}".format(SERVER_LOG_PATH)
            )
        self._port = _free_port()
        self._spawn_server()
        self._restarts += 1
        await self._wait_ready()
        logger.info(
            "nemo-speech server restarted (pid {}, attempt {}/{})",
            self._proc.pid, self._restarts, MAX_SERVER_RESTARTS,
        )

    async def _run(self) -> None:
        backoff = 0
        while not self._stop.is_set():
            try:
                await self._ensure_server()
                await self._stream_session()
                # A clean return means the server closed the socket on its
                # own: log it so a recurring pattern is visible in the file.
                logger.warning("ASR session closed by server; reconnecting")
                backoff = 0
            except asyncio.CancelledError:
                raise
            except _RestartLimitReached as e:
                logger.error("ASR engine stopping: {}", e)
                break
            except Exception as e:
                logger.warning("ASR session failed: {}; reconnecting", e)
                await asyncio.sleep(RECONNECT_BACKOFF_S[min(backoff, len(RECONNECT_BACKOFF_S) - 1)])
                backoff += 1
                self._reset_utterance()

    def _reset_utterance(self) -> None:
        self._partial = ""
        self._pending_punct = ""
        self._open_indices = []

    def _emit_word(self, word: str) -> None:
        # RNNT tokens sometimes detach punctuation (e.g. '. A sk' for
        # '. Ask'): re-attach a punct-only token to the next word.
        if not any(ch.isalnum() for ch in word):
            self._pending_punct += word
            return
        if self._pending_punct:
            word = self._pending_punct + word
            self._pending_punct = ""
        self._open_indices.append(self._words.add_word(word).index)

    def _feed_delta(self, delta: str) -> None:
        """Accumulate a token delta and emit complete words as interim."""
        self._partial += delta
        while " " in self._partial:
            word, self._partial = self._partial.split(" ", 1)
            if word:
                self._emit_word(word)

    async def _stream_session(self) -> None:
        language = LANGUAGE_MAP[self._asr_cfg["language"]]
        uri = f"ws://127.0.0.1:{self._port}/v1/audio/transcriptions/realtime"
        async with websockets.connect(
            uri, ping_interval=20, ping_timeout=20,
        ) as ws:
            import json

            await asyncio.wait_for(
                ws.recv(), timeout=HANDSHAKE_TIMEOUT_S
            )  # session.created
            self._restarts = 0  # a live session clears the crash-loop count
            await asyncio.wait_for(ws.send(json.dumps({
                "type": "session.update",
                "session": {
                    "sample_rate": 16000,
                    "language": language,
                    "endpointing_ms": 800,
                },
            })), timeout=HANDSHAKE_TIMEOUT_S)
            await asyncio.wait_for(
                ws.recv(), timeout=HANDSHAKE_TIMEOUT_S
            )  # session.updated
            self._reset_utterance()

            # Discard audio buffered while the session was down: it is
            # stale for the new ASR context and would transcribe as a
            # burst of old speech.
            self._drain_stale_audio()

            send_task = asyncio.create_task(self._pump_audio(ws))
            try:
                async for raw in ws:
                    event = json.loads(raw)
                    etype = event.get("type", "")
                    if etype == "conversation.item.input_audio_transcription.delta":
                        self._feed_delta(event.get("delta", ""))
                    elif etype == "conversation.item.input_audio_transcription.completed":
                        self._finalize_utterance()
            finally:
                send_task.cancel()
                # Retrieve the task so a failed/cancelled pump never dies
                # silently; its own errors are already logged in
                # _pump_audio before it exits.
                try:
                    await asyncio.wait_for(send_task, timeout=1.0)
                except BaseException:
                    pass

    def _drain_stale_audio(self) -> None:
        """Drop capture-queue frames buffered while the session was down."""
        queue = self._audio_queue
        dropped = 0
        while True:
            try:
                queue.get_nowait()
                dropped += 1
            except asyncio.QueueEmpty:
                break
        if dropped:
            logger.info(
                "ASR session connected: discarded {} stale audio frame(s)",
                dropped,
            )

    @staticmethod
    def _kill_connection(ws) -> None:
        """Abruptly close the TCP transport.

        A graceful close handshake (ws.close) would itself block on a
        full send buffer; aborting the transport guarantees any blocked
        send and any blocked recv fail immediately with ConnectionClosed,
        tearing the session down so the loop can reconnect.
        """
        transport = getattr(ws, "transport", None)
        if transport is not None:
            transport.close()

    async def _pump_audio(self, ws) -> None:
        """Forward capture-queue frames to the realtime socket.

        Every send is guarded by SEND_TIMEOUT_S: if the nemo-speech
        server stops draining the socket (app-level hang), the TCP
        buffer fills and the send blocks forever. The timeout kills the
        connection so the session loop reconnects instead of stalling
        the whole pipeline. Any other failure is logged, kills the
        connection (unblocking the receive loop) and propagates.
        """
        queue = self._audio_queue
        frames_sent = 0
        try:
            while not self._stop.is_set():
                frame = await queue.get()
                try:
                    await asyncio.wait_for(ws.send(frame), timeout=SEND_TIMEOUT_S)
                except asyncio.TimeoutError:
                    logger.error(
                        "nemo-speech stopped reading audio: ws.send blocked "
                        "for {:.0f}s after {} frame(s) — killing session to "
                        "reconnect", SEND_TIMEOUT_S, frames_sent,
                    )
                    self._kill_connection(ws)
                    raise ConnectionError(
                        "audio send blocked; session killed"
                    ) from None
                frames_sent += 1
        except asyncio.CancelledError:
            logger.debug(
                "audio pump cancelled ({} frame(s) sent this session)",
                frames_sent,
            )
            raise
        except ConnectionError:
            raise  # already logged by the timeout handler above
        except Exception as e:
            logger.error("audio pump failed: {} — killing connection", e)
            self._kill_connection(ws)
            raise

    def _finalize_utterance(self) -> None:
        """Finalize all open words of the current utterance."""
        remainder = self._partial.strip()
        self._partial = ""
        if self._pending_punct and not remainder:
            self._pending_punct = ""  # trailing punct with no word: drop
        if remainder:
            self._emit_word(remainder)
            self._pending_punct = ""
        for index in self._open_indices:
            self._words.mark_final(index)
        self._open_indices = []

    @property
    def _audio_queue(self):
        # Resolved lazily so the engine can be constructed before the
        # capture queue exists.
        return self._queue

    def set_audio_queue(self, queue) -> None:
        self._queue = queue
