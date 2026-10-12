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
from pathlib import Path

import aiohttp
import websockets
from loguru import logger

from .wordstream import WordStream

LANGUAGE_MAP = {"en": "en-US", "it": "it-IT"}
RECONNECT_BACKOFF_S = (1.0, 2.0, 5.0, 10.0)


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
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
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
        logger.info("ASR engine stopped")

    def _spawn_server(self) -> None:
        binary = _find_nemo_speech()
        chunk_size_s = self._asr_cfg["chunk_size_ms"] / 1000
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
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
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

    async def _run(self) -> None:
        backoff = 0
        while not self._stop.is_set():
            try:
                await self._stream_session()
                backoff = 0
            except asyncio.CancelledError:
                raise
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
        async with websockets.connect(uri) as ws:
            import json

            await ws.recv()  # session.created
            await ws.send(json.dumps({
                "type": "session.update",
                "session": {
                    "sample_rate": 16000,
                    "language": language,
                    "endpointing_ms": 800,
                },
            }))
            await ws.recv()  # session.updated
            self._reset_utterance()

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

    async def _pump_audio(self, ws) -> None:
        """Forward capture-queue frames to the realtime socket."""
        queue = self._audio_queue
        while not self._stop.is_set():
            frame = await queue.get()
            await ws.send(frame)

    @property
    def _audio_queue(self):
        # Resolved lazily so the engine can be constructed before the
        # capture queue exists.
        return self._queue

    def set_audio_queue(self, queue) -> None:
        self._queue = queue
