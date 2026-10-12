"""Non-blocking microphone capture.

Uses sounddevice (PortAudio) with a callback on PortAudio's own thread;
each 160 ms block is pushed into an asyncio queue. If the consumer lags,
frames are dropped and a throttled warning is logged (spec section 3.3).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import sounddevice as sd
from loguru import logger

FRAME_DURATION_S = 0.16
DROP_WARNING_INTERVAL_S = 5.0


class AudioCapture:
    """Microphone capture feeding an asyncio queue of PCM16 frames."""

    def __init__(self, sample_rate: int = 16000, chunk_frames: int = 2560) -> None:
        self.sample_rate = sample_rate
        self.chunk_frames = chunk_frames
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=64)
        self._stream: sd.InputStream | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._frames_dropped = 0
        self._last_drop_warning = 0.0
        self._captured_seconds = 0.0

    @property
    def dropped_frames(self) -> int:
        return self._frames_dropped

    @property
    def captured_seconds(self) -> float:
        return self._captured_seconds

    async def start(self) -> None:
        """Open the default input device and start capturing."""
        self._loop = asyncio.get_running_loop()
        # Open + start off the event loop (PortAudio calls the callback
        # from its own thread from this point on).
        await self._loop.run_in_executor(None, self._open_stream)
        logger.info(
            "Microphone opened (default input device, {} Hz mono)", self.sample_rate
        )

    def _open_stream(self) -> None:
        self._stream = sd.InputStream(
            samplerate=self.sample_rate,
            channels=1,
            dtype="int16",
            blocksize=self.chunk_frames,
            callback=self._callback,
        )
        self._stream.start()

    def _callback(self, indata: Any, frames: int, time_info: Any, status: Any) -> None:
        # Copy: PortAudio reuses this buffer.
        pcm16 = bytes(indata[:, 0])
        self._captured_seconds += frames / self.sample_rate
        assert self._loop is not None
        self._loop.call_soon_threadsafe(self._push, pcm16)

    def _push(self, data: bytes) -> None:
        if self.queue.full():
            self._frames_dropped += 1
            now = time.monotonic()
            if now - self._last_drop_warning >= DROP_WARNING_INTERVAL_S:
                self._last_drop_warning = now
                logger.warning(
                    "Audio consumer lagging: dropped {} frame(s)", self._frames_dropped
                )
            return
        self.queue.put_nowait(data)

    def stop(self) -> None:
        if self._stream is not None:
            self._stream.stop()
            self._stream.close()
            self._stream = None
        self._loop = None
        logger.info(
            "Audio capture stopped ({} s captured, {} frame(s) dropped)",
            round(self._captured_seconds, 1),
            self._frames_dropped,
        )
