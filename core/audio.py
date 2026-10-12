"""Non-blocking microphone capture.

A dedicated PortAudio thread reads 16 kHz mono PCM16 and pushes frames
into an asyncio queue. If the consumer falls behind, frames are dropped
(a throttled warning is logged) rather than blocking the capture thread
(spec section 3).
"""

from __future__ import annotations

import asyncio
import queue
import threading
import time

from loguru import logger

# 160 ms of 16 kHz mono PCM16 = 2560 samples = 5120 bytes.
FRAME_SAMPLES = 2560
FRAME_BYTES = FRAME_SAMPLES * 2
SAMPLE_RATE = 16000
CHANNELS = 1


class AudioCapture:
    """Captures microphone audio into an asyncio queue of PCM16 bytes."""

    def __init__(self, max_queue_size: int = 64) -> None:
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=max_queue_size)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._dropped = 0
        self._captured_frames = 0
        self._last_drop_log = 0.0

    @property
    def captured_seconds(self) -> float:
        return self._captured_frames * FRAME_SAMPLES / SAMPLE_RATE

    @property
    def dropped_frames(self) -> int:
        return self._dropped

    async def start(self) -> None:
        """Start the capture thread (must be called from the event loop)."""
        self._loop = asyncio.get_running_loop()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="audio-capture")
        self._thread.start()
        logger.info("Audio capture started ({} Hz mono, {} ms frames)",
                    SAMPLE_RATE, FRAME_SAMPLES * 1000 // SAMPLE_RATE)

    def stop(self) -> None:
        """Signal the capture thread to stop and wait for it to exit."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info(
            "Audio capture stopped (captured {:.1f}s, dropped {} frames)",
            self.captured_seconds, self._dropped,
        )

    def _log_drop(self) -> None:
        now = time.monotonic()
        if now - self._last_drop_log >= 5.0:
            logger.warning(
                "ASR is falling behind: {} audio frame(s) dropped",
                self._dropped,
            )
            self._last_drop_log = now

    def _run(self) -> None:
        import pyaudio  # imported here so the rest of the app loads without PortAudio

        pa = pyaudio.PyAudio()
        try:
            stream = pa.open(
                format=pyaudio.paInt16,
                channels=CHANNELS,
                rate=SAMPLE_RATE,
                input=True,
                frames_per_buffer=FRAME_SAMPLES,
            )
            logger.info("Microphone opened: {}", stream.get_input_device_info_get())
            while not self._stop.is_set():
                data = stream.read(FRAME_SAMPLES, exception_on_overflow=False)
                self._captured_frames += 1
                try:
                    # Non-blocking: drop audio if the consumer is behind.
                    self._loop.call_soon_threadsafe(self.queue.put_nowait, data)
                except queue.Full:
                    self._dropped += 1
                    self._log_drop()
            stream.stop_stream()
            stream.close()
        finally:
            pa.terminate()
