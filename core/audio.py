"""Non-blocking microphone capture.

Uses sounddevice (PortAudio) with a callback on PortAudio's own thread;
each 160 ms block is pushed into an asyncio queue. If the consumer lags,
frames are dropped and a throttled warning is logged (spec section 3.3).

Watchdog: PortAudio can silently stop delivering callbacks (ALSA
input_overflow on Linux, device errors). A loop-side watchdog detects
the silence and re-opens the stream, capped so a broken device fails
loudly instead of restarting forever.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import sounddevice as sd
from loguru import logger

FRAME_DURATION_S = 0.16
DROP_WARNING_INTERVAL_S = 5.0

# Watchdog: no audio frame delivered for this long => the PortAudio
# callback has stopped; restart the stream.
STALL_TIMEOUT_S = 10.0
WATCHDOG_INTERVAL_S = 5.0
# More than this many restarts inside the window below => give up and
# log loudly (the device is broken, restarting won't help).
RESTART_WINDOW_S = 600.0
MAX_RESTARTS = 3
DEAD_LOG_INTERVAL_S = 60.0


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
        self._last_frame_at: float | None = None
        self._overflow_count = 0
        self._last_overflow_log = 0.0
        self._watchdog: asyncio.Task | None = None
        self._restart_times: list[float] = []
        self._last_dead_log = 0.0
        self._started = False

    @property
    def last_frame_at(self) -> float | None:
        """Loop time of the most recent delivered frame (None until first)."""
        return self._last_frame_at

    @property
    def overflow_count(self) -> int:
        """Frames lost at capture (PortAudio input_overflow)."""
        return self._overflow_count

    @property
    def dropped_frames(self) -> int:
        return self._frames_dropped

    @property
    def captured_seconds(self) -> float:
        return self._captured_seconds

    async def start(self) -> None:
        """Open the default input device and start capturing."""
        self._loop = asyncio.get_running_loop()
        self._started = True
        # Open + start off the event loop (PortAudio calls the callback
        # from its own thread from this point on).
        await self._loop.run_in_executor(None, self._open_stream)
        # Baseline so the watchdog can also detect a stream that is dead
        # from the start (no first frame ever delivered).
        self._last_frame_at = time.monotonic()
        self._watchdog = asyncio.create_task(
            self._watchdog_loop(), name="audio-watchdog"
        )
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
        if status.input_overflow:
            # ALSA/Pulse lost input data: repeated overflows usually mean
            # the callback stopped running — the watchdog will restart.
            self._overflow_count += 1
            now = time.monotonic()
            if now - self._last_overflow_log >= DROP_WARNING_INTERVAL_S:
                self._last_overflow_log = now
                logger.warning(
                    "PortAudio input_overflow: {} frame(s) lost at capture",
                    self._overflow_count,
                )
        assert self._loop is not None
        self._loop.call_soon_threadsafe(self._push, pcm16)

    def _push(self, data: bytes) -> None:
        self._last_frame_at = time.monotonic()
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

    async def _watchdog_loop(self) -> None:
        """Detect a dead PortAudio callback and restart the stream.

        PortAudio delivers frames from its own thread; if that callback
        stops (ALSA input_overflow, device error), no frames arrive and
        the queue starves. Re-opening the stream recovers without a
        manual restart.
        """
        while True:
            await asyncio.sleep(WATCHDOG_INTERVAL_S)
            if not self._started:
                continue  # not started / shutting down
            if self._stream is None:
                # The stream never opened or a previous restart failed to
                # re-open it: retry while the cap allows, then log loudly.
                if self._can_restart():
                    logger.error("Microphone stream is down — retrying open")
                    try:
                        await self._restart_stream()
                    except Exception as e:
                        logger.error("Microphone restart failed: {}", e)
                        # Count failed attempts too so a broken device
                        # hits the cap instead of retrying forever.
                        self._restart_times.append(time.monotonic())
                else:
                    now = time.monotonic()
                    if now - self._last_dead_log >= DEAD_LOG_INTERVAL_S:
                        self._last_dead_log = now
                        logger.error(
                            "Microphone stream is down (open failed) and "
                            "the restart cap ({}) is hit — capture dead, "
                            "restart logos", MAX_RESTARTS,
                        )
                continue
            if self._last_frame_at is None:
                continue  # stream just opened, waiting for first frames
            silence = time.monotonic() - self._last_frame_at
            if silence < STALL_TIMEOUT_S:
                continue
            if self._can_restart():
                logger.error(
                    "Microphone stalled for {:.0f}s (PortAudio callback "
                    "stopped delivering frames) — restarting stream",
                    silence,
                )
                try:
                    await self._restart_stream()
                except Exception as e:
                    logger.error("Microphone restart failed: {}", e)
                    # Count failed attempts too so a broken device hits
                    # the cap instead of retrying forever.
                    self._restart_times.append(time.monotonic())
            else:
                now = time.monotonic()
                if now - self._last_dead_log >= DEAD_LOG_INTERVAL_S:
                    self._last_dead_log = now
                    logger.error(
                        "Microphone stalled for {:.0f}s and the restart cap "
                        "({} in {:.0f}s) is hit — capture is dead, restart "
                        "logos", silence, MAX_RESTARTS, RESTART_WINDOW_S,
                    )

    def _can_restart(self) -> bool:
        now = time.monotonic()
        self._restart_times = [
            t for t in self._restart_times if now - t <= RESTART_WINDOW_S
        ]
        return len(self._restart_times) < MAX_RESTARTS

    async def _restart_stream(self) -> None:
        """Close and re-open the PortAudio stream off the event loop."""
        old = self._stream
        self._stream = None
        if old is not None:
            await self._loop.run_in_executor(None, self._close_stream, old)
        await self._loop.run_in_executor(None, self._open_stream)
        # Treat the restart moment as "frame just delivered": if the new
        # stream is also dead, the stall check triggers again (cap-limited)
        # instead of waiting forever for a first frame that never comes.
        self._last_frame_at = time.monotonic()
        self._restart_times.append(time.monotonic())
        logger.info(
            "Microphone stream restarted (restart #{})", len(self._restart_times),
        )

    @staticmethod
    def _close_stream(stream: Any) -> None:
        try:
            stream.stop()
        except Exception:
            pass
        stream.close()

    def stop(self) -> None:
        self._started = False
        if self._watchdog is not None:
            self._watchdog.cancel()
            self._watchdog = None
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
