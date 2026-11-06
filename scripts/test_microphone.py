"""Microphone test: verify the pipeline is actually hearing the mic.

Opens the same input device the core uses (default input, 16 kHz mono,
160 ms blocks), records for a few seconds, prints a live level meter and
saves a WAV file so you can listen to exactly what the app receives.

Usage:
    python scripts/test_microphone.py [--seconds 5] [--output /tmp/mic_test.wav]

Exit codes: 0 = mic is producing audio, 1 = silent/no frames.
"""

from __future__ import annotations

import argparse
import sys
import time
import wave
from pathlib import Path
from typing import Any

import numpy as np
import sounddevice as sd
from loguru import logger

SAMPLE_RATE = 16000
CHUNK_FRAMES = 2560  # 160 ms blocks, same as core.audio
SILENCE_RMS = 30.0  # int16 RMS below this is effectively silence


def default_input_name() -> str:
    """Name of the system default input device ('(none)' if absent)."""
    index = sd.default.device[0]
    if index is None:
        return "(none)"
    return sd.query_devices(index, "input")["name"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Test microphone capture")
    parser.add_argument("--seconds", type=float, default=5.0)
    parser.add_argument("--output", type=Path, default=Path("/tmp/mic_test.wav"))
    args = parser.parse_args()

    logger.info("Default input device: {}", default_input_name())
    logger.info("Recording for {:.0f}s — speak into the mic now", args.seconds)

    frames: list[bytes] = []
    peak = 0
    voiced_frames = 0
    total_frames = 0

    def callback(indata: Any, frame_count: int, time_info: Any, status: Any) -> None:
        nonlocal peak, voiced_frames, total_frames
        data = indata[:, 0]
        frames.append(bytes(data))
        total_frames += 1
        rms = float(np.sqrt(np.mean(data.astype(np.int64) ** 2)))
        peak = max(peak, int(np.abs(data).max()))
        if rms >= SILENCE_RMS:
            voiced_frames += 1
        bar = "#" * min(40, int(rms / 100))
        print(f"\r  rms={rms:8.1f}  {bar:<40}", end="", flush=True)

    stream = sd.InputStream(
        samplerate=SAMPLE_RATE,
        channels=1,
        dtype="int16",
        blocksize=CHUNK_FRAMES,
        callback=callback,
    )
    stream.start()
    time.sleep(args.seconds)
    stream.stop()
    stream.close()
    print()

    with wave.open(str(args.output), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(b"".join(frames))
    logger.info("Saved {:.1f} s to {}", args.seconds, args.output)

    if total_frames == 0:
        logger.error("No frames captured: the device is not delivering audio")
        return 1

    ratio = voiced_frames / total_frames
    logger.info("Captured {} frame(s), peak level {}", total_frames, peak)
    if ratio < 0.05:
        logger.warning(
            "Mostly silence ({:.0%} of frames above the noise floor). "
            "Check: right default input device, mic mute, system input volume.",
            ratio,
        )
        return 1
    logger.info("Microphone is working: {:.0%} of frames contain audio", ratio)
    return 0


if __name__ == "__main__":
    sys.exit(main())
