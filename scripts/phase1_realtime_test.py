"""Phase 1 research: stream a WAV over the nemo-speech realtime WebSocket.

Feeds PCM16 audio at real-time pace in 160 ms chunks, records when partial
and final transcription events arrive, and reports end-to-end latency
numbers for docs/research.md.
"""

import asyncio
import json
import time

import soundfile as sf
import websockets

SERVER = "ws://127.0.0.1:8081/v1/audio/transcriptions/realtime"
WAV_PATH = "/tmp/jfk.wav"
CHUNK_MS = 160
SAMPLE_RATE = 16000
CHUNK_SAMPLES = SAMPLE_RATE * CHUNK_MS // 1000


async def main() -> None:
    samples, rate = sf.read(WAV_PATH, dtype="int16")
    if samples.ndim > 1:
        samples = samples.mean(axis=1).astype("int16")
    assert rate == SAMPLE_RATE, f"unexpected rate {rate}"
    data = samples.tobytes()

    t0 = time.monotonic()
    deltas: list[tuple[float, str]] = []
    finals: list[tuple[float, str]] = []

    async with websockets.connect(SERVER) as ws:
        created = json.loads(await ws.recv())
        print(f"[{time.monotonic() - t0:7.3f}] session.created")
        await ws.send(json.dumps({
            "type": "session.update",
            "session": {
                "sample_rate": SAMPLE_RATE,
                "language": "en-US",
                "word_timestamps": True,
            },
        }))
        updated = json.loads(await ws.recv())
        print(f"[{time.monotonic() - t0:7.3f}] {updated['type']}")

        send_task = asyncio.create_task(pump(ws, data))
        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
            except asyncio.TimeoutError:
                break
            event = json.loads(msg)
            etype = event["type"]
            now = time.monotonic() - t0
            if etype == "conversation.item.input_audio_transcription.delta":
                text = event.get("delta", "")
                processed = event.get("audio_processed")
                deltas.append((now, text))
                lag = now - processed if processed is not None else float("nan")
                if text:
                    print(f"[{now:7.3f}] delta: {text!r} (audio @ {processed:.2f}s, lag {lag:.2f}s)")
            elif etype == "conversation.item.input_audio_transcription.completed":
                text = event.get("transcript", "")
                finals.append((now, text))
                print(f"[{now:7.3f}] FINAL: {text!r}")
            elif etype == "input_audio_buffer.committed":
                print(f"[{now:7.3f}] committed")
            elif etype == "error":
                print(f"[{now:7.3f}] ERROR: {event}")
            else:
                print(f"[{now:7.3f}] {etype}")
        await send_task

    duration = len(data) / 2 / SAMPLE_RATE
    print(f"\nAudio duration: {duration:.2f}s")
    if deltas:
        print(f"First partial at {deltas[0][0]:.3f}s after stream start")
    if finals:
        print(f"Final at {finals[-1][0]:.3f}s after stream start")


async def pump(ws, data: bytes) -> None:
    total = len(data) // 2
    offset = 0
    while offset < total:
        n = min(CHUNK_SAMPLES, total - offset)
        await ws.send(data[offset * 2:(offset + n) * 2])
        offset += n
        # Real-time pace: 160 ms of audio every 160 ms.
        await asyncio.sleep(CHUNK_MS / 1000)
    await ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
    print(f"[pump] sent {(total / SAMPLE_RATE):.2f}s of audio")


if __name__ == "__main__":
    asyncio.run(main())
