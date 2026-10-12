"""Phase 3 end-to-end test: feed a WAV through the full pipeline.

Starts the app in-process, connects to /ws, pushes the WAV's PCM16
frames into the audio queue at real-time pace, and collects word events.
"""

import asyncio
import json

import soundfile as sf
import uvicorn
import websockets

from core.config import load_config
from core.server import Core, create_app

WAV_PATH = "/tmp/jfk.wav"


async def _noop() -> None:
    pass


async def main() -> None:
    config = load_config()
    core = Core(config)
    # Disable live mic capture: this test feeds the WAV directly into the
    # audio queue, and real mic audio would interleave with it.
    core.audio.start = lambda: _noop()
    app = create_app(core)

    words: list[dict] = []
    server = uvicorn.Server(uvicorn.Config(
        app,
        host=config["overlay"]["host"],
        port=config["overlay"]["port"],
        log_level="warning",
    ))
    server_task = asyncio.create_task(server.serve())
    # Wait for the lifespan (model load) to finish and the socket to bind.
    for _ in range(120):
        try:
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", config["overlay"]["port"]
            )
            writer.close()
            break
        except OSError:
            await asyncio.sleep(0.5)
    try:
        async with websockets.connect(
            f"ws://127.0.0.1:{config['overlay']['port']}/ws"
        ) as ws:
            samples, rate = sf.read(WAV_PATH, dtype="int16")
            data = samples.tobytes()
            chunk = 5120  # 160 ms frames

            async def collect():
                async for raw in ws:
                    event = json.loads(raw)
                    words.append(event)
                    tag = "final" if event["is_final"] else "interim"
                    print(f"[{event['index']:3d}] {tag:7s} {event['text']!r}")

            collect_task = asyncio.create_task(collect())
            offset = 0
            while offset < len(data):
                await core.audio.queue.put(data[offset:offset + chunk])
                offset += chunk
                await asyncio.sleep(chunk / 2 / 16000)  # real-time pace
            # Let endpointing finalize the utterance.
            await asyncio.sleep(3.0)
            collect_task.cancel()
    finally:
        server.should_exit = True
        await server_task

    print(f"\nTotal word events: {len(words)}")
    finals = [w for w in words if w["is_final"]]
    print(f"Final words: {' '.join(w['text'] for w in finals)}")
    assert finals, "no final words received"


if __name__ == "__main__":
    asyncio.run(main())
