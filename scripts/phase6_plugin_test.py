"""Phase 6 test: PluginManager + ObsPlugin against a fake OBS server.

Spins up a minimal obs-websocket 5.x protocol server (handshake + the
requests the plugin uses) and verifies the full dispatch path:
keyword trigger -> PluginManager -> ObsPlugin -> OBS.
"""

import asyncio
import base64
import hashlib
import json

import websockets

from core.config import load_config
from core.plugins import PluginManager
from plugins.obs_plugin import ObsPlugin

SCENES = ["Scene A", "Scene B"]
SOURCES = {"Webcam": True, "Browser": False}


async def fake_obs_server(ws) -> None:
    # server-hello (op 7)
    await ws.send(json.dumps({
        "op": 7,
        "d": {
            "obsWebSocketVersion": "5.0.0",
            "rpcVersion": 1,
        },
    }))
    # identify (op 1) -> authenticated (op 2)
    raw = await ws.recv()
    assert json.loads(raw)["op"] == 1
    await ws.send(json.dumps({"op": 2, "d": {"negotiatedRpcVersion": 1}}))
    state = {"clip_active": False, "scene": SCENES[0]}
    async for raw in ws:
        msg = json.loads(raw)
        if msg["op"] != 6:
            continue
        req = msg["d"]
        rtype = req["requestType"]
        data = req.get("requestData", {})
        result, response_data = "success", {}
        if rtype == "GetVersion":
            response_data = {"obsVersion": "30.0"}
        elif rtype == "SetCurrentScene":
            if data["sceneName"] not in SCENES:
                result = "An internal error occurred."
            else:
                state["scene"] = data["sceneName"]
        elif rtype == "GetRecordClipStatus":
            response_data = {"active": state["clip_active"]}
        elif rtype == "SetRecordClipActive":
            state["clip_active"] = data["active"]
        elif rtype == "GetCurrentScene":
            response_data = {"sceneName": state["scene"]}
        elif rtype == "GetSceneItemList":
            response_data = {
                "sceneItems": [
                    {"sourceName": n, "sceneItemId": i + 1,
                     "sceneItemEnabled": e}
                    for i, (n, e) in enumerate(SOURCES.items())
                ]
            }
        elif rtype == "SetSceneItemEnabled":
            for i, (n, _) in enumerate(SOURCES.items()):
                if i + 1 == data["sceneItemId"]:
                    SOURCES[n] = data["sceneItemEnabled"]
        await ws.send(json.dumps({
            "op": 5,
            "d": {
                "requestType": rtype,
                "requestId": req["requestId"],
                "result": result,
                "responseData": response_data,
            },
        }))


async def main() -> None:
    server = await websockets.serve(fake_obs_server, "127.0.0.1", 4455)

    config = load_config()
    manager = PluginManager(config)
    await manager.start()
    obs = manager.plugins["obs"]
    assert isinstance(obs, ObsPlugin)
    print("loaded plugins:", list(manager.plugins))
    print("registered actions:", sorted(manager.registered_actions()))
    assert "obs.change_scene" in manager.registered_actions()

    # Wait for the background connection.
    for _ in range(50):
        if obs.connected:
            break
        await asyncio.sleep(0.2)
    assert obs.connected, "OBS plugin did not connect"

    # change_scene
    await manager.dispatch("obs", "change_scene", {"scene": "Scene B"})
    # record_clip (start)
    await manager.dispatch("obs", "record_clip", {})
    # record_clip (stop)
    await manager.dispatch("obs", "record_clip", {})
    # toggle_source
    await manager.dispatch("obs", "toggle_source", {"source": "Webcam"})
    # Unknown scene: must log, not raise.
    await manager.dispatch("obs", "change_scene", {"scene": "Nope"})
    # Unknown source: must log, not raise.
    await manager.dispatch("obs", "toggle_source", {"source": "Nope"})
    # Unknown action through the manager: must raise.
    try:
        await manager.dispatch("obs", "nope", {})
        raise AssertionError("unknown action did not raise")
    except RuntimeError as e:
        print("unknown action raised:", e)

    await manager.stop()
    server.close()
    print("plugin tests OK")


if __name__ == "__main__":
    asyncio.run(main())
