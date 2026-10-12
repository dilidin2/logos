# logos — Specification

**logos** is a local, real-time speech-to-text overlay and action dispatcher.
It listens to the microphone, transcribes speech with a streaming ASR model,
shows a live sliding window of the most recent words on an HTML/JS overlay
(served on localhost), and dispatches configurable keyword triggers to
in-process plugins (e.g. OBS actions).

## 1. Architecture Overview

```
 mic ──▶ AudioCapture ──▶ ASREngine (nemo-speech, GGUF q8_0) ──▶ WordStream
                                                                    │
                                                                    ▼
                                                          Core (FastAPI app)
                                                         ┌───────────────────┐
                                                         │  KeywordEngine    │
                                                         │  Overlay (WS)     │
                                                         │  PluginManager    │
                                                         └───────────────────┘
                                                           │         │
                                                     WebSocket   Plugin API
                                                           │         │
                                                     overlay.html   obs_plugin
                                                      (localhost)   (in-process)
```

- **Core** owns: audio capture, ASR, word stream, keyword engine, overlay
  server, plugin loading/dispatch. Nothing else.
- **Plugins** are in-process Python packages under `plugins/`. They register
  actions with the core; the core calls them when a keyword trigger matches.
- The **overlay** is a static HTML/JS page served by the core, updated in
  real time over a local WebSocket.

### Repository layout

```
logos/
├── AGENTS.md
├── SPEC.md
├── README.md
├── main.py                     # entry point: loads config, starts everything
├── requirements.txt
├── config/
│   └── logos_config.yaml.example
├── core/
│   ├── __init__.py
│   ├── audio.py                # non-blocking mic capture → audio queue
│   ├── asr.py                  # ASREngine: nemo-speech streaming wrapper
│   ├── wordstream.py           # word events, sliding window state
│   ├── keywords.py             # KeywordEngine: matching + cooldown + dispatch
│   ├── plugins.py              # PluginManager: load, register, dispatch
│   ├── server.py               # FastAPI app: static overlay + WebSocket
│   └── overlay/
│       ├── index.html
│       ├── overlay.css
│       └── overlay.js
├── plugins/
│   ├── __init__.py
│   ├── base.py                 # Plugin base class
│   └── obs_plugin/
│       ├── __init__.py
│       └── obs_plugin.py
└── models/                     # model weights live here (downloaded)
```

## 2. ASR

- **Model**: `nvidia/nemotron-3.5-asr-streaming-0.6b`, quantized
  **`q8_0` GGUF** (official file
  `nemotron-3.5-asr-streaming-0.6b.q8_0.gguf`).
- **Runtime**: `nemo-speech` (official NVIDIA package). No alternatives.
- Model file is downloaded into `models/` (HF download) and loaded at start.
- Streaming: the engine consumes audio chunks from the audio queue and
  emits **word events** as they are produced (the RNNT decoder emits tokens
  progressively; each finalized word becomes an event).
- **Language**: configured (`en` default, `it` supported). Passed to the
  model's language-ID conditioning.
- **Chunk size**: configurable, one of `80 | 160 | 320 | 560 | 1120` ms.
  Default `160` (latency/accuracy balance).
- **Punctuation & capitalization**: the model outputs them natively; they are
  **kept** in the overlay and in keyword matching.

## 3. Audio Capture

- Source: **system default microphone** (no device selection in config v1).
- 16 kHz mono PCM.
- Capture runs on a dedicated thread (PortAudio) writing to an
  `asyncio.Queue`. **Never blocking**: if ASR falls behind, audio is
  dropped (log a warning) rather than blocking the capture thread.
- Graceful shutdown: capture stops cleanly on Ctrl-C.

## 4. Word Stream & Sliding Window

- The core maintains a **sliding window of at most `overlay.window_size`
  words** (default 5). A new word pushes out the oldest.
- Each word event carries: `text`, `index` (monotonic), `is_final`.
- Word events are broadcast to the overlay over WebSocket:
  `{"type": "word", "text": "...", "index": 123, "is_final": true}`
- Keyword matching uses the same rolling window (last N words, "stable" =
  finalized words only).

## 5. Keyword Engine

- Triggers are defined in config (see §8). Matching rules:
  - **case-insensitive**, whole-phrase substring match against the current
    rolling window text (punctuation kept, spaces normalized).
  - Only **finalized** words are considered.
- **Cooldown**: after a trigger fires, the same trigger is ignored for
  `keywords.cooldown_seconds` (default 3). Cooldown is per-trigger.
- On match, the core:
  1. logs the trigger,
  2. dispatches the action to the owning plugin (`action: <plugin>.<action>`),
  3. (if `overlay.show_keyword_triggers` is true) broadcasts
     `{"type": "trigger", "trigger": "...", "action": "..."}` to the overlay.
- Dispatch is fire-and-forget with exception logging: a failing plugin action
  never crashes the core.

## 6. Overlay

- Served by the core at `http://127.0.0.1:<port>/` (default port `8765`).
  Intended to be loaded as a **Browser Source** in OBS.
- WebSocket endpoint `/ws` pushes word/trigger events.
- Rendering:
  - The sliding window of words is shown as a single line/flow.
  - The **most recent word is highlighted** (bright, `highlight_color`);
    older words fade with a per-step opacity falloff (`dim_step`), and slide
    out as new words arrive (CSS transition).
  - Font, size, colors come entirely from config.
  - **Keyword trigger badges**: when enabled, a triggered keyword appears as
    a short-lived animated badge (e.g. "⚡ clip") that fades out after a couple
    of seconds.
- The page must be self-contained (no CDN): inline CSS/JS or local static
  files only, so it works offline in OBS.

## 7. Plugin System

- `plugins/base.py` defines:

  ```python
  class Plugin:
      name: str                          # e.g. "obs"
      def setup(self, config: dict) -> None: ...
      async def connect(self) -> None: ...
      async def shutdown(self) -> None: ...
      def actions(self) -> dict[str, Callable]: ...   # name → async callable
      async def execute(self, action: str, args: dict) -> None: ...
  ```

- `core/plugins.py` (PluginManager):
  - loads plugins listed/enabled in config,
  - calls `setup` + `connect` at startup,
  - validates that every `action` referenced in the keyword config maps to a
    registered plugin + action (warn + skip on startup if not),
  - dispatches `execute(plugin, action, args)` off the main loop.
- Action namespacing: `obs.change_scene`, `obs.record_clip`, `obs.toggle_source`.

### 7.1 OBS plugin (`plugins/obs_plugin/`)

- Connects to **obs-websocket 5.x** (`obsws-python`), host/port/password from
  config (defaults: `127.0.0.1:4455`, empty password).
- Non-blocking connection: connect/reconnect in background; if OBS is not
  running at startup, retry with backoff and log clearly.
- Actions:
  - **`change_scene`** — args `{scene: str}` → switch current scene.
  - **`record_clip`** — no args → toggle clip recording (start if not
    recording, stop if recording). OBS writes the clip to its clip folder.
  - **`toggle_source`** — args `{source: str}` → toggle a source's
    visibility/enabled state on the current scene.
- Unknown scene/source names are logged as errors, not raised.

## 8. Configuration

Single file: `config/logos_config.yaml` (repo ships
`config/logos_config.yaml.example`). Schema:

```yaml
asr:
  model_path: models/nemotron-3.5-asr-streaming-0.6b.q8_0.gguf
  language: en              # en | it
  chunk_size_ms: 160        # 80 | 160 | 320 | 560 | 1120

audio:
  device: default           # system default microphone
  sample_rate: 16000

overlay:
  host: 127.0.0.1
  port: 8765
  window_size: 5            # max words on screen
  font_family: "Inter, sans-serif"
  font_size_px: 42
  color: "#ffffff"
  highlight_color: "#ffd93d"
  dim_step: 0.25            # opacity lost per step back from the newest word
  show_keyword_triggers: true

keywords:
  cooldown_seconds: 3
  triggers:
    - trigger: "change scene"
      action: obs.change_scene
      args: { scene: "Just Chatting" }
    - trigger: "clip"
      action: obs.record_clip
    - trigger: "toggle webcam"
      action: obs.toggle_source
      args: { source: "Webcam" }

plugins:
  obs:
    enabled: true
    host: 127.0.0.1
    port: 4455
    password: ""
```

- All user-facing values live in this file. Missing file → clear error with a
  pointer to the `.example`.
- Invalid values → fail fast at startup with a readable message.

## 9. Core Server

- FastAPI app (`core/server.py`), started by `main.py` with uvicorn, bound to
  `127.0.0.1` only.
- Endpoints:
  - `GET /` → overlay page
  - `WS /ws` → word/trigger events
  - `GET /health` → status (ASR loaded, words/sec, plugin states)
- The app also hosts the background tasks: audio capture, ASR loop, keyword
  engine, plugin manager.

## 10. Non-Goals (v1)

- No device selection UI, no multi-mic, no remote access (localhost only).
- No persistence of transcripts.
- No plugin hot-reload.
- No web dashboard beyond `/health`.

## 11. Development Phases

The project is developed in phases. **Each phase ends with a git commit** —
the repo must be in a consistent, buildable state at every phase boundary.
Phase commits use Conventional Commits style (`feat: ...`, `docs: ...`, etc.).

### Phase 1 — Research & model validation
- Download the `q8_0` GGUF into `models/`.
- Prove `nemo-speech` streaming inference on this hardware: latency and
  word-by-word emission test with a WAV file and with the live mic.
- Record findings (latency numbers, chunk-size trade-offs, any runtime quirks)
  in `docs/research.md`.
- **Commit** after this phase.

### Phase 2 — Core skeleton & audio capture
- Repo layout, `main.py`, config loading + validation, FastAPI app with
  `/health`.
- Non-blocking mic capture (`core/audio.py`) → asyncio queue; verify drop
  behavior under load.
- **Commit** after this phase.

### Phase 3 — ASR integration & word stream
- `core/asr.py`: nemo-speech streaming engine wired to the audio queue.
- `core/wordstream.py`: word events (`text`, `index`, `is_final`), rolling
  window state.
- WebSocket endpoint `/ws` streaming word events end-to-end.
- **Commit** after this phase.

### Phase 4 — Overlay
- `core/overlay/`: sliding window UI, newest-word highlight, `dim_step`
  fading, CSS slide-out, keyword trigger badges.
- All styling driven by config; page fully self-contained (no CDN).
- **Commit** after this phase.

### Phase 5 — Keyword engine
- `core/keywords.py`: matching rules (case-insensitive, finalized words),
  per-trigger cooldown, fire-and-forget dispatch with exception logging.
- Config validation: every `action` must map to a registered plugin action.
- **Commit** after this phase.

### Phase 6 — Plugin system & OBS plugin
- `plugins/base.py` (Plugin base class), `core/plugins.py` (PluginManager:
  load, validate, dispatch off the main loop).
- `plugins/obs_plugin/`: obs-websocket 5.x connection with background
  reconnect, actions `change_scene`, `record_clip`, `toggle_source`.
- **Commit** after this phase.

### Phase 7 — Polish & release
- README.md, `config/logos_config.yaml.example`, logging pass, clean
  shutdown, final review against this spec.
- **Commit** after this phase (tag `v0.1.0`).
