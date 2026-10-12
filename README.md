# logos

Local, real-time speech-to-text overlay and action dispatcher.

logos listens to the microphone, transcribes speech with a streaming ASR
model (NVIDIA Nemotron 3.5 ASR 0.6B, q8_0 GGUF, via the official
`nemo-speech` runtime), shows a live sliding window of the most recent
words on an HTML/JS overlay (for a OBS Browser Source), and dispatches
configurable keyword triggers to in-process plugins (e.g. OBS actions).

Everything runs locally on this machine; the server binds to
`127.0.0.1` only.

## Setup

One command does everything (system library, `nemo-speech` runtime,
virtualenv, model download, initial config). Safe to re-run:

```sh
cd ~/logos
./install.sh
$EDITOR config/logos_config.yaml
```

- Supported distros: openSUSE (zypper), Debian/Ubuntu (apt), Arch (pacman)
- Python 3.12+ required
- The one-off model download is ~708 MB into `models/`
- `nemo-speech` is installed to `~/.local/share/nemo-speech`

## Run

```sh
.venv/bin/python main.py
```

- Overlay: <http://127.0.0.1:8765/> (add as a Browser Source in OBS)
- Health: <http://127.0.0.1:8765/health>
- Stop with Ctrl-C (clean shutdown of capture, ASR, and plugins).

The first start loads the ASR model (~2-3 s on CPU). If OBS is not
running, the OBS plugin keeps retrying in the background and connects
automatically when OBS starts.

## Configuration

All runtime behavior lives in `config/logos_config.yaml` (see the
`.example` file): ASR model/language/chunk size, overlay styling and
window size, keyword triggers with their actions and args, and plugin
settings. Invalid values fail fast at startup with a readable message.

### Keyword triggers

Triggers match case-insensitively against the last N finalized words
(N = `overlay.window_size`). Each trigger has an independent cooldown
(`keywords.cooldown_seconds`). Actions are namespaced
`<plugin>.<action>`, e.g. `obs.change_scene` with `args: {scene: "..."}`.

Available OBS actions:

| Action | Args | Effect |
|---|---|---|
| `obs.change_scene` | `scene` | Switch the current OBS scene |
| `obs.record_clip` | — | Toggle OBS clip recording |
| `obs.toggle_source` | `source` | Show/hide a source in the current scene |

## Project layout

```
main.py                entry point
core/
  audio.py             non-blocking mic capture -> asyncio queue
  asr.py               nemo-speech streaming engine (child process + WS)
  wordstream.py        word events, sliding window, finalization
  keywords.py          trigger matching, cooldown, dispatch
  plugins.py           PluginManager
  config.py            config loading + validation
  server.py            FastAPI app (/, /ws, /health)
  overlay/             self-contained overlay page (HTML/CSS/JS)
plugins/
  base.py              Plugin base class
  obs_plugin/          OBS plugin (obs-websocket 5.x)
config/                logos_config.yaml (+ .example)
models/                ASR model weights (git-ignored)
scripts/               phase test harnesses
docs/research.md       Phase 1 findings (latency, protocol quirks)
```

## Development

Phases and design rules are in `SPEC.md` and `AGENTS.md`. Every phase
ends with a Conventional-Commits git commit. Test harnesses:

```sh
PYTHONPATH=. .venv/bin/python scripts/phase3_e2e_test.py   # WAV -> ASR -> /ws
PYTHONPATH=. .venv/bin/python scripts/phase6_plugin_test.py  # fake OBS server
```
