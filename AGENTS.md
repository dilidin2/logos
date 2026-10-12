# AGENTS.md — Working rules for this repo

## Language
- **Everything is written in English, for everyone**: this rule applies to all
  contributors (humans and AI agents alike). Code, comments, docstrings, config
  files, docs, commit messages, logs — English only. No exceptions.

## Continuity & patterns
- Before writing any new code, **read the existing code first** and follow the
  patterns already established in this repo (structure, naming, logging, config
  handling, error handling).
- When in doubt about how something should be done, look at how similar
  things are already implemented here and stay consistent with that.
- The reference project for general patterns (FastAPI app, YAML config,
  loguru logging, service layout) is `/home/matteo/phonema` — mirror its
  conventions where this repo does not define its own.

## Project rules
- Follow `SPEC.md` for architecture and behavior. If a change contradicts the
  spec, update the spec in the same change.
- The core does **only** transcription + overlay + keyword dispatch. Any
  external integration (OBS, etc.) lives in `plugins/` as a plugin module.
- Nothing may block the main event loop: audio capture, ASR inference and
  plugin actions all run off the main loop (threads/tasks) with queues between
  stages.
- All runtime behavior (language, chunk size, overlay styling, sliding window,
  keyword list, cooldown, plugin settings) is configured in
  `config/logos_config.yaml` — never hardcode user-facing values.
- Keep the project runnable on this machine: openSUSE Tumbleweed,
  AMD Ryzen 7 7800X3D + Radeon RX 7900 XT (20 GB), Python via venv.

## Code style
- Python 3.12+, `loguru` for logging, `aiohttp`/`asyncio` for network I/O.
- Type hints on public functions.
- Small modules with a single responsibility; no god-files.
- Every public class/module gets a short docstring describing its role.
