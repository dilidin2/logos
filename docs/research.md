# Phase 1 — Research & Model Validation

Date: 2026-10-12
Hardware: AMD Ryzen 7 7800X3D (8c/16t), Radeon RX 7900 XT (20 GB — fully
occupied by the local LLM, so all tests ran on the **CPU** build),
openSUSE Tumbleweed.

## Runtime: `nemo-speech` (NeMo-Speech.cpp)

- `nemo-speech` is **not** a Python package — it is NVIDIA's official native
  C++ runtime ([NVIDIA/NeMo-Speech.cpp](https://github.com/NVIDIA/NeMo-Speech.cpp),
  ggml-based). Installed v0.2.0 via the official install script into
  `~/.local/share/nemo-speech` (cpu build).
- Provides: CLI (`transcribe`, `bench`, `serve`, ...), a C API, and a local
  HTTP/WebSocket server with a realtime ASR protocol.
- **Integration decision for Phase 3**: the core will run `nemo-speech serve`
  as a child process and stream audio to its realtime WebSocket
  (`/v1/audio/transcriptions/realtime`). No Python bindings needed.

## Model

- `nvidia/nemotron-3.5-asr-streaming-0.6b`, file
  `nemotron-3.5-asr-streaming-0.6b.q8_0.gguf` (708 MB), downloaded to
  `models/` via `huggingface_hub`.
- 40 language-locales; `it-IT` is in the **transcription-ready** tier (as is
  `en-US`), so `language: it` in config maps directly to the model's
  language-ID prompt conditioning.
- Native punctuation & capitalization: confirmed in all outputs.

## Realtime WebSocket protocol (key findings)

- `GET /health` → `{"status":"ok"}` once the model is loaded (~6 s on CPU).
- Connect → server sends `session.created`.
- Send one `session.update` (JSON) before audio: `sample_rate`, `language`,
  `word_timestamps`, `endpointing_ms`, ...
- Then send binary little-endian **PCM16 mono** frames (160 ms = 5120 bytes
  works well).
- Server events:
  - `conversation.item.input_audio_transcription.delta` — **append-style
    token deltas** (`delta` field, e.g. `" fel"`, `"low"`, `"ca"`), plus
    `audio_processed` (seconds of audio consumed so far). Deltas arrive
    immediately after each 160 ms chunk (measured lag ≈ −0.02…−0.06 s, i.e.
    no perceptible latency on this CPU).
  - `conversation.item.input_audio_transcription.completed` — final text for
    the utterance (`transcript` field, plus `words[]` with `start`/`end` when
    `word_timestamps` is on).
  - `input_audio_buffer.commit` / `input_audio_buffer.committed` — flush.
  - `input_audio_buffer.clear`, `response.cancel` — discard.
- **Implication**: deltas are sub-word tokens, not whole words. The
  `WordStream` (Phase 3) must accumulate deltas and split into words
  (space-delimited), emitting a word event when a space completes it; the
  `completed` event finalizes the utterance (all words `is_final=true`).

## Latency & throughput (CPU, q8_0)

Test: `scripts/phase1_realtime_test.py` streams `jfk.wav` (11 s, 16 kHz mono)
at real-time pace in 160 ms chunks.

| Metric | Value |
|---|---|
| First partial | ~1.26 s after stream start (first speech in file starts ~1.0 s in) |
| Token-delta lag vs `audio_processed` | ≈ 0 s (−0.02…−0.06 s) |
| Final after end of audio | ~0.27 s |
| Offline bench (11 s WAV, concurrency 1) | ~21× real time, P50 492 ms |
| Model load time (CPU) | ~2.3 s |

Chunk-size sweep (same WAV, streaming): **80 / 160 / 320 ms produce
identical transcripts** on this clean audio, with no measurable latency
difference on CPU — the CPU is fast enough that the model's latency-accuracy
trade-off is not the bottleneck here. 160 ms remains the default (matches
spec; safest for noisy real-world audio).

Config key for chunk size when running the server:
`--asr.streaming.chunk_size 0.16` (seconds).

## Live microphone

- `nemo-speech transcribe --live` captures the default mic
  ("HyperX Virtual Surround Sound Analog Stereo") at 16 kHz cleanly;
  Ctrl-C stops it gracefully. The core will do its own PortAudio capture
  (spec §3) and feed the WebSocket, so `--live` is only a validation tool.

## Endpointing (Phase 3 finding)

- `completed` events are **not** emitted on their own: the server needs
  the `--asr.endpointing.enable` flag (boolean flag, no value) at startup.
  With it, each silence pause (≈ `endpointing_ms` in `session.update`,
  800 ms used) finalizes an utterance and emits
  `conversation.item.input_audio_transcription.completed`.
- Without it, a final only arrives after a manual
  `input_audio_buffer.commit` — unusable for a continuous mic stream.

## Quirks / notes

- The realtime WS closes with code 1011 if a non-bytes frame is sent —
  keep audio frames strictly `bytes`.
- RNNT token deltas sometimes detach punctuation from the word
  (e.g. `. A sk` for `. Ask`); the ASR engine re-attaches punct-only
  tokens to the next word.
- `session.update` is rejected once audio has started.
- The CLI downloads its own model cache; we bypass it by passing the local
  GGUF path explicitly (`--model <path>`).
- Server binds to 127.0.0.1 by default — matches the localhost-only rule.
