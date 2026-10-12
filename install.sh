#!/usr/bin/env bash
# logos installer: sets up everything needed to run the project.
#
#   ./install.sh          -> system deps, nemo-speech, venv, model
#
# Safe to re-run: every step is skipped if already done.

set -euo pipefail
cd "$(dirname "$0")"

VENV=".venv"
MODEL_DIR="models"
MODEL_FILE="$MODEL_DIR/nemotron-3.5-asr-streaming-0.6b.q8_0.gguf"
NEMO_BIN="$HOME/.local/share/nemo-speech/bin/nemo-speech"
CONFIG="config/logos_config.yaml"

info()  { echo -e "\033[1;32m[install]\033[0m $*"; }
skip()  { echo -e "\033[1;36m[skip]\033[0m $*"; }
need_sudo() {
    if command -v sudo >/dev/null 2>&1; then sudo "$@"; else "$@"; fi
}

# ---------------------------------------------------------------- system
# sounddevice (pure wheel) only needs the libportaudio2 runtime library.
# Detect the distro from /etc/os-release and use the right package manager.
DISTRO=""
if [[ -r /etc/os-release ]]; then
    . /etc/os-release
    DISTRO="${ID:-}"
    [[ -z "$DISTRO" ]] && DISTRO="${ID_LIKE:-}"
    DISTRO="${DISTRO,,}"
fi

have_portaudio() {
    python3 -c "import ctypes; ctypes.CDLL('libportaudio.so.2')" 2>/dev/null
}

install_portaudio() {
    case "$DISTRO" in
        openSUSE*|suse*)
            need_sudo zypper install -y libportaudio2 ;;
        debian|ubuntu)
            need_sudo apt-get update -qq
            need_sudo apt-get install -y libportaudio2 ;;
        arch)
            need_sudo pacman -Sy --noconfirm portaudio ;;
        *)
            echo "unknown/unsupported distro '${DISTRO:-?}': install the 'portaudio' runtime library manually" >&2
            return 1 ;;
    esac
}

if have_portaudio; then
    skip "libportaudio2 already installed"
else
    info "installing libportaudio2 (distro: ${DISTRO:-unknown}, needs sudo)"
    install_portaudio
fi

# ------------------------------------------------------------ nemo-speech
if [[ -x "$NEMO_BIN" ]]; then
    skip "nemo-speech already installed ($($NEMO_BIN --version 2>/dev/null | head -1))"
else
    info "installing nemo-speech (NeMo-Speech.cpp runtime)"
    curl -fsSL https://github.com/NVIDIA/NeMo-Speech.cpp/raw/main/scripts/install.sh | sh
    [[ -x "$NEMO_BIN" ]] || { echo "nemo-speech install failed"; exit 1; }
fi

# ------------------------------------------------------------------- venv
if [[ ! -x "$VENV/bin/python" ]]; then
    info "creating virtualenv"
    python3 -m venv "$VENV"
fi
info "installing Python dependencies"
"$VENV/bin/pip" install -q -r requirements.txt

# --------------------------------------------------------------- model
if [[ -f "$MODEL_FILE" ]]; then
    skip "ASR model already present ($MODEL_FILE)"
else
    info "downloading ASR model (~708 MB)"
    mkdir -p "$MODEL_DIR"
    "$VENV/bin/python" - <<'EOF'
from huggingface_hub import hf_hub_download

hf_hub_download(
    repo_id="nvidia/nemotron-3.5-asr-streaming-0.6b",
    filename="nemotron-3.5-asr-streaming-0.6b.q8_0.gguf",
    local_dir="models",
)
EOF
fi

# -------------------------------------------------------------- config
if [[ ! -f "$CONFIG" ]]; then
    info "creating config/logos_config.yaml from example"
    cp config/logos_config.yaml.example "$CONFIG"
fi

info "done. Start logos with:  $VENV/bin/python main.py"
