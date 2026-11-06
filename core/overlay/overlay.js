"use strict";

// Embedded by the server from config/logos_config.yaml (overlay section).
const CONFIG = /*__CONFIG__*/{};

const wordsEl = document.getElementById("words");
const track = document.getElementById("track");
const badgesEl = document.getElementById("badges");

// index -> { text, el }
const words = new Map();
let lastIndex = -1;
let lastWordAt = 0;

const HIDE_AFTER_MS = (CONFIG.hide_after_seconds ?? 0) * 1000;

function layout() {
  // Pin the newest word's right edge at the horizontal center. The whole
  // track is moved with a (transitioned) transform, so words scroll left
  // smoothly instead of the flex row reflowing (no layout jumps).
  const anchor = wordsEl.clientWidth / 2;
  const x = anchor - track.scrollWidth;
  track.style.transform = `translateX(${x}px)`;
}

function applyFading() {
  // Newest word is index === lastIndex; each step back loses dim_step.
  for (const [index, w] of words) {
    const age = lastIndex - index;
    w.el.classList.toggle("latest", age === 0);
    const dim = CONFIG.dim_step ?? 0.25;
    w.el.style.opacity = Math.max(1 - dim * age, 0.05);
  }
}

function prune() {
  const keep = CONFIG.window_size ?? 5;
  const indices = [...words.keys()].sort((a, b) => a - b);
  while (indices.length > keep) {
    const oldest = indices.shift();
    const w = words.get(oldest);
    if (!w) continue;
    w.el.remove();
    words.delete(oldest);
  }
  applyFading();
  layout();
}

function addWord(event) {
  if (event.index <= lastIndex && words.has(event.index)) return;
  lastIndex = Math.max(lastIndex, event.index);
  const el = document.createElement("span");
  el.className = "word";
  el.textContent = event.text;
  track.appendChild(el);
  words.set(event.index, { text: event.text, el });
  lastWordAt = performance.now();
  wordsEl.classList.remove("hidden");
  prune();
}

function addBadge(trigger) {
  const el = document.createElement("span");
  el.className = "badge";
  el.textContent = "\u26A1 " + trigger;
  badgesEl.appendChild(el);
  setTimeout(() => el.remove(), 2300);
}

// The server sends a ping at least every 15 s, so any silence longer
// than this means the connection is dead (no onclose ever fired):
// close it so the reconnect below kicks in.
const STALE_TIMEOUT_MS = 30000;

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  let staleTimer = null;
  const armWatchdog = () => {
    if (staleTimer !== null) clearTimeout(staleTimer);
    staleTimer = setTimeout(() => ws.close(), STALE_TIMEOUT_MS);
  };
  ws.onmessage = (msg) => {
    armWatchdog();
    const event = JSON.parse(msg.data);
    if (event.type === "word") addWord(event);
    else if (event.type === "trigger" && CONFIG.show_keyword_triggers) {
      addBadge(event.trigger);
    }
  };
  ws.onclose = () => {
    if (staleTimer !== null) clearTimeout(staleTimer);
    setTimeout(connect, 1500);
  };
  armWatchdog();
}

// Auto-hide the whole overlay after `hide_after_seconds` of silence
// (0 = never hide).
setInterval(() => {
  if (HIDE_AFTER_MS > 0 && lastWordAt > 0 &&
      performance.now() - lastWordAt > HIDE_AFTER_MS) {
    wordsEl.classList.add("hidden");
  }
}, 500);

window.addEventListener("resize", layout);

// Apply config-driven styling.
const root = document.documentElement.style;
root.setProperty("--font-family", CONFIG.font_family || "Inter, sans-serif");
root.setProperty("--font-size", (CONFIG.font_size_px || 42) + "px");
root.setProperty("--color", CONFIG.color || "#ffffff");
root.setProperty("--highlight-color", CONFIG.highlight_color || "#ffd93d");

connect();
