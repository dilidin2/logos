"use strict";

// Embedded by the server from config/logos_config.yaml (overlay section).
const CONFIG = /*__CONFIG__*/{};

const wordsEl = document.getElementById("words");
const badgesEl = document.getElementById("badges");

// index -> { text, el }
const words = new Map();
let lastIndex = -1;

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
    w.el.classList.add("sliding-out");
    words.delete(oldest);
    setTimeout(() => w.el.remove(), 320);
  }
  applyFading();
}

function addWord(event) {
  if (event.index <= lastIndex && words.has(event.index)) return;
  lastIndex = Math.max(lastIndex, event.index);
  const el = document.createElement("span");
  el.className = "word";
  el.textContent = event.text;
  wordsEl.appendChild(el);
  words.set(event.index, { text: event.text, el });
  prune();
}

function addBadge(trigger) {
  const el = document.createElement("span");
  el.className = "badge";
  el.textContent = "\u26A1 " + trigger;
  badgesEl.appendChild(el);
  setTimeout(() => el.remove(), 2300);
}

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onmessage = (msg) => {
    const event = JSON.parse(msg.data);
    if (event.type === "word") addWord(event);
    else if (event.type === "trigger" && CONFIG.show_keyword_triggers) {
      addBadge(event.trigger);
    }
  };
  ws.onclose = () => setTimeout(connect, 1500);
}

// Apply config-driven styling.
const root = document.documentElement.style;
root.setProperty("--font-family", CONFIG.font_family || "Inter, sans-serif");
root.setProperty("--font-size", (CONFIG.font_size_px || 42) + "px");
root.setProperty("--color", CONFIG.color || "#ffffff");
root.setProperty("--highlight-color", CONFIG.highlight_color || "#ffd93d");

connect();
