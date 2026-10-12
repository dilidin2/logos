"""Word events and sliding window state.

A WordStream assigns a monotonic index to every word, keeps a rolling
display window of the most recent `window_size` words, and tracks which
words are finalized (spec section 4). Keyword matching uses finalized
words only, via `finalized_window_text`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class WordEvent:
    """A single word in the stream.

    Interim events (is_final=False) are emitted as tokens are produced;
    a final event (is_final=True) reuses the same index and text.
    """

    text: str
    index: int
    is_final: bool


class WordStream:
    """Rolling window of words with monotonic indexing and finalization."""

    def __init__(
        self,
        window_size: int,
        on_word: Callable[[WordEvent], None] | None = None,
        history_size: int = 64,
    ) -> None:
        self._window_size = window_size
        self._history_size = max(history_size, window_size)
        self._events: list[WordEvent] = []
        self._finalized: set[int] = set()
        self._next_index = 0
        self._on_word = on_word
        self.total_words = 0

    @property
    def window(self) -> list[WordEvent]:
        """The current display window, oldest first."""
        return self._events[-self._window_size:]

    @property
    def window_text(self) -> str:
        """Space-joined text of the display window (oldest first)."""
        return " ".join(e.text for e in self.window)

    @property
    def finalized_window_text(self) -> str:
        """Space-joined text of the most recent finalized words, limited
        to `window_size` (the stable text used for keyword matching)."""
        finalized = [e for e in self._events if e.index in self._finalized]
        return " ".join(e.text for e in finalized[-self._window_size:])

    def add_word(self, text: str, is_final: bool = False) -> WordEvent:
        """Register a new word and emit its (interim) event."""
        event = WordEvent(text=text, index=self._next_index, is_final=is_final)
        self._next_index += 1
        self.total_words += 1
        self._events.append(event)
        del self._events[: -self._history_size]
        if is_final:
            self._finalized.add(index := event.index)
        if self._on_word is not None:
            self._on_word(event)
        return event

    def mark_final(self, index: int) -> WordEvent | None:
        """Mark a previously interim word as finalized and emit a final
        event. Works even if the word already slid out of the display
        window (it is then only tracked, not re-broadcast)."""
        if index in self._finalized:
            return None
        self._finalized.add(index)
        for event in self._events:
            if event.index == index:
                final = WordEvent(text=event.text, index=index, is_final=True)
                if self._on_word is not None:
                    self._on_word(final)
                return final
        return None
