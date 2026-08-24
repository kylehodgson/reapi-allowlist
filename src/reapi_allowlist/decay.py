"""Last-seen tracking, so feeder access survives a reconnect."""

from collections.abc import Iterable

DEFAULT_WINDOW_SECONDS = 3600


class FeederSet:
    """Prefixes with expiry. A prefix stays active `window_seconds` after last seen."""

    def __init__(self, window_seconds: int = DEFAULT_WINDOW_SECONDS) -> None:
        self._window = window_seconds
        self._last_seen: dict[str, float] = {}

    def seed(self, prefixes: Iterable[str], now: float) -> None:
        """Load prefixes recovered from the cluster object at startup.

        Identical to observe(), named separately so the call site reads as
        what it is: recovering state we previously wrote, not a fresh sighting.
        """
        self.observe(prefixes, now)

    def observe(self, prefixes: Iterable[str], now: float) -> None:
        for prefix in prefixes:
            self._last_seen[prefix] = now

    def active(self, now: float) -> set[str]:
        cutoff = now - self._window
        return {p for p, seen in self._last_seen.items() if seen >= cutoff}

    def prune(self, now: float) -> int:
        """Drop expired entries. Returns how many were removed."""
        cutoff = now - self._window
        expired = [p for p, seen in self._last_seen.items() if seen < cutoff]
        for prefix in expired:
            del self._last_seen[prefix]
        return len(expired)

    def __len__(self) -> int:
        return len(self._last_seen)
