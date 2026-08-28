"""Last-seen tracking, so feeder access survives a reconnect."""

from collections.abc import Iterable

DEFAULT_WINDOW_SECONDS = 3600


from mutmut.mutation.trampoline import wrap_in_trampoline as _mutmut_mutated, MutantDict
mutants_xǁFeederSetǁ__init____mutmut: MutantDict = {}  # type: ignore
mutants_xǁFeederSetǁseed__mutmut: MutantDict = {}  # type: ignore
mutants_xǁFeederSetǁobserve__mutmut: MutantDict = {}  # type: ignore
mutants_xǁFeederSetǁactive__mutmut: MutantDict = {}  # type: ignore
mutants_xǁFeederSetǁprune__mutmut: MutantDict = {}  # type: ignore


class FeederSet:
    """Prefixes with expiry. A prefix stays active `window_seconds` after last seen."""

    @_mutmut_mutated(mutants_xǁFeederSetǁ__init____mutmut)
    def __init__(self, window_seconds: int = DEFAULT_WINDOW_SECONDS) -> None:
        self._window = window_seconds
        self._last_seen: dict[str, float] = {}

    def xǁFeederSetǁ__init____mutmut_orig(self, window_seconds: int = DEFAULT_WINDOW_SECONDS) -> None:
        self._window = window_seconds
        self._last_seen: dict[str, float] = {}

    def xǁFeederSetǁ__init____mutmut_1(self, window_seconds: int = DEFAULT_WINDOW_SECONDS) -> None:
        self._window = None
        self._last_seen: dict[str, float] = {}

    def xǁFeederSetǁ__init____mutmut_2(self, window_seconds: int = DEFAULT_WINDOW_SECONDS) -> None:
        self._window = window_seconds
        self._last_seen: dict[str, float] = None

    @_mutmut_mutated(mutants_xǁFeederSetǁseed__mutmut)
    def seed(self, prefixes: Iterable[str], now: float) -> None:
        """Load prefixes recovered from the cluster object at startup.

        Identical to observe(), named separately so the call site reads as
        what it is: recovering state we previously wrote, not a fresh sighting.
        """
        self.observe(prefixes, now)

    def xǁFeederSetǁseed__mutmut_orig(self, prefixes: Iterable[str], now: float) -> None:
        """Load prefixes recovered from the cluster object at startup.

        Identical to observe(), named separately so the call site reads as
        what it is: recovering state we previously wrote, not a fresh sighting.
        """
        self.observe(prefixes, now)

    def xǁFeederSetǁseed__mutmut_1(self, prefixes: Iterable[str], now: float) -> None:
        """Load prefixes recovered from the cluster object at startup.

        Identical to observe(), named separately so the call site reads as
        what it is: recovering state we previously wrote, not a fresh sighting.
        """
        self.observe(None, now)

    def xǁFeederSetǁseed__mutmut_2(self, prefixes: Iterable[str], now: float) -> None:
        """Load prefixes recovered from the cluster object at startup.

        Identical to observe(), named separately so the call site reads as
        what it is: recovering state we previously wrote, not a fresh sighting.
        """
        self.observe(prefixes, None)

    def xǁFeederSetǁseed__mutmut_3(self, prefixes: Iterable[str], now: float) -> None:
        """Load prefixes recovered from the cluster object at startup.

        Identical to observe(), named separately so the call site reads as
        what it is: recovering state we previously wrote, not a fresh sighting.
        """
        self.observe(now)

    def xǁFeederSetǁseed__mutmut_4(self, prefixes: Iterable[str], now: float) -> None:
        """Load prefixes recovered from the cluster object at startup.

        Identical to observe(), named separately so the call site reads as
        what it is: recovering state we previously wrote, not a fresh sighting.
        """
        self.observe(prefixes, )

    @_mutmut_mutated(mutants_xǁFeederSetǁobserve__mutmut)
    def observe(self, prefixes: Iterable[str], now: float) -> None:
        for prefix in prefixes:
            self._last_seen[prefix] = now

    def xǁFeederSetǁobserve__mutmut_orig(self, prefixes: Iterable[str], now: float) -> None:
        for prefix in prefixes:
            self._last_seen[prefix] = now

    def xǁFeederSetǁobserve__mutmut_1(self, prefixes: Iterable[str], now: float) -> None:
        for prefix in prefixes:
            self._last_seen[prefix] = None

    @_mutmut_mutated(mutants_xǁFeederSetǁactive__mutmut)
    def active(self, now: float) -> set[str]:
        cutoff = now - self._window
        return {p for p, seen in self._last_seen.items() if seen >= cutoff}

    def xǁFeederSetǁactive__mutmut_orig(self, now: float) -> set[str]:
        cutoff = now - self._window
        return {p for p, seen in self._last_seen.items() if seen >= cutoff}

    def xǁFeederSetǁactive__mutmut_1(self, now: float) -> set[str]:
        cutoff = None
        return {p for p, seen in self._last_seen.items() if seen >= cutoff}

    def xǁFeederSetǁactive__mutmut_2(self, now: float) -> set[str]:
        cutoff = now + self._window
        return {p for p, seen in self._last_seen.items() if seen >= cutoff}

    def xǁFeederSetǁactive__mutmut_3(self, now: float) -> set[str]:
        cutoff = now - self._window
        return {p for p, seen in self._last_seen.items() if seen > cutoff}

    @_mutmut_mutated(mutants_xǁFeederSetǁprune__mutmut)
    def prune(self, now: float) -> int:
        """Drop expired entries. Returns how many were removed."""
        cutoff = now - self._window
        expired = [p for p, seen in self._last_seen.items() if seen < cutoff]
        for prefix in expired:
            del self._last_seen[prefix]
        return len(expired)

    def xǁFeederSetǁprune__mutmut_orig(self, now: float) -> int:
        """Drop expired entries. Returns how many were removed."""
        cutoff = now - self._window
        expired = [p for p, seen in self._last_seen.items() if seen < cutoff]
        for prefix in expired:
            del self._last_seen[prefix]
        return len(expired)

    def xǁFeederSetǁprune__mutmut_1(self, now: float) -> int:
        """Drop expired entries. Returns how many were removed."""
        cutoff = None
        expired = [p for p, seen in self._last_seen.items() if seen < cutoff]
        for prefix in expired:
            del self._last_seen[prefix]
        return len(expired)

    def xǁFeederSetǁprune__mutmut_2(self, now: float) -> int:
        """Drop expired entries. Returns how many were removed."""
        cutoff = now + self._window
        expired = [p for p, seen in self._last_seen.items() if seen < cutoff]
        for prefix in expired:
            del self._last_seen[prefix]
        return len(expired)

    def xǁFeederSetǁprune__mutmut_3(self, now: float) -> int:
        """Drop expired entries. Returns how many were removed."""
        cutoff = now - self._window
        expired = None
        for prefix in expired:
            del self._last_seen[prefix]
        return len(expired)

    def xǁFeederSetǁprune__mutmut_4(self, now: float) -> int:
        """Drop expired entries. Returns how many were removed."""
        cutoff = now - self._window
        expired = [p for p, seen in self._last_seen.items() if seen <= cutoff]
        for prefix in expired:
            del self._last_seen[prefix]
        return len(expired)

    def __len__(self) -> int:
        return len(self._last_seen)

mutants_xǁFeederSetǁ__init____mutmut['_mutmut_orig'] = FeederSet.xǁFeederSetǁ__init____mutmut_orig # type: ignore # mutmut generated
mutants_xǁFeederSetǁ__init____mutmut['xǁFeederSetǁ__init____mutmut_1'] = FeederSet.xǁFeederSetǁ__init____mutmut_1 # type: ignore # mutmut generated
mutants_xǁFeederSetǁ__init____mutmut['xǁFeederSetǁ__init____mutmut_2'] = FeederSet.xǁFeederSetǁ__init____mutmut_2 # type: ignore # mutmut generated

mutants_xǁFeederSetǁseed__mutmut['_mutmut_orig'] = FeederSet.xǁFeederSetǁseed__mutmut_orig # type: ignore # mutmut generated
mutants_xǁFeederSetǁseed__mutmut['xǁFeederSetǁseed__mutmut_1'] = FeederSet.xǁFeederSetǁseed__mutmut_1 # type: ignore # mutmut generated
mutants_xǁFeederSetǁseed__mutmut['xǁFeederSetǁseed__mutmut_2'] = FeederSet.xǁFeederSetǁseed__mutmut_2 # type: ignore # mutmut generated
mutants_xǁFeederSetǁseed__mutmut['xǁFeederSetǁseed__mutmut_3'] = FeederSet.xǁFeederSetǁseed__mutmut_3 # type: ignore # mutmut generated
mutants_xǁFeederSetǁseed__mutmut['xǁFeederSetǁseed__mutmut_4'] = FeederSet.xǁFeederSetǁseed__mutmut_4 # type: ignore # mutmut generated

mutants_xǁFeederSetǁobserve__mutmut['_mutmut_orig'] = FeederSet.xǁFeederSetǁobserve__mutmut_orig # type: ignore # mutmut generated
mutants_xǁFeederSetǁobserve__mutmut['xǁFeederSetǁobserve__mutmut_1'] = FeederSet.xǁFeederSetǁobserve__mutmut_1 # type: ignore # mutmut generated

mutants_xǁFeederSetǁactive__mutmut['_mutmut_orig'] = FeederSet.xǁFeederSetǁactive__mutmut_orig # type: ignore # mutmut generated
mutants_xǁFeederSetǁactive__mutmut['xǁFeederSetǁactive__mutmut_1'] = FeederSet.xǁFeederSetǁactive__mutmut_1 # type: ignore # mutmut generated
mutants_xǁFeederSetǁactive__mutmut['xǁFeederSetǁactive__mutmut_2'] = FeederSet.xǁFeederSetǁactive__mutmut_2 # type: ignore # mutmut generated
mutants_xǁFeederSetǁactive__mutmut['xǁFeederSetǁactive__mutmut_3'] = FeederSet.xǁFeederSetǁactive__mutmut_3 # type: ignore # mutmut generated

mutants_xǁFeederSetǁprune__mutmut['_mutmut_orig'] = FeederSet.xǁFeederSetǁprune__mutmut_orig # type: ignore # mutmut generated
mutants_xǁFeederSetǁprune__mutmut['xǁFeederSetǁprune__mutmut_1'] = FeederSet.xǁFeederSetǁprune__mutmut_1 # type: ignore # mutmut generated
mutants_xǁFeederSetǁprune__mutmut['xǁFeederSetǁprune__mutmut_2'] = FeederSet.xǁFeederSetǁprune__mutmut_2 # type: ignore # mutmut generated
mutants_xǁFeederSetǁprune__mutmut['xǁFeederSetǁprune__mutmut_3'] = FeederSet.xǁFeederSetǁprune__mutmut_3 # type: ignore # mutmut generated
mutants_xǁFeederSetǁprune__mutmut['xǁFeederSetǁprune__mutmut_4'] = FeederSet.xǁFeederSetǁprune__mutmut_4 # type: ignore # mutmut generated
