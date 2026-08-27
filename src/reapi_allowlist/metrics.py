"""Prometheus text output, hand-rolled to match adsblol/api's convention."""

from dataclasses import dataclass, field


@dataclass
class Metrics:
    set_size: int = 0
    adds: int = 0
    removes: int = 0
    anomalies: int = 0
    internal_prefixes: int = 0
    source_errors: int = 0
    last_success: float | None = None
    refusals: dict[str, int] = field(default_factory=dict)
    no_change: int = 0
    large_shrink: int = 0
    consecutive_partial_cycles: int = 0

    def render(self, now: float) -> str:
        since = -1 if self.last_success is None else int(now - self.last_success)
        lines = [
            f"adsb_reapi_allowlist_size {self.set_size}",
            f"adsb_reapi_allowlist_adds {self.adds}",
            f"adsb_reapi_allowlist_removes {self.removes}",
            f"adsb_reapi_allowlist_parse_anomalies {self.anomalies}",
            f"adsb_reapi_allowlist_internal_prefixes {self.internal_prefixes}",
            f"adsb_reapi_allowlist_large_shrink {self.large_shrink}",
            f"adsb_reapi_allowlist_source_errors {self.source_errors}",
            f"adsb_reapi_allowlist_seconds_since_success {since}",
            f"adsb_reapi_allowlist_no_change {self.no_change}",
            f"adsb_reapi_allowlist_consecutive_partial_cycles {self.consecutive_partial_cycles}",
        ]
        lines += [
            f'adsb_reapi_allowlist_refusals{{reason="{reason}"}} {count}'
            for reason, count in sorted(self.refusals.items())
        ]
        return "\n".join(lines) + "\n"
