from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from live_engine.parity.replay import HistoricalReplayRunner, ParityReport


def __getattr__(name: str):
    if name in ("HistoricalReplayRunner", "ParityReport"):
        from live_engine.parity.replay import HistoricalReplayRunner, ParityReport
        return locals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["HistoricalReplayRunner", "ParityReport"]
