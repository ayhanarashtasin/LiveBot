"""Coin dashboards, lazily exported so python -m runs without double imports."""
from importlib import import_module

__all__ = ["BTCDashboard", "HYPEDashboard", "LITDashboard", "ZECDashboard"]


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    module = "hype_dashboard" if name == "HYPEDashboard" else f"{name[:3].lower()}_dashboard"
    return getattr(import_module(f".{module}", __name__), name)
