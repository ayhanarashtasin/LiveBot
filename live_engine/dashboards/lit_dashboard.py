"""LITUSDT pullback observability in PAPER and SHADOW."""
from .base import BaseDashboard, metric_cards, run_dashboard_cli


class LITDashboard(BaseDashboard):
    symbol = "LITUSDT"
    timeframe = "15m"
    benchmark_id = "LIT_SUPERTREND_15M"
    strategy_class = "LITSupertrendPullback15M"
    supertrend_defaults = (28, 2.0)

    def cards(self):
        return ('<p>LITSupertrendPullback15M · 15m · Supertrend (28, 2.0)</p>' + metric_cards([
            ("atr", "ATR(7)"), ("pullback_distance", "Distance above Supertrend / ATR(7)"),
            ("pullback_limit", "Pullback threshold: 0.5 × ATR(7)"),
        ]))


if __name__ == "__main__":
    run_dashboard_cli(LITDashboard)
