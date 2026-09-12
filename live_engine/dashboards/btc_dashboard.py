"""BTCUSDT ST_09 Supertrend pullback observability."""
from dataclasses import asdict

from .base import BaseDashboard, metric_cards, run_dashboard_cli


class BTCDashboard(BaseDashboard):
    symbol = "BTCUSDT"
    timeframe = "5m"
    benchmark_id = "BTC_ST_09_5M"
    strategy_class = "BTCSupertrendPullback5M"
    supertrend_defaults = (10, 3.0)
    allowed_modes = ("PAPER", "SHADOW", "LIVE")

    def cards(self):
        return ('<p>BTCSupertrendPullback5M (ST_09) · 5m · Supertrend (10, 3.0)</p>' + metric_cards([
            ("atr", "ATR(10)"),
            ("pullback_distance", "Distance above Supertrend / ATR(10)"),
            ("pullback_limit", "Pullback threshold: 0.5 × ATR(10)"),
            ("stop_loss_preview", "Stop-loss preview: 1.5 × ATR(10)"),
            ("take_profit_preview", "Take-profit preview: 3.0 × ATR(10)"),
        ]))

    def inline_cards(self):
        # Real capital at risk: canary state and gate evidence never hide behind a toggle.
        if self.config.mode != "LIVE":
            return ''
        return (
            '<section class="bg-dark-800 border border-dark-700 rounded-xl p-4">'
            f'Canary: {"ON" if self.config.canary_mode else "OFF"} · Allocation cap: ${self.config.max_canary_allocation_usd}'
            '<details><summary>24 funded safety gates · local evidence check</summary>'
            '<pre id="safety-gates" class="whitespace-pre-wrap">Awaiting evidence</pre></details></section>'
        )

    def get_dashboard_payload(self, requested_timeframe=None):
        payload = super().get_dashboard_payload(requested_timeframe)
        if self.config.mode == "LIVE":
            from live_engine.risk.safety_gates import SafetyGateVerifier
            passed, gates = SafetyGateVerifier(self.config, self.base_dir).evaluate_all_gates()
            payload.update(safety_gates=[asdict(g) for g in gates], safety_gates_passed=passed,
                           canary_mode=self.config.canary_mode,
                           max_canary_allocation_usd=str(self.config.max_canary_allocation_usd))
        return payload


if __name__ == "__main__":
    run_dashboard_cli(BTCDashboard)
