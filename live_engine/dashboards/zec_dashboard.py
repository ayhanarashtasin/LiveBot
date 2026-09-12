"""ZECUSDT Momentum M03 EMA Cross (10, 100) PAPER and SHADOW dashboard."""
import pandas as pd

from .base import BaseDashboard, metric_cards, MODE_LABELS, run_dashboard_cli


class ZECDashboard(BaseDashboard):
    symbol = "ZECUSDT"
    timeframe = "15m"
    benchmark_id = "ZEC_MOMENTUM_M03_15M"
    strategy_class = "ZECMomentumEMA15M"
    supertrend_defaults = (10, 100.0)

    def cards(self):
        return ('<p>ZECMomentumEMA15M · 15m · EMA Cross (10, 100)</p>' + metric_cards([
            ("ema_fast", "Fast EMA(10)"),
            ("ema_slow", "Slow EMA(100)"),
            ("crossover", "EMA Crossover Status"),
            ("price_filter", "Price Confirmation"),
            ("regime", "Momentum Trend Regime"),
        ]) + '<p class="text-xs text-gray-400">Institutional trend-following momentum strategy with price confirmation filter and full reversal exit.</p>')

    def get_dashboard_payload(self, requested_timeframe=None):
        payload = super().get_dashboard_payload(requested_timeframe)
        payload["benchmark_id"] = self.benchmark_id
        payload["mode_badge"], payload["disclaimer"], _ = MODE_LABELS[self.config.mode]
        payload["strategy_metrics"] = {}
        if payload["timeframe"] == self.timeframe and len(payload["candles"]) >= 10:
            frame = self.strategy.populate_indicators(pd.DataFrame(payload["candles"]), {"pair": self.symbol})
            latest = frame.iloc[-1]
            ema_fast = latest.get("ema_fast")
            ema_slow = latest.get("ema_slow")
            if pd.notna(ema_fast) and pd.notna(ema_slow):
                close = float(latest["close"])
                is_bull = float(ema_fast) > float(ema_slow)
                price_filter_ok = close > float(ema_fast)
                metrics = {
                    "ema_fast": round(float(ema_fast), 4),
                    "ema_slow": round(float(ema_slow), 4),
                    "crossover": "BULLISH (Fast > Slow)" if is_bull else "BEARISH (Fast < Slow)",
                    "price_filter": "CONFIRMED (Close > EMA10)" if price_filter_ok else "UNCONFIRMED",
                    "regime": "BULLISH EXPANSION" if (is_bull and price_filter_ok) else ("BEARISH CONTRACTION" if not is_bull else "CONSOLIDATION"),
                }
                payload["strategy_metrics"] = metrics
                payload["timeframe_decision"] = "BULLISH MOMENTUM" if (is_bull and price_filter_ok) else ("BEARISH REVERSAL" if not is_bull else "WAIT MOMENTUM")
        return payload


if __name__ == "__main__":
    run_dashboard_cli(ZECDashboard)
