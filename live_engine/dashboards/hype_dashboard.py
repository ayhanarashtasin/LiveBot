"""HYPEUSDT LuxAlgo Rank 12 PAPER and SHADOW dashboard."""
import json
import time

import pandas as pd

from .base import BaseDashboard, metric_cards, read_connection, run_dashboard_cli


class HYPEDashboard(BaseDashboard):
    # Promoted to full production readiness alongside BTC: the manifest allows all four modes.
    allowed_modes = ("PAPER", "SHADOW", "TESTNET", "LIVE")
    symbol = "HYPEUSDT"
    timeframe = "5m"
    benchmark_id = "HYPE_LUXALGO_RANK12_5M"
    strategy_class = "HYPELuxAlgoRank12_5M"
    supertrend_defaults = (10, 2.0)
    config_prefix = "hype"
    chart_history = 1000

    def cards(self):
        return ('<p>LuxAlgo Rank 12 · 5m · 3× · 12 independent slots</p>' + metric_cards([
            ("open_slots", "Open logical slots / 12"),
            ("atr", "ATR(14)"),
            ("adx", "ADX(14)"),
            ("active_group", "Entry rule group"),
            ("stop_loss_preview", "Stop preview: 6 × ATR(14)"),
            ("take_profit_preview", "Target preview: 4.5 × ATR(14)"),
        ]))

    def get_dashboard_payload(self, requested_timeframe=None):
        payload = super().get_dashboard_payload(requested_timeframe)
        metrics = {"open_slots": 0}
        if self.db_path.is_file():
            try:
                with read_connection(self.db_path) as conn:
                    row = conn.execute(
                        "SELECT value_json FROM engine_state WHERE key=?",
                        (f"slot_book:{self.benchmark_id}",),
                    ).fetchone()
                    if row:
                        metrics["open_slots"] = len(json.loads(row[0]).get("slots", []))
                    rows = conn.execute(
                        "SELECT payload_json, timestamp FROM audit_events "
                        "WHERE event_type='STRATEGY_SLOT_CLOSED' ORDER BY event_id"
                    ).fetchall()
                    journal, wins, losses, gross_profit, gross_loss = [], 0, 0, 0.0, 0.0
                    peak, max_drawdown = 10000.0, 0.0
                    for row in rows:
                        trade = json.loads(row[0])
                        pnl = float(trade["net_pnl"])
                        wins += pnl >= 0
                        losses += pnl < 0
                        gross_profit += max(pnl, 0.0)
                        gross_loss += max(-pnl, 0.0)
                        equity = float(trade["realized_equity"])
                        peak = max(peak, equity)
                        max_drawdown = max(max_drawdown, (peak - equity) / peak * 100 if peak else 0.0)
                        journal.append({
                            "entry_time": time.strftime("%b %d %H:%M", time.gmtime(int(trade["entry_open_time"]) / 1000)),
                            "exit_time": time.strftime("%b %d %H:%M", time.gmtime(row[1] / 1000)),
                            "side": "LONG", "qty": trade["exit_quantity"],
                            "entry_price": trade["entry_price"], "exit_price": trade["exit_price"],
                            "pnl": round(pnl, 2), "hold_seconds": max(0, int((row[1] - int(trade["entry_open_time"])) / 1000)),
                        })
                    if journal:
                        payload.update(
                            trade_journal=list(reversed(journal)), win_count=wins, loss_count=losses,
                            completed_trades=wins + losses,
                            profit_factor=f"{gross_profit / gross_loss:.2f}" if gross_loss else "∞",
                            max_drawdown_pct=f"{max_drawdown:.2f}",
                        )
            except Exception:
                pass
        if payload["timeframe"] == self.timeframe and len(payload["candles"]) >= self.strategy.startup_candle_count:
            latest = self.strategy.populate_indicators(
                pd.DataFrame(payload["candles"]), {"pair": self.symbol}
            ).iloc[-1]
            if pd.notna(latest.atr):
                groups = [str(i) for i in (1, 2, 3) if bool(latest[f"entry_group_{i}"])]
                close, atr = float(latest.close), float(latest.atr)
                metrics.update(
                    atr=atr,
                    adx=float(latest.adx),
                    active_group=", ".join(groups) or "none",
                    stop_loss_preview=close - self.strategy.sl_multiplier * atr,
                    take_profit_preview=close + self.strategy.tp_multiplier * atr,
                )
                payload["timeframe_decision"] = f"ENTRY GROUP {'+'.join(groups)}" if groups else "WAIT"
        payload["strategy_metrics"] = metrics
        return payload


if __name__ == "__main__":
    run_dashboard_cli(HYPEDashboard)
