"""Deterministic client order ID generator.

Format: ESC-{symbol}-{tf}-{candle_ts_sec}-{action}
Guarantees:
- Deterministic: Same candle boundary, symbol, timeframe and action always produce identical ID.
- Binance compatibility: Maximum 36 characters, alphanumeric plus hyphen/underscore.
"""
import hashlib
from typing import Optional
from live_engine.execution.models import SignalAction, OrderSide


def generate_client_order_id(
    symbol: str,
    timeframe: str,
    candle_timestamp_ms: int,
    action: str,
    prefix: str = "ESC",
) -> str:
    """Generates a deterministic client order ID <= 36 chars."""
    # Normalize inputs
    clean_sym = symbol.replace("/", "").replace(":", "").upper()
    clean_tf = timeframe.upper()
    ts_sec = int(candle_timestamp_ms // 1000)
    clean_act = action.upper().replace("ENTER_", "").replace("EXIT_", "")

    # Base candidate: e.g. ESC-BTCUSDT-15M-1723818600-BUY
    candidate = f"{prefix}-{clean_sym}-{clean_tf}-{ts_sec}-{clean_act}"
    if len(candidate) <= 36:
        return candidate

    # If symbol or format exceeds 36 chars, compress symbol or use deterministic short hash
    h = hashlib.sha256(f"{clean_sym}:{clean_tf}:{ts_sec}:{clean_act}".encode("utf-8")).hexdigest()[:10]
    return f"{prefix}-{ts_sec}-{clean_act}-{h}"[:36]
