from __future__ import annotations

import time
from pathlib import Path
import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SYMBOLS = ["BTCUSDT", "LITUSDT", "ZECUSDT"]
TIMEFRAMES = ["1m", "5m", "15m", "1h"]
BASE_URL = "https://testnet.binancefuture.com/fapi/v1/klines"

def fetch_klines(symbol: str, interval: str, limit: int = 1500) -> pd.DataFrame:
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    for attempt in range(1, 4):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as exc:
            if attempt == 3:
                raise RuntimeError(f"Failed to fetch {symbol} {interval} klines after 3 attempts: {exc}")
            time.sleep(1.0 * attempt)

    rows = []
    now_ms = int(time.time() * 1000)
    for r in data:
        close_time = int(r[6])
        if close_time > now_ms:
            continue
        rows.append({
            "open_time": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
            "close_time": close_time,
            "trade_count": int(r[8]),
            "taker_buy_base_volume": float(r[9]),
            "is_closed": 1,
        })
    df = pd.DataFrame(rows).sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    return df

for symbol in SYMBOLS:
    sym_dir = REPO_ROOT / f"{symbol}_USDM_DATA" / "candles"
    sym_dir.mkdir(parents=True, exist_ok=True)
    for tf in TIMEFRAMES:
        df = fetch_klines(symbol, tf)
        p = sym_dir / f"{symbol}_{tf}.parquet"
        df.to_parquet(p, index=False)
        print(f"Saved {symbol} {tf}: {len(df)} bars to {p}")

print("All symbol warmup data successfully downloaded!")
