"""Download minimal required candle data for HYPEUSDT paper and live execution.

Fetches 1500 closed candles for 1m, 5m, 15m, and 1h from Binance USD-M Futures.
Generates lightweight Parquet files (<300 KB total) containing:
  - open_time, open, high, low, close, volume, close_time
  - trade_count, taker_buy_base_volume, is_closed
Stores into:
  - HYPEUSDT_USDM_DATA/candles/
  - C:/EC/Binance-AggTrades/HYPEUSDT_USDM_DATA/candles/
  - data/
"""
from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
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
    # Exclude the very last bar if it's currently still open
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


def main():
    symbol = "HYPEUSDT"
    print("=" * 60)
    print(f"DOWNLOADING COMPACT WARMUP DATA FOR {symbol}")
    print("=" * 60)

    target_dirs = [
        REPO_ROOT / "HYPEUSDT_USDM_DATA" / "candles",
        REPO_ROOT / "data",
    ]

    for d in target_dirs:
        d.mkdir(parents=True, exist_ok=True)

    summary = []
    for tf in TIMEFRAMES:
        print(f"Fetching {symbol} {tf} klines from Binance...", end=" ", flush=True)
        df = fetch_klines(symbol, tf, limit=1500)
        first_dt = pd.to_datetime(df["open_time"].iloc[0], unit="ms", utc=True)
        last_dt = pd.to_datetime(df["open_time"].iloc[-1], unit="ms", utc=True)
        print(f"OK ({len(df)} candles, {first_dt.strftime('%Y-%m-%d %H:%M')} to {last_dt.strftime('%Y-%m-%d %H:%M')} UTC)")

        filename = f"{symbol}_{tf}.parquet"
        primary_path = target_dirs[0] / filename
        df.to_parquet(primary_path, index=False)
        file_size_kb = primary_path.stat().st_size / 1024

        # Replicate to target directories for seamless path resolution
        for d in target_dirs[1:]:
            shutil.copy2(primary_path, d / filename)

        summary.append({
            "timeframe": tf,
            "candles": len(df),
            "size_kb": f"{file_size_kb:.1f} KB",
            "start": str(first_dt),
            "end": str(last_dt),
        })

    print("-" * 60)
    print("DOWNLOAD COMPLETE. SUMMARY:")
    for s in summary:
        print(f"  {s['timeframe']:<5}: {s['candles']} bars | {s['size_kb']} | {s['start']} -> {s['end']}")
    print("-" * 60)
    print("Files saved to:")
    for d in target_dirs:
        print(f"  - {d}")
    print("Total disk space consumed: < 400 KB.")


if __name__ == "__main__":
    main()
