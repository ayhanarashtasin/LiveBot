"""Historical Binance aggTrades Parquet adapter.

Maps archived Binance Vision aggregate trade parquet files into normalized
AggTrade models for canonical candle construction and replay parity.
"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Iterator, List, Optional, Sequence
import pyarrow.parquet as pq

from live_engine.market_data.models import AggTrade, Candle
from live_engine.market_data.candle_builder import MinuteCandleBuilder
from live_engine.market_data.resampler import TimeframeResampler


class HistoricalAggTradeAdapter:
    """Streams and adapts historical Binance USD-M Futures aggTrades from Parquet files."""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol.upper()

    def stream_trades(
        self,
        parquet_path: str | Path,
        start_ts_ms: Optional[int] = None,
        end_ts_ms: Optional[int] = None,
        batch_size: int = 50000,
    ) -> Iterator[AggTrade]:
        """Reads Parquet file and yields strongly-typed AggTrade objects in sequence."""
        path = Path(parquet_path)
        if not path.exists():
            raise FileNotFoundError(f"AggTrades parquet not found: {path}")

        table_file = pq.ParquetFile(str(path))

        # Check required columns
        schema_names = table_file.schema.names
        # Handle field name variations between vision exports and internal formats
        id_col = "aggregate_trade_id" if "aggregate_trade_id" in schema_names else "agg_trade_id"
        time_col = "timestamp" if "timestamp" in schema_names else "trade_time"
        maker_col = "buyer_maker" if "buyer_maker" in schema_names else "buyer_is_market_maker"

        # Pushdown row group elimination using Parquet statistics
        row_groups = None
        if (start_ts_ms is not None or end_ts_ms is not None) and time_col in schema_names:
            time_col_idx = schema_names.index(time_col)
            row_groups = []
            for rg_idx in range(table_file.num_row_groups):
                rg = table_file.metadata.row_group(rg_idx)
                stats = rg.column(time_col_idx).statistics
                if stats is not None:
                    if start_ts_ms is not None and stats.max < start_ts_ms:
                        continue
                    if end_ts_ms is not None and stats.min >= end_ts_ms:
                        continue
                row_groups.append(rg_idx)

        for record_batch in table_file.iter_batches(batch_size=batch_size, row_groups=row_groups):
            pydict = record_batch.to_pydict()
            ids = pydict[id_col]
            prices = pydict["price"]
            quantities = pydict["quantity"]
            first_ids = pydict.get("first_trade_id", [0] * len(ids))
            last_ids = pydict.get("last_trade_id", [0] * len(ids))
            times = pydict[time_col]
            makers = pydict.get(maker_col, [False] * len(ids))

            n = len(ids)
            for i in range(n):
                t_ms = int(times[i])
                if start_ts_ms is not None and t_ms < start_ts_ms:
                    continue
                if end_ts_ms is not None and t_ms >= end_ts_ms:
                    return

                yield AggTrade(
                    event_type="aggTrade",
                    event_time=t_ms,
                    symbol=self.symbol,
                    agg_trade_id=int(ids[i]),
                    price=Decimal(str(prices[i])),
                    quantity=Decimal(str(quantities[i])),
                    first_trade_id=int(first_ids[i]),
                    last_trade_id=int(last_ids[i]),
                    trade_time=t_ms,
                    buyer_is_market_maker=bool(makers[i]),
                    received_at=t_ms,
                )

    def _stream_many(self, paths, start_ts_ms, end_ts_ms) -> Iterator[AggTrade]:
        """Streams several monthly archives as one continuous chronological sequence."""
        for path in paths:
            yield from self.stream_trades(path, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms)

    def build_candles_from_aggtrades(
        self,
        parquet_path: str | Path | Sequence[str | Path],
        target_timeframe: str = "15m",
        start_ts_ms: Optional[int] = None,
        end_ts_ms: Optional[int] = None,
    ) -> List[Candle]:
        """Constructs canonical higher-timeframe candles from one or more raw files.

        Multi-month evaluation windows need every monthly archive they touch; the files
        are streamed in chronological order through one shared builder so bucket state
        carries across a month boundary instead of restarting.
        """
        paths = [parquet_path] if isinstance(parquet_path, (str, Path)) else list(parquet_path)
        paths = sorted(paths, key=lambda p: Path(p).stem)

        builder = MinuteCandleBuilder(symbol=self.symbol)
        resampler = TimeframeResampler(symbol=self.symbol, target_timeframe=target_timeframe)
        candles: List[Candle] = []

        for trade in self._stream_many(paths, start_ts_ms, end_ts_ms):
            c1m = builder.add_trade(trade)
            if c1m is not None:
                c_htf = resampler.add_1m_candle(c1m)
                if c_htf is not None:
                    candles.append(c_htf)

        # Flush any remaining completed candle at end of stream
        last_1m = builder.flush()
        if last_1m is not None:
            c_htf = resampler.add_1m_candle(last_1m)
            if c_htf is not None:
                candles.append(c_htf)
        last_htf = resampler.finalize_current_candle()
        if last_htf is not None:
            candles.append(last_htf)

        return candles
