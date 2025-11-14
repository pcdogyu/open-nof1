"""
Indicator calculation utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


@dataclass(slots=True)
class IndicatorResult:
    timeframe: str
    macd: float | None
    macd_signal: float | None
    macd_hist: float | None
    rsi: float | None
    realized_volatility: float | None
    close_price: float | None = None


class IndicatorCalculator:
    """Compute technical indicators on OHLCV, trades, and order book snapshots."""

    def compute_candle_indicators(
        self, timeframe: str, candles: Iterable[List[str]]
    ) -> IndicatorResult:
        df = self._candles_to_df(candles)
        if df.empty:
            return IndicatorResult(timeframe, None, None, None, None, None, None)
        close = df["close"]
        latest_close = float(close.iloc[-1]) if not close.empty else None

        exp12 = close.ewm(span=12, adjust=False).mean()
        exp26 = close.ewm(span=26, adjust=False).mean()
        macd_val = exp12 - exp26
        signal = macd_val.ewm(span=9, adjust=False).mean()
        hist = macd_val - signal

        delta = close.diff()
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)
        avg_gain = pd.Series(gain).rolling(window=14).mean()
        avg_loss = pd.Series(loss).rolling(window=14).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        returns = close.pct_change().dropna()
        realized_vol = returns.std() * np.sqrt(len(returns)) if not returns.empty else None

        return IndicatorResult(
            timeframe=timeframe,
            macd=macd_val.iloc[-1] if not macd_val.empty else None,
            macd_signal=signal.iloc[-1] if not signal.empty else None,
            macd_hist=hist.iloc[-1] if not hist.empty else None,
            rsi=rsi.iloc[-1] if not rsi.empty else None,
            realized_volatility=realized_vol,
            close_price=latest_close,
        )

    @staticmethod
    def compute_cvd(trades: Iterable[dict]) -> float | None:
        """Calculate cumulative volume delta from recent trades."""
        cvd = 0.0
        count = 0
        for trade in trades:
            side = trade.get("side")
            size = float(trade.get("sz", trade.get("size", 0)))
            if side == "buy":
                cvd += size
            elif side == "sell":
                cvd -= size
            count += 1
        return cvd if count else None

    @staticmethod
    def compute_orderbook_imbalance(orderbook: Dict[str, List[List[str]]]) -> float | None:
        """Compute bid/ask imbalance from order book snapshot."""
        bids = orderbook.get("bids") or []
        asks = orderbook.get("asks") or []
        bid_vol = sum(float(b[1]) for b in bids)
        ask_vol = sum(float(a[1]) for a in asks)
        total = bid_vol + ask_vol
        if total == 0:
            return None
        return (bid_vol - ask_vol) / total

    @staticmethod
    def compute_volume_profile(candles: Iterable[List[str]]) -> Dict[str, float]:
        """Return basic volume statistics to provide more context for models."""
        df = IndicatorCalculator._candles_to_df(candles)
        if df.empty:
            return {}
        return {
            "volume_sum": float(df["volume"].sum()),
            "volume_avg": float(df["volume"].mean()),
            "volume_max": float(df["volume"].max()),
        }

    @staticmethod
    def _candles_to_df(candles: Iterable[List[str]]) -> pd.DataFrame:
        rows = []
        for candle in candles:
            if len(candle) < 6:
                continue
            ts, op, hi, lo, cl, vol = candle[:6]
            rows.append(
                {
                    "timestamp": pd.to_datetime(int(ts), unit="ms"),
                    "open": float(op),
                    "high": float(hi),
                    "low": float(lo),
                    "close": float(cl),
                    "volume": float(vol),
                }
            )
        df = pd.DataFrame(rows).sort_values("timestamp")
        return df
