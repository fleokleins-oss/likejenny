"""
strategy.py — quoting logic.

This is the "true OpenClaw" piece:
- dynamic spread (adverse selection + latency aware)
- quote shading (inventory + short-horizon drift)
- econophysics regime gate (optional)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .config import Settings
from .utils import EMA, clamp, hurst_rs, shannon_entropy_returns, lyapunov_rosenstein, hill_tail_index, transfer_entropy_discrete, microprice

log = logging.getLogger("strategy")


@dataclass(slots=True)
class Regime:
    hurst: float = 0.5
    entropy: float = 0.0
    lle: float = 0.0
    tail_alpha: float = 3.0
    te: float = 0.0  # contagion proxy
    ok: bool = True


@dataclass(slots=True)
class Quote:
    token_id: str
    side: str  # BUY/SELL
    price: float
    size: float
    post_only: bool = True


class MarketMakerStrategy:
    def __init__(self, s: Settings):
        self.s = s
        self.lat_ema: Dict[str, EMA] = {}

        # order flow proxy from last_trade_price events: token_id -> list[(ts_ms, signed_size)]
        self.flow: Dict[str, List[Tuple[int, float]]] = {}

    def update_latency(self, token_id: str, latency_ms: float) -> float:
        ema = self.lat_ema.get(token_id)
        if ema is None:
            ema = EMA(self.s.MM_LAT_EMA_ALPHA)
            self.lat_ema[token_id] = ema
        return ema.update(latency_ms)

    def update_flow(self, token_id: str, ts_ms: int, side: str, size: float) -> None:
        signed = float(size) * (1.0 if side.upper() == "BUY" else -1.0)
        arr = self.flow.setdefault(token_id, [])
        arr.append((ts_ms, signed))
        # prune 30s
        cutoff = ts_ms - 30_000
        while arr and arr[0][0] < cutoff:
            arr.pop(0)

    def flow_score(self, token_id: str) -> float:
        arr = self.flow.get(token_id) or []
        if len(arr) < 3:
            return 0.0
        net = sum(x for _, x in arr)
        tot = sum(abs(x) for _, x in arr) + 1e-9
        return float(clamp(net / tot, -1.0, 1.0))

    def compute_regime(self, spot_prices: Sequence[float], peer_prices: Optional[Sequence[float]] = None) -> Regime:
        if not self.s.USE_ECONOPHYSICS:
            return Regime(ok=True)

        prices = list(spot_prices)
        if len(prices) < 220:
            return Regime(ok=True)  # don't block early

        h = hurst_rs(prices)
        ent = shannon_entropy_returns(prices)
        lle = lyapunov_rosenstein(prices)
        r = np.diff(np.log(np.asarray(prices)))
        alpha = hill_tail_index(r, k=min(30, max(20, len(r)//10)))
        te = 0.0
        if peer_prices is not None and len(peer_prices) >= len(prices):
            # Use returns for TE
            te = transfer_entropy_discrete(np.diff(np.log(np.asarray(peer_prices[-len(prices):]))), r)

        ok = (h >= self.s.HURST_MIN) and (ent <= self.s.ENTROPY_MAX) and (lle <= self.s.LLE_MAX) and (te <= self.s.TE_MAX)
        return Regime(h, ent, lle, alpha, te, ok)

    def dynamic_spread_bps(self, token_id: str, mid: float, best_bid: float, bid_sz: float, best_ask: float, ask_sz: float,
                           tick: float, latency_ms: float, spot_returns: Sequence[float]) -> float:
        s = self.s
        base = s.MM_BASE_SPREAD_BPS

        # volatility proxy (short horizon)
        vol = float(np.std(spot_returns[-30:]) if len(spot_returns) >= 10 else 0.0)
        vol = clamp(vol * 10000.0, 0.0, 200.0)  # scale-ish

        # microprice divergence
        mp = microprice(best_bid, bid_sz, best_ask, ask_sz)
        micro = abs(mp - mid) / max(tick, 1e-6)
        micro = clamp(micro / 5.0, 0.0, 2.0)

        lat = clamp(latency_ms / 250.0, 0.0, 2.0)  # 250ms -> 1.0

        flow = abs(self.flow_score(token_id))  # 0..1

        mult = 1.0 + s.MM_AS_K_LAT * lat + s.MM_AS_K_VOL * (vol / 50.0) + s.MM_AS_K_MICRO * micro + s.MM_AS_K_FLOW * flow
        spread = base * mult
        return float(clamp(spread, s.MM_MIN_SPREAD_BPS, s.MM_MAX_SPREAD_BPS))

    def quote_center(self, mid: float, token_inv_usdc: float, max_inv_usdc: float, drift: float, latency_ms: float) -> float:
        """
        Center price = mid + (inventory skew) + (latency-aware drift shading).
        Drift is log-return per step (small).
        """
        inv_frac = clamp(token_inv_usdc / max(max_inv_usdc, 1e-9), -1.0, 1.0)
        inv_shift = -self.s.MM_INV_SKEW_K * inv_frac * mid  # long => shift down to sell

        lat_s = latency_ms / 1000.0
        # If drift positive (uptrend), shift up slightly with latency (avoid selling too cheap)
        shade = self.s.MM_LAT_SHADE_K * drift * lat_s * mid * 100.0  # scale: drift ~ 1e-4
        return mid + inv_shift + shade

    def build_quotes(self, token_id: str, mid: float, best_bid: float, bid_sz: float, best_ask: float, ask_sz: float,
                     tick: float, latency_ms: float, token_inv_usdc: float, spot_returns: Sequence[float]) -> List[Quote]:
        # drift estimate: mean of recent returns
        drift = float(np.mean(spot_returns[-20:]) if len(spot_returns) >= 5 else 0.0)

        spread_bps = self.dynamic_spread_bps(token_id, mid, best_bid, bid_sz, best_ask, ask_sz, tick, latency_ms, spot_returns)
        half = (spread_bps / 10_000.0) / 2.0

        center = self.quote_center(mid, token_inv_usdc, self.s.MM_MAX_INV_USDC, drift, latency_ms)
        bid = center * (1.0 - half)
        ask = center * (1.0 + half)

        # Respect book: don't cross with post-only
        if self.s.MM_POST_ONLY:
            bid = min(bid, best_ask - tick)
            ask = max(ask, best_bid + tick)

        bid = float(np.round(bid / tick) * tick)
        ask = float(np.round(ask / tick) * tick)
        bid = clamp(bid, tick, 1.0 - tick)
        ask = clamp(ask, tick, 1.0 - tick)

        # size in shares (target notional / price)
        target_usdc = self.s.MM_TARGET_DEPTH_USDC
        bid_sz_sh = target_usdc / max(bid, tick)
        ask_sz_sh = target_usdc / max(ask, tick)

        # cap per order notional
        bid_sz_sh = min(bid_sz_sh, self.s.MM_MAX_USDC_PER_ORDER / max(bid, tick))
        ask_sz_sh = min(ask_sz_sh, self.s.MM_MAX_USDC_PER_ORDER / max(ask, tick))

        return [
            Quote(token_id, "BUY", bid, bid_sz_sh, post_only=self.s.MM_POST_ONLY),
            Quote(token_id, "SELL", ask, ask_sz_sh, post_only=self.s.MM_POST_ONLY),
        ]
