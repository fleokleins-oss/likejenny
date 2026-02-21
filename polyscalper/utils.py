"""
utils.py — small helpers + econophysics signals used by strategy/MM.

Notes:
- These estimators are intentionally lightweight and robust for streaming.
- In paper mode they are for *regime gating*; in live mode validate thoroughly.
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

import numpy as np


# -----------------------------
# Logging / JSON
# -----------------------------
def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def safe_json_dumps(obj) -> str:
    """Compact JSON for HMAC signing (stable key order, no whitespace)."""
    return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def utc_ts() -> int:
    return int(time.time())


def ms_ts() -> int:
    return int(time.time() * 1000)


# -----------------------------
# Math helpers
# -----------------------------
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def round_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    return round(price / tick) * tick


def microprice(best_bid: float, bid_size: float, best_ask: float, ask_size: float) -> float:
    denom = (bid_size + ask_size)
    if denom <= 0:
        return (best_bid + best_ask) / 2.0
    return (best_bid * ask_size + best_ask * bid_size) / denom


def log_returns(prices: Sequence[float]) -> np.ndarray:
    p = np.asarray(prices, dtype=float)
    p = p[p > 0]
    if len(p) < 2:
        return np.array([], dtype=float)
    return np.diff(np.log(p))


# -----------------------------
# Econophysics signals
# -----------------------------
def hurst_rs(prices: Sequence[float], min_chunk: int = 8, max_chunk: int = 128) -> float:
    """
    Hurst exponent via R/S method (quick).
    Returns ~0.5 for random walk, >0.5 persistence, <0.5 mean-reversion.
    """
    x = np.asarray(prices, dtype=float)
    if len(x) < max(min_chunk * 4, 32):
        return 0.5

    r = log_returns(x)
    if len(r) < 32:
        return 0.5

    n = len(r)
    sizes = np.unique(np.clip(np.logspace(np.log10(min_chunk), np.log10(min(max_chunk, n//2)), 8).astype(int), 2, n))
    rs_vals = []
    for s in sizes:
        m = n // s
        if m < 2:
            continue
        rs = []
        for i in range(m):
            seg = r[i*s:(i+1)*s]
            seg = seg - seg.mean()
            z = np.cumsum(seg)
            R = z.max() - z.min()
            S = seg.std(ddof=1)
            if S > 1e-12:
                rs.append(R / S)
        if rs:
            rs_vals.append((s, float(np.mean(rs))))
    if len(rs_vals) < 3:
        return 0.5
    sizes, rs = zip(*rs_vals)
    log_s = np.log(np.asarray(sizes, dtype=float))
    log_rs = np.log(np.asarray(rs, dtype=float))
    slope = np.polyfit(log_s, log_rs, 1)[0]
    return float(clamp(slope, 0.0, 1.0))


def shannon_entropy_returns(prices: Sequence[float], bins: int = 12) -> float:
    """Entropy of binned log-returns (higher => noisier regime)."""
    r = log_returns(prices)
    if len(r) < 32:
        return 0.0
    hist, _ = np.histogram(r, bins=bins, density=True)
    p = hist / (hist.sum() + 1e-12)
    p = p[p > 0]
    return float(-np.sum(p * np.log(p + 1e-12)))


def hill_tail_index(returns: Sequence[float], k: int = 20) -> float:
    """
    Hill estimator for tail index (alpha). Lower alpha => fatter tails.
    Works on absolute returns; use as a *risk inflation* proxy.
    """
    x = np.sort(np.abs(np.asarray(returns, dtype=float)))
    x = x[x > 0]
    if len(x) < max(3*k, 60):
        return 3.0
    tail = x[-k:]
    xk = tail[0]
    if xk <= 0:
        return 3.0
    alpha = k / np.sum(np.log(tail / xk + 1e-12))
    return float(clamp(alpha, 0.5, 10.0))


def transfer_entropy_discrete(x: Sequence[float], y: Sequence[float], bins: int = 6) -> float:
    """
    Very small discrete transfer entropy TE(x->y) using 1-step histories.
    Use as a contagion proxy (higher => x helps predict y).
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    n = min(len(x), len(y))
    if n < 64:
        return 0.0
    x = x[-n:]
    y = y[-n:]

    # Bin by quantiles to be robust to scaling
    def qbin(a):
        qs = np.quantile(a, np.linspace(0, 1, bins+1))
        qs[0] -= 1e-9
        qs[-1] += 1e-9
        return np.digitize(a, qs[1:-1], right=True)

    xb = qbin(x)
    yb = qbin(y)
    # TE: I( y_t ; x_{t-1} | y_{t-1} )
    yt = yb[1:]
    y1 = yb[:-1]
    x1 = xb[:-1]

    # joint counts
    def counts(*arrs):
        key = np.stack(arrs, axis=1)
        # map rows to integers
        base = bins
        idx = key[:, 0].astype(int)
        for col in key[:, 1:].T:
            idx = idx * base + col.astype(int)
        c = np.bincount(idx, minlength=base ** key.shape[1]).astype(float)
        return c / (c.sum() + 1e-12)

    p_yt_y1_x1 = counts(yt, y1, x1)
    p_y1_x1 = counts(y1, x1)
    p_yt_y1 = counts(yt, y1)
    p_y1 = counts(y1)

    te = 0.0
    # iterate over all states (small)
    for yt_i in range(bins):
        for y1_i in range(bins):
            for x1_i in range(bins):
                idx3 = (yt_i * bins + y1_i) * bins + x1_i
                p3 = p_yt_y1_x1[idx3]
                if p3 <= 0:
                    continue
                idx2a = y1_i * bins + x1_i
                p_y1x1 = p_y1_x1[idx2a]
                idx2b = yt_i * bins + y1_i
                p_yty1 = p_yt_y1[idx2b]
                p_y1v = p_y1[y1_i]
                # p(yt|y1,x1) / p(yt|y1)
                num = p3 / (p_y1x1 + 1e-12)
                den = p_yty1 / (p_y1v + 1e-12)
                te += p3 * math.log((num + 1e-12) / (den + 1e-12))
    return float(max(0.0, te))


def lyapunov_rosenstein(series: Sequence[float], m: int = 3, tau: int = 1, max_t: int = 20) -> float:
    """
    Largest Lyapunov exponent (Rosenstein-style) on 1D series.
    Negative/near 0 => stable; positive => chaotic divergence.
    """
    x = np.asarray(series, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 200:
        return 0.0

    # embed
    N = len(x) - (m - 1) * tau
    if N < 100:
        return 0.0
    X = np.zeros((N, m))
    for i in range(m):
        X[:, i] = x[i*tau:i*tau+N]

    # nearest neighbor excluding temporal neighbors
    dists = np.linalg.norm(X[:, None, :] - X[None, :, :], axis=2)
    np.fill_diagonal(dists, np.inf)
    # Theiler window to avoid trivial neighbors
    theiler = 10
    for i in range(N):
        lo = max(0, i - theiler)
        hi = min(N, i + theiler + 1)
        dists[i, lo:hi] = np.inf

    nn = np.argmin(dists, axis=1)
    # track divergence
    max_t = min(max_t, N - 1)
    div = []
    for t in range(1, max_t + 1):
        valid = (np.arange(N - t) >= 0)
        i_idx = np.arange(N - t)
        j_idx = nn[:N - t]
        # ensure neighbor also has future
        ok = (j_idx + t) < N
        i_idx = i_idx[ok]
        j_idx = j_idx[ok]
        if len(i_idx) < 30:
            continue
        dt = np.linalg.norm(X[i_idx + t] - X[j_idx + t], axis=1)
        d0 = np.linalg.norm(X[i_idx] - X[j_idx], axis=1)
        ratio = dt / (d0 + 1e-12)
        div.append(np.log(ratio + 1e-12).mean())
    if len(div) < 5:
        return 0.0
    t_axis = np.arange(1, len(div) + 1, dtype=float)
    slope = np.polyfit(t_axis, np.asarray(div), 1)[0]
    return float(slope)


# -----------------------------
# Streaming stats
# -----------------------------
@dataclass(slots=True)
class EMA:
    alpha: float
    value: float = 0.0
    initialized: bool = False

    def update(self, x: float) -> float:
        if not self.initialized:
            self.value = x
            self.initialized = True
            return self.value
        self.value = self.alpha * x + (1.0 - self.alpha) * self.value
        return self.value
