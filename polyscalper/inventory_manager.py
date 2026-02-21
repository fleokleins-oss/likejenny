"""
inventory_manager.py — "OpenClaw-style" inventory manager layer.

Goals:
- Keep paper-first safety.
- Auto-neutralize exposure between YES/NO tokens (same condition) by skewing quotes.
- Contagion-aware per-token risk limits using transfer entropy (TE) across token mid-price returns.
- Provide simple "unwind-only" behavior when inventory breaches hard bands.

This module does NOT execute trades; it only:
- Tracks rolling mid series per token
- Computes contagion indices
- Outputs quote adjustments and dynamic max inventory caps
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from .config import Settings
from .utils import clamp, transfer_entropy_discrete


@dataclass(slots=True)
class Pair:
    pair_id: str
    yes: str
    no: str


@dataclass(slots=True)
class TokenPolicy:
    max_inv_usdc: float
    # if True, only quote the side that reduces inventory
    unwind_only: bool = False
    # size multipliers applied to BUY and SELL quotes
    buy_mult: float = 1.0
    sell_mult: float = 1.0


class InventoryManager:
    def __init__(self, s: Settings, pairs: Optional[Sequence[Pair]] = None):
        self.s = s
        self.pairs: Dict[str, Pair] = {p.pair_id: p for p in (pairs or [])}
        self.token_to_pair: Dict[str, str] = {}
        for p in (pairs or []):
            self.token_to_pair[p.yes] = p.pair_id
            self.token_to_pair[p.no] = p.pair_id

        self.mid_hist: Dict[str, Deque[float]] = {}
        self.contagion: Dict[str, float] = {}
        self._last_recalc: float = 0.0

    # ---------------- Rolling series ----------------

    def update_mid(self, token_id: str, mid: float) -> None:
        if mid <= 0:
            return
        dq = self.mid_hist.get(token_id)
        if dq is None:
            dq = deque(maxlen=int(self.s.INV_TE_WINDOW))
            self.mid_hist[token_id] = dq
        dq.append(float(mid))

    def _returns(self, token_id: str) -> Optional[np.ndarray]:
        dq = self.mid_hist.get(token_id)
        if not dq or len(dq) < max(60, int(self.s.INV_TE_WINDOW) // 3):
            return None
        arr = np.asarray(dq, dtype=float)
        arr = np.clip(arr, 1e-6, 1.0)
        r = np.diff(np.log(arr))
        if r.size < 40:
            return None
        return r

    # ---------------- Contagion (TE) ----------------

    def maybe_recalc_contagion(self) -> None:
        if not self.s.INV_TE_ENABLED:
            return
        now = time.time()
        if (now - self._last_recalc) < float(self.s.INV_TE_RECALC_SEC):
            return
        self._last_recalc = now

        tokens = list(self.mid_hist.keys())
        if len(tokens) < 2:
            return

        # compute TE matrix sparsely (cap pairs for speed)
        # For each token i, contagion = mean TE from others -> i (returns_j -> returns_i)
        out: Dict[str, float] = {}
        for i, ti in enumerate(tokens):
            ri = self._returns(ti)
            if ri is None:
                continue
            tes: List[float] = []
            for tj in tokens:
                if tj == ti:
                    continue
                rj = self._returns(tj)
                if rj is None:
                    continue
                # align lengths
                n = min(len(ri), len(rj), 180)
                if n < 60:
                    continue
                te = float(transfer_entropy_discrete(rj[-n:], ri[-n:]))
                tes.append(te)
                if len(tes) >= 6:  # cap comparisons
                    break
            out[ti] = float(np.mean(tes)) if tes else 0.0

        self.contagion.update(out)

    def contagion_index(self, token_id: str) -> float:
        return float(self.contagion.get(token_id, 0.0))

    # ---------------- Policies ----------------

    def dynamic_max_inv_usdc(self, token_id: str, base_max_inv: float) -> float:
        """
        Reduce max inventory when contagion rises (transfer entropy).
        """
        base_max_inv = float(base_max_inv)
        if (not self.s.INV_TE_ENABLED) or base_max_inv <= 0:
            return base_max_inv

        te = self.contagion_index(token_id)
        # Normalize TE by TE_MAX (regime threshold)
        norm = te / max(self.s.TE_MAX, 1e-9)
        # risk multiplier decreases with contagion
        mult = 1.0 / (1.0 + self.s.INV_TE_RISK_K * max(0.0, norm))
        mult = float(clamp(mult, self.s.INV_TE_MIN_MULT, 1.0))
        return float(base_max_inv * mult)

    def pair_exposure_shares(self, pair_id: str, positions: Dict[str, float]) -> float:
        p = self.pairs.get(pair_id)
        if not p:
            return 0.0
        yes_sh = float(positions.get(p.yes, 0.0))
        no_sh = float(positions.get(p.no, 0.0))
        # event-exposure proxy: YES_shares - NO_shares
        return yes_sh - no_sh

    def policy_for_token(self, token_id: str, *, positions: Dict[str, float], inv_usdc: float, base_max_inv_usdc: float) -> TokenPolicy:
        max_inv = self.dynamic_max_inv_usdc(token_id, base_max_inv_usdc)
        max_inv = max(1e-9, float(max_inv))

        # soft/hard bands
        soft = float(self.s.INV_SOFT_BAND_USDC) * max_inv
        hard = float(self.s.INV_HARD_BAND_USDC) * max_inv

        buy_mult = 1.0
        sell_mult = 1.0
        unwind_only = False

        if abs(inv_usdc) >= hard:
            unwind_only = True
            if inv_usdc > 0:
                # long => sell to unwind
                buy_mult = 0.0
                sell_mult = float(self.s.INV_UNWIND_ONLY_SIZE_MULT)
            else:
                sell_mult = 0.0
                buy_mult = float(self.s.INV_UNWIND_ONLY_SIZE_MULT)
        elif abs(inv_usdc) >= soft:
            # gentle skew: reduce the side that increases inventory
            frac = float(clamp(abs(inv_usdc) / max_inv, 0.0, 1.0))
            k = 0.6 * frac
            if inv_usdc > 0:
                buy_mult *= (1.0 - k)         # don't add long
                sell_mult *= (1.0 + 0.5 * k)  # encourage sell
            else:
                sell_mult *= (1.0 - k)
                buy_mult *= (1.0 + 0.5 * k)

        # YES/NO exposure neutralization
        if self.s.INV_PAIR_ENABLED:
            pid = self.token_to_pair.get(token_id)
            if pid:
                exp = self.pair_exposure_shares(pid, positions) - float(self.s.INV_PAIR_TARGET_EXPOSURE_SHARES)
                band = max(1e-9, float(self.s.INV_PAIR_EXPOSURE_BAND_SHARES))
                skew = float(clamp(exp / band, -2.0, 2.0))  # -2..2
                p = self.pairs[pid]
                # If exp>0 => too long YES vs NO. For YES token: encourage selling; for NO token: encourage buying.
                sgn = 1.0 if token_id == p.yes else -1.0
                k = float(self.s.INV_PAIR_SKEW_K)
                buy_mult *= float(clamp(1.0 - sgn * k * skew, 0.0, 2.0))
                sell_mult *= float(clamp(1.0 + sgn * k * skew, 0.0, 2.0))

        return TokenPolicy(max_inv_usdc=max_inv, unwind_only=unwind_only, buy_mult=buy_mult, sell_mult=sell_mult)

    def apply_policy_to_quotes(self, quotes: Sequence, policy: TokenPolicy) -> List:
        """
        Apply size multipliers and optionally drop sides.
        `quotes` is a list of strategy.Quote objects (duck-typed).
        """
        out = []
        for q in quotes:
            side = str(q.side).upper()
            mult = policy.buy_mult if side == "BUY" else policy.sell_mult
            size = float(q.size) * float(mult)
            if size <= 0:
                continue
            q.size = size
            out.append(q)
        return out
