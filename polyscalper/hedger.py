"""
hedger.py — cross-market hedge layer (paper-first).

Implements a simple online beta hedge:
- Estimate beta between token mid returns and underlying spot returns (BTC/ETH/SOL)
- Compute desired hedge notional to offset inventory risk
- Maintain a paper hedge ledger (PnL tracked)

Notes:
- For prediction market tokens, "beta" is only a proxy and can be unstable.
- This is OFF by default (HEDGE_ENABLED=false).
"""
from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np

from .config import Settings
from .utils import clamp

log = logging.getLogger("hedge")


def _log_returns(arr: Sequence[float]) -> np.ndarray:
    a = np.asarray(arr, dtype=float)
    if a.size < 3:
        return np.array([], dtype=float)
    a = np.clip(a, 1e-9, np.inf)
    return np.diff(np.log(a))


@dataclass(slots=True)
class HedgePos:
    qty: float = 0.0   # in base asset (e.g., BTC)
    entry_px: float = 0.0


class PaperHedgeLedger:
    def __init__(self, usdc: float, slippage_bps: float = 5.0):
        self.usdc = float(usdc)
        self.slippage_bps = float(slippage_bps)
        self.pos: Dict[str, HedgePos] = {}
        self.mark: Dict[str, float] = {}
        self.realized: float = 0.0

    def update_mark(self, symbol: str, px: float) -> None:
        if px > 0:
            self.mark[symbol] = float(px)

    def equity(self) -> float:
        eq = float(self.usdc)
        for sym, p in self.pos.items():
            px = float(self.mark.get(sym, 0.0))
            eq += p.qty * px
        return eq

    def trade_to_target(self, symbol: str, target_qty: float, px: float, max_notional: float) -> None:
        """
        Paper market trade to adjust position to target (with simple slippage).
        """
        px = float(px)
        if px <= 0:
            return
        cur = float(self.pos.get(symbol, HedgePos()).qty)
        d = float(target_qty - cur)
        if abs(d) <= 1e-9:
            return

        notional = abs(d) * px
        if notional > max_notional:
            d = np.sign(d) * (max_notional / px)
            notional = abs(d) * px

        slip = self.slippage_bps / 10_000.0
        exec_px = px * (1.0 + slip) if d > 0 else px * (1.0 - slip)

        # BUY increases qty and uses cash; SELL decreases qty and adds cash
        self.usdc -= d * exec_px

        # update average entry only roughly (good enough for paper PnL)
        p = self.pos.get(symbol)
        if p is None:
            self.pos[symbol] = HedgePos(qty=d, entry_px=exec_px)
        else:
            new_qty = p.qty + d
            if abs(new_qty) < 1e-9:
                # flat: realize pnl vs entry
                self.realized += p.qty * (exec_px - p.entry_px)
                self.pos.pop(symbol, None)
            else:
                # weighted entry
                p.entry_px = (p.qty * p.entry_px + d * exec_px) / new_qty
                p.qty = new_qty

        log.info("HEDGE %s trade d=%.6f @ %.2f -> qty=%.6f eq=%.2f", symbol, d, exec_px, self.pos.get(symbol, HedgePos()).qty, self.equity())


class BetaHedger:
    def __init__(self, s: Settings, hedge_symbols: Sequence[str]):
        self.s = s
        self.hedge_symbols = list(hedge_symbols)
        self.token_mid_hist: Dict[str, Deque[float]] = {}
        # betas[token_id][sym] = beta
        self.betas: Dict[str, Dict[str, float]] = {}
        self._last_recalc: float = 0.0

    def update_token_mid(self, token_id: str, mid: float) -> None:
        dq = self.token_mid_hist.get(token_id)
        if dq is None:
            dq = deque(maxlen=int(self.s.HEDGE_WINDOW))
            self.token_mid_hist[token_id] = dq
        if mid > 0:
            dq.append(float(mid))

    def maybe_recalc_betas(self, spot_histories: Dict[str, List[float]]) -> None:
        now = time.time()
        if (now - self._last_recalc) < float(self.s.HEDGE_REBALANCE_SEC):
            return
        self._last_recalc = now

        for token_id, dq in self.token_mid_hist.items():
            mids = list(dq)
            rt = _log_returns(mids)
            if rt.size < 40:
                continue
            bmap: Dict[str, float] = {}
            for sym in self.hedge_symbols:
                sh = spot_histories.get(sym) or []
                rs = _log_returns(sh)
                n = min(rt.size, rs.size, int(self.s.HEDGE_WINDOW) - 2)
                if n < 40:
                    continue
                x = rs[-n:]
                y = rt[-n:]
                varx = float(np.var(x))
                if varx <= 1e-12:
                    continue
                beta = float(np.cov(x, y)[0, 1] / varx)
                bmap[sym] = beta
            if bmap:
                self.betas[token_id] = bmap

    def best_beta(self, token_id: str) -> Tuple[str, float]:
        bmap = self.betas.get(token_id) or {}
        if not bmap:
            return ("", 0.0)
        # choose hedge symbol with max |beta|
        sym = max(bmap.keys(), key=lambda k: abs(bmap[k]))
        return (sym, float(bmap[sym]))

    def target_hedges(
        self,
        *,
        token_inventories_usdc: Dict[str, float],
        spot_prices: Dict[str, float],
        spot_histories: Dict[str, List[float]],
    ) -> Dict[str, float]:
        """
        Aggregate desired hedge qty per hedge symbol.

        heuristic: hedge_notional ~ -beta * inventory_notional.
        qty = hedge_notional / spot_price.
        """
        self.maybe_recalc_betas(spot_histories)
        targets: Dict[str, float] = {sym: 0.0 for sym in self.hedge_symbols}
        for tid, inv_usdc in token_inventories_usdc.items():
            sym, beta = self.best_beta(tid)
            if not sym:
                continue
            px = float(spot_prices.get(sym, 0.0))
            if px <= 0:
                continue
            hedge_notional = -beta * float(inv_usdc)
            qty = hedge_notional / px
            targets[sym] += qty
        return targets
