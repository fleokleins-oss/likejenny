"""
risk_manager.py — paper wallet + risk controls.

This is intentionally conservative:
- paper-first (default)
- no naked shorting unless allow_short=True
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional

from .utils import clamp

log = logging.getLogger("risk")


@dataclass(slots=True)
class OpenOrder:
    order_id: str
    token_id: str
    side: str  # BUY or SELL
    price: float
    size: float
    created_ts: int


@dataclass
class PaperWallet:
    usdc: float
    allow_short: bool = False
    positions: Dict[str, float] = field(default_factory=dict)  # token_id -> shares
    open_orders: Dict[str, OpenOrder] = field(default_factory=dict)
    mark: Dict[str, float] = field(default_factory=dict)  # token_id -> mid mark
    peak_equity: float = 0.0

    def __post_init__(self):
        self.peak_equity = self.equity()

    def equity(self) -> float:
        eq = float(self.usdc)
        for tok, sh in self.positions.items():
            px = self.mark.get(tok, 0.0)
            eq += sh * px
        return eq

    def update_mark(self, token_id: str, mid: float) -> None:
        self.mark[token_id] = mid
        eq = self.equity()
        if eq > self.peak_equity:
            self.peak_equity = eq

    def drawdown_pct(self) -> float:
        eq = self.equity()
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, (self.peak_equity - eq) / self.peak_equity)

    def inventory_usdc(self, token_id: str) -> float:
        return float(self.positions.get(token_id, 0.0) * self.mark.get(token_id, 0.0))

    def record_open_order(self, o: OpenOrder) -> None:
        self.open_orders[o.order_id] = o

    def cancel_open_order(self, order_id: str) -> None:
        self.open_orders.pop(order_id, None)

    def can_place(self, token_id: str, side: str, price: float, size: float, max_usdc_per_order: float, max_inv_usdc: float) -> bool:
        notional = price * size
        if notional <= 0:
            return False
        if notional > max_usdc_per_order:
            return False

        inv = self.inventory_usdc(token_id)
        pos = self.positions.get(token_id, 0.0)

        if side.upper() == "BUY":
            if self.usdc < notional:
                return False
            # do not exceed max inventory
            if (inv + notional) > max_inv_usdc:
                return False
        else:
            # SELL
            if (not self.allow_short) and (pos < size - 1e-9):
                return False
            if (inv - notional) < -max_inv_usdc and not self.allow_short:
                return False
        return True

    def apply_fill(self, token_id: str, side: str, price: float, size: float, fee_bps: float = 0.0) -> None:
        """
        Apply a fill to paper wallet. Fee model: fee_bps applied to notional.
        """
        side = side.upper()
        notional = price * size
        fee = notional * (fee_bps / 10_000.0)
        if side == "BUY":
            self.usdc -= (notional + fee)
            self.positions[token_id] = self.positions.get(token_id, 0.0) + size
        else:
            self.usdc += (notional - fee)
            self.positions[token_id] = self.positions.get(token_id, 0.0) - size
        log.info("FILL %s %s size=%.4f px=%.4f fee=%.4f usdc=%.2f pos=%.4f",
                 token_id, side, size, price, fee, self.usdc, self.positions.get(token_id, 0.0))

    def reconcile_trade_event(self, trade: dict, my_owner: Optional[str] = None) -> None:
        """
        Update inventory from a user-channel trade event.
        The user WS includes maker_orders[] with order_id and matched_amount.
        """
        if trade.get("event_type") != "trade":
            return
        maker_orders = trade.get("maker_orders") or []
        for mo in maker_orders:
            # If my_owner provided, filter; otherwise accept all maker_orders (server already scoped to user).
            if my_owner and mo.get("owner") and mo.get("owner") != my_owner:
                continue
            token_id = str(mo.get("asset_id") or trade.get("asset_id") or "")
            side = str(mo.get("side") or trade.get("side") or "").upper()
            price = float(mo.get("price") or trade.get("price") or 0.0)
            size = float(mo.get("matched_amount") or 0.0)
            order_id = str(mo.get("order_id") or "")
            fee_bps = float(mo.get("fee_rate_bps") or trade.get("fee_rate_bps") or 0.0)

            if token_id and size > 0 and price > 0:
                self.apply_fill(token_id, side, price, size, fee_bps=fee_bps)
            if order_id:
                # reduce open order remaining size (best-effort)
                oo = self.open_orders.get(order_id)
                if oo:
                    remaining = max(0.0, oo.size - size)
                    if remaining <= 1e-9:
                        self.open_orders.pop(order_id, None)
                    else:
                        oo.size = remaining
