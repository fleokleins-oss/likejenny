"""
microstructure.py — OpenClaw-style maker loop.

Implements the incremental upgrades:
1) Batch cancel/replace (reduces API load)
2) User-channel fill reconciliation (inventory updated from real fills)
3) Queue position heuristics from full-depth book deltas (approx)
4) Latency-aware quote shading + dynamic spread (in strategy.py)

Safety:
- Paper-first by default (LIVE requires both env LIVE_MODE and --live flag)
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from .config import Settings
from .exchange import (
    BookTracker,
    HttpClient,
    PolyCreds,
    PolymarketCLOBPublic,
    PolymarketCLOBTrader,
    PolymarketMarketWS,
    PolymarketUserWS,
)
from .risk_manager import OpenOrder, PaperWallet
from .strategy import MarketMakerStrategy, Quote
from .inventory_manager import InventoryManager, Pair
from .hedger import BetaHedger, PaperHedgeLedger
from .spot_feed import BinanceSpotPoller
from .utils import clamp, ms_ts, utc_ts

log = logging.getLogger("micro")


@dataclass(slots=True)
class OrderKey:
    token_id: str
    side: str  # BUY/SELL


@dataclass(slots=True)
class QueueState:
    token_id: str
    side: str
    price: float
    start_level_size: float
    remaining_ahead: float
    created_ms: int
    last_progress_ms: int


class QueueEstimator:
    """
    Approximates queue position using level-size deltas at our price level.
    - On placement, snapshot current level size (ahead of us).
    - On subsequent price_change/book updates, if level size shrinks at that price, assume queue ahead gets consumed.
    """
    def __init__(self):
        self.by_order_id: Dict[str, QueueState] = {}

    def on_place(self, order_id: str, token_id: str, side: str, price: float, level_size_before: float) -> None:
        now = ms_ts()
        self.by_order_id[order_id] = QueueState(
            token_id=token_id,
            side=side.upper(),
            price=price,
            start_level_size=float(level_size_before),
            remaining_ahead=float(level_size_before),
            created_ms=now,
            last_progress_ms=now,
        )

    def on_level_update(self, token_id: str, side: str, price: float, new_size: float, old_size: float) -> None:
        if new_size >= old_size:  # queue ahead not reduced
            return
        delta = old_size - new_size
        now = ms_ts()
        for q in list(self.by_order_id.values()):
            if q.token_id == token_id and q.side == side.upper() and abs(q.price - price) < 1e-9:
                q.remaining_ahead = max(0.0, q.remaining_ahead - delta)
                q.last_progress_ms = now

    def progress_frac(self, order_id: str) -> float:
        q = self.by_order_id.get(order_id)
        if not q:
            return 1.0
        if q.start_level_size <= 1e-9:
            return 1.0
        cleared = q.start_level_size - q.remaining_ahead
        return float(clamp(cleared / q.start_level_size, 0.0, 1.0))

    def age_sec(self, order_id: str) -> float:
        q = self.by_order_id.get(order_id)
        if not q:
            return 0.0
        return max(0.0, (ms_ts() - q.created_ms) / 1000.0)

    def stale_sec(self, order_id: str) -> float:
        q = self.by_order_id.get(order_id)
        if not q:
            return 0.0
        return max(0.0, (ms_ts() - q.last_progress_ms) / 1000.0)

    def drop(self, order_id: str) -> None:
        self.by_order_id.pop(order_id, None)


class MarketMakerEngine:
    def __init__(self, s: Settings, token_ids: Sequence[str], market_ids: Optional[Dict[str, str]] = None, tick_sizes: Optional[Dict[str, float]] = None, pairs: Optional[Sequence[Pair]] = None):
        self.s = s
        self.token_ids = list(token_ids)
        self.market_ids = market_ids or {}  # token_id -> condition_id (for user WS filter)
        self.tick_sizes = tick_sizes or {t: 0.01 for t in self.token_ids}
        self.books: Dict[str, BookTracker] = {t: BookTracker() for t in self.token_ids}

        self.wallet = PaperWallet(usdc=s.PAPER_START_USDC)
        self.strategy = MarketMakerStrategy(s)
        self.queue = QueueEstimator()

        # Inventory / contagion / pairing
        self.invman = InventoryManager(s, pairs=pairs)
        self.max_inv_by_token: Dict[str, float] = {}

        # Cross-hedge (paper-first)
        self.hedge_symbols = [x.strip() for x in (s.HEDGE_SYMBOLS or '').split(',') if x.strip()]
        self.hedger = BetaHedger(s, self.hedge_symbols) if self.hedge_symbols else None
        self.hedge_ledger = PaperHedgeLedger(usdc=0.0, slippage_bps=s.HEDGE_SLIPPAGE_BPS)
        self.spot_poller: Optional[BinanceSpotPoller] = None

        self._cooldown_until: Dict[OrderKey, float] = {}

        # Spot history (Binance poller; replace with ccxt.pro WS for low latency)
        self.spot_hist: Dict[str, List[float]] = {}
        self.peer_symbol = "BTCUSDT"

        # Live (optional)
        self.http = HttpClient(timeout=10.0)
        self.public: Optional[PolymarketCLOBPublic] = None
        self.trader: Optional[PolymarketCLOBTrader] = None
        self.market_ws: Optional[PolymarketMarketWS] = None
        self.user_ws: Optional[PolymarketUserWS] = None
        self._heartbeat_id = ""
        self._stop = asyncio.Event()

    async def start(self, live: bool = False):
        await self.http.__aenter__()
        self.public = PolymarketCLOBPublic(self.s.CLOB_BASE, self.http)

        if live:
            if not (self.s.POLY_API_KEY and self.s.POLY_API_SECRET and self.s.POLY_API_PASSPHRASE and self.s.POLY_FUNDER):
                raise RuntimeError("Missing POLY_* credentials for live mode")
            creds = PolyCreds(
                api_key=self.s.POLY_API_KEY,
                secret=self.s.POLY_API_SECRET,
                passphrase=self.s.POLY_API_PASSPHRASE,
                address=self.s.POLY_FUNDER,
            )
            self.trader = PolymarketCLOBTrader(self.s.CLOB_BASE, self.http, creds, self.s.POLY_API_OWNER)

        # WebSockets
        if self.s.MM_USE_WSS:
            self.market_ws = PolymarketMarketWS(self.s.WSS_MARKET, self.token_ids, self._on_market_event)
            await self.market_ws.start()

        # Start Binance spot poller if econophysics or hedging needs it
        if self.http.session and (self.s.USE_ECONOPHYSICS or self.s.HEDGE_ENABLED):
            syms = self.hedge_symbols or [self.peer_symbol]
            self.spot_poller = BinanceSpotPoller(self.s, self.http.session, syms)
            await self.spot_poller.start()


        if live:
            # user ws should only run in server environments (docs warning)
            markets = list(set(self.market_ids.values())) if self.market_ids else []
            self.user_ws = PolymarketUserWS(self.s.WSS_USER, self.s.POLY_API_KEY, self.s.POLY_API_SECRET, self.s.POLY_API_PASSPHRASE, markets, self._on_user_event)
            await self.user_ws.start()

        asyncio.create_task(self._run(live=live))

    async def stop(self):
        self._stop.set()
        if self.market_ws:
            await self.market_ws.stop()
        if self.user_ws:
            await self.user_ws.stop()
        if self.spot_poller:
            await self.spot_poller.stop()
        await self.http.__aexit__(None, None, None)

    # ------------------- Event handlers -------------------
    def _on_market_event(self, msg: dict):
        et = msg.get("event_type")
        now = ms_ts()
        if et == "book":
            token_id = str(msg.get("asset_id"))
            if token_id in self.books:
                book = self.books[token_id]
                old_bids = book.bids.copy()
                old_asks = book.asks.copy()
                book.apply_snapshot(msg.get("bids") or [], msg.get("asks") or [], int(msg.get("timestamp") or now))
                # update queue estimator for all updated levels at our order prices (best-effort)
                self._diff_levels(token_id, old_bids, old_asks, book)
                self._update_wallet_mark(token_id, book)
        elif et == "price_change":
            ts = int(msg.get("timestamp") or now)
            for pc in (msg.get("price_changes") or []):
                token_id = str(pc.get("asset_id"))
                if token_id not in self.books:
                    continue
                book = self.books[token_id]
                side = str(pc.get("side") or "")
                price = float(pc.get("price") or 0.0)
                size = float(pc.get("size") or 0.0)
                # capture old size at this level for queue progress
                old = (book.bids if side.upper() == "BUY" else book.asks).get(price, 0.0)
                book.apply_delta(side, price, size, ts, best_bid=pc.get("best_bid"), best_ask=pc.get("best_ask"))
                self.queue.on_level_update(token_id, side, price, new_size=size, old_size=old)
                self._update_wallet_mark(token_id, book)
        elif et == "last_trade_price":
            token_id = str(msg.get("asset_id"))
            if token_id in self.books:
                self.strategy.update_flow(token_id, int(msg.get("timestamp") or now), str(msg.get("side") or ""), float(msg.get("size") or 0.0))
        # else ignore

    def _diff_levels(self, token_id: str, old_bids: Dict[float, float], old_asks: Dict[float, float], book: BookTracker) -> None:
        # look at changes at prices where we might have an order
        changed = []
        for side, old_map, new_map in (("BUY", old_bids, book.bids), ("SELL", old_asks, book.asks)):
            for px, new_sz in new_map.items():
                old_sz = old_map.get(px, 0.0)
                if new_sz != old_sz:
                    changed.append((side, px, old_sz, new_sz))
            for px, old_sz in old_map.items():
                if px not in new_map:
                    changed.append((side, px, old_sz, 0.0))
        for side, px, old_sz, new_sz in changed:
            self.queue.on_level_update(token_id, side, px, new_sz, old_sz)

    def _update_wallet_mark(self, token_id: str, book: BookTracker) -> None:
        bb, _, ba, _ = book.top()
        mid = (bb + ba) / 2.0 if bb and ba else (bb or ba or 0.5)
        self.wallet.update_mark(token_id, float(mid))
        # latency estimate: local now - book timestamp
        lat_ms = max(0.0, ms_ts() - book.ts_ms)
        self.strategy.update_latency(token_id, lat_ms)

    def _on_user_event(self, msg: dict):
        et = msg.get("event_type")
        if et == "trade":
            self.wallet.reconcile_trade_event(msg, my_owner=self.s.POLY_API_OWNER)
        elif et == "order":
            # best-effort state sync for cancellations
            if str(msg.get("type", "")).upper() == "CANCELLATION":
                oid = str(msg.get("id") or "")
                if oid:
                    self.wallet.cancel_open_order(oid)
                    self.queue.drop(oid)

    # ------------------- Main loop -------------------
    async def _run(self, live: bool):
        log.info("MM loop started live=%s assets=%d", live, len(self.token_ids))
        last_hb = 0.0
        while not self._stop.is_set():
            t0 = time.time()
            try:
                if live and self.trader and (time.time() - last_hb) >= self.s.MM_HEARTBEAT_SEC:
                    try:
                        hb = await self.trader.heartbeat(self._heartbeat_id)
                        self._heartbeat_id = str(hb.get("heartbeat_id") or self._heartbeat_id)
                    except Exception as e:
                        log.warning("heartbeat failed: %s", e)
                    last_hb = time.time()

                await self._cycle(live=live)
            except Exception as e:
                log.exception("cycle error: %s", e)

            dt = time.time() - t0
            await asyncio.sleep(max(0.0, self.s.MM_REFRESH_SEC - dt))

    async def _cycle(self, live: bool):
        # Global risk stop
        if self.wallet.drawdown_pct() >= self.s.MAX_DRAWDOWN_PCT:
            log.error("Drawdown cap hit %.2f%% — canceling all and stopping.", self.wallet.drawdown_pct() * 100)
            await self._cancel_all(live=live)
            self._stop.set()
            return

        # Build desired quotes per token
        desired: Dict[OrderKey, Quote] = {}
        for token_id in self.token_ids:
            book = self.books.get(token_id)
            if not book:
                continue
            bb, bs, ba, a_s = book.top()
            if bb <= 0 or ba <= 0 or ba <= bb:
                continue

            tick = self.tick_sizes.get(token_id, 0.01)
            mid = (bb + ba) / 2.0
            # inv handled by inventory manager
            lat = self.strategy.lat_ema.get(token_id).value if token_id in self.strategy.lat_ema else 0.0

            # Update rolling series for contagion + hedging
            self.invman.update_mid(token_id, mid)
            if self.hedger:
                self.hedger.update_token_mid(token_id, mid)

            # Recalc contagion occasionally (uses token mids)
            self.invman.maybe_recalc_contagion()

            # Token policy: dynamic max inventory + unwind-only / skew multipliers
            inv_usdc = self.wallet.inventory_usdc(token_id)
            policy = self.invman.policy_for_token(
                token_id,
                positions=self.wallet.positions,
                inv_usdc=inv_usdc,
                base_max_inv_usdc=self.s.MM_MAX_INV_USDC,
            )
            self.max_inv_by_token[token_id] = policy.max_inv_usdc

            # Use Binance spot series when available; otherwise fall back to token mid series
            if self.spot_poller:
                spot_series = self.spot_poller.history(self.peer_symbol) or self.spot_poller.history(self.hedge_symbols[0]) if self.hedge_symbols else []
                peer_series = None
                if self.hedge_symbols and len(self.hedge_symbols) > 1:
                    peer_series = self.spot_poller.history(self.hedge_symbols[1])
            else:
                spot_series = list(self.invman.mid_hist.get(token_id, []))
                peer_series = None

            spot_returns = np.diff(np.log(np.asarray(spot_series))) if len(spot_series) > 10 else np.array([], dtype=float)

            # Econophysics gate (optional)
            reg = self.strategy.compute_regime(spot_series, peer_prices=peer_series)
            if not reg.ok:
                log.debug("Regime blocked %s (H=%.2f Ent=%.2f LLE=%.3f TE=%.3f)", token_id, reg.hurst, reg.entropy, reg.lle, reg.te)
                continue

            quotes = self.strategy.build_quotes(token_id, mid, bb, bs, ba, a_s, tick, lat, (inv_usdc * (self.s.MM_MAX_INV_USDC / max(policy.max_inv_usdc, 1e-9))), spot_returns)
            # Apply inventory policy (unwind-only + pairing skew + contagion sizing)
            quotes = self.invman.apply_policy_to_quotes(quotes, policy)
            for q in quotes:
                desired[OrderKey(token_id, q.side.upper())] = q

        # Reconcile current open orders (paper state) and decide cancels/places
        cancels: List[str] = []
        places: List[Quote] = []

        now = time.time()
        for key, q in desired.items():
            # cooldown
            cd = self._cooldown_until.get(key, 0.0)
            if now < cd:
                continue

            existing = self._find_open_order(key)
            if existing is None:
                places.append(q)
                continue

            # Cancel/replace criteria
            tick = self.tick_sizes.get(q.token_id, 0.01)
            price_changed = abs(existing.price - q.price) >= tick - 1e-12
            age = now - (existing.created_ts)
            qprog = self.queue.progress_frac(existing.order_id)
            stale = self.queue.stale_sec(existing.order_id)

            if age >= self.s.MM_ORDER_TTL_SEC:
                cancels.append(existing.order_id)
                places.append(q)
            elif price_changed:
                cancels.append(existing.order_id)
                places.append(q)
            else:
                # Queue heuristic: if no progress for a while and progress < threshold, step away or reprice
                if stale >= self.s.MM_QUEUE_STALE_SEC and qprog < self.s.MM_QUEUE_MIN_PROGRESS:
                    cancels.append(existing.order_id)
                    # "step ahead" by 1 tick if possible to regain priority
                    if q.side.upper() == "BUY":
                        q.price = min(q.price + tick, self.books[q.token_id].best_ask - tick)
                    else:
                        q.price = max(q.price - tick, self.books[q.token_id].best_bid + tick)
                    places.append(q)

        # Also cancel stale orders that are no longer desired
        desired_keys = set(desired.keys())
        for oid, oo in list(self.wallet.open_orders.items()):
            key = OrderKey(oo.token_id, oo.side.upper())
            if key not in desired_keys:
                if (time.time() - oo.created_ts) >= self.s.MM_ORDER_TTL_SEC:
                    cancels.append(oid)

        if cancels or places:
            await self._apply_cancel_replace(cancels, places, live=live)

        # Cross-hedge (paper-first): adjust Binance hedge positions based on online beta
        if self.s.HEDGE_ENABLED and self.hedger and self.spot_poller:
            spot_prices = {sym: self.spot_poller.price(sym) for sym in self.hedge_symbols}
            spot_hist = {sym: self.spot_poller.history(sym) for sym in self.hedge_symbols}
            token_inv = {tid: self.wallet.inventory_usdc(tid) for tid in self.token_ids}
            targets = self.hedger.target_hedges(token_inventories_usdc=token_inv, spot_prices=spot_prices, spot_histories=spot_hist)
            # Apply targets to paper hedge ledger
            for sym, px in spot_prices.items():
                self.hedge_ledger.update_mark(sym, px)
            for sym, tgt_qty in targets.items():
                px = spot_prices.get(sym, 0.0)
                if px <= 0:
                    continue
                self.hedge_ledger.trade_to_target(sym, tgt_qty, px, max_notional=self.s.HEDGE_MAX_NOTIONAL_USDC)

    def _find_open_order(self, key: OrderKey) -> Optional[OpenOrder]:
        # one order per token per side
        for oo in self.wallet.open_orders.values():
            if oo.token_id == key.token_id and oo.side.upper() == key.side.upper():
                return oo
        return None

    async def _apply_cancel_replace(self, cancels: Sequence[str], places: Sequence[Quote], live: bool):
        cancels = list(dict.fromkeys(cancels))  # unique
        # cancel first (avoid crossing after reprice)
        if cancels:
            await self._cancel_batch(cancels, live=live)
            # cooldown keys associated with cancels
            for oid in cancels:
                oo = self.wallet.open_orders.get(oid)
                if oo:
                    self._cooldown_until[OrderKey(oo.token_id, oo.side.upper())] = time.time() + self.s.MM_COOLDOWN_SEC
                self.wallet.cancel_open_order(oid)
                self.queue.drop(oid)

        if places:
            await self._place_batch(places, live=live)

    async def _cancel_batch(self, order_ids: Sequence[str], live: bool):
        if not live or not self.trader:
            # paper cancel
            for oid in order_ids:
                self.wallet.cancel_open_order(oid)
                self.queue.drop(oid)
            log.info("paper cancel %d", len(order_ids))
            return

        # Live cancel (batch)
        ids = list(order_ids)
        if self.s.MM_USE_BATCH_CANCEL:
            chunk = self.s.MM_BATCH_CANCEL_MAX
            for i in range(0, len(ids), chunk):
                sub = ids[i:i+chunk]
                await self.trader.cancel_orders(sub)
                log.info("cancelled batch %d", len(sub))
        else:
            for oid in ids:
                await self.trader.cancel_order(oid)

    async def _place_batch(self, quotes: Sequence[Quote], live: bool):
        quotes = list(quotes)
        if not quotes:
            return

        def _max_inv(tok: str) -> float:
            return float(self.max_inv_by_token.get(tok, self.s.MM_MAX_INV_USDC))

        if not live or not self.trader:
            # paper place: create pseudo order IDs; record queue start
            placed = 0
            for q in quotes:
                if not self.wallet.can_place(q.token_id, q.side, q.price, q.size, self.s.MM_MAX_USDC_PER_ORDER, _max_inv(q.token_id)):
                    continue
                oid = "paper_" + secrets.token_hex(16)
                self.wallet.record_open_order(OpenOrder(oid, q.token_id, q.side, q.price, q.size, created_ts=int(time.time())))
                # snapshot current level size ahead
                book = self.books[q.token_id]
                lvl = (book.bids if q.side.upper() == "BUY" else book.asks).get(q.price, 0.0)
                self.queue.on_place(oid, q.token_id, q.side, q.price, lvl)
                placed += 1
            log.info("paper placed %d", placed)
            return

        # Live placement: create signed orders (via py-clob-client if enabled), then batch POST /orders.
        if self.sdk_client is None:
            raise RuntimeError(
                "Live mode requires order signing. Set USE_PY_CLOB_CLIENT=true and POLY_PRIVATE_KEY, "
                "or plug in your EIP-712 signer and call trader.post_orders() with signed payloads."
            )

        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL

        signed_orders = []
        level_sizes_before = []
        kept_quotes: List[Quote] = []

        for q in quotes:
            if not self.wallet.can_place(q.token_id, q.side, q.price, q.size, self.s.MM_MAX_USDC_PER_ORDER, _max_inv(q.token_id)):
                continue
            side_const = BUY if q.side.upper() == "BUY" else SELL
            oa = OrderArgs(token_id=q.token_id, price=float(q.price), size=float(q.size), side=side_const)
            so = self.sdk_client.create_order(oa)
            signed_orders.append(so)
            kept_quotes.append(q)
            book = self.books[q.token_id]
            lvl = (book.bids if q.side.upper() == "BUY" else book.asks).get(q.price, 0.0)
            level_sizes_before.append(lvl)

        if not signed_orders:
            return

        # Batch post (max 15 per request per docs)
        maxn = int(self.s.MM_BATCH_MAX)
        for i in range(0, len(signed_orders), maxn):
            sub_orders = signed_orders[i:i+maxn]
            sub_lvls = level_sizes_before[i:i+maxn]
            resp = await self.trader.post_orders(sub_orders, order_type=self.s.MM_ORDER_TYPE, post_only=self.s.MM_POST_ONLY)
            # resp is array aligned with request
            for j, item in enumerate(resp):
                if item.get("success") and item.get("orderID"):
                    oid = str(item["orderID"])
                    q = kept_quotes[i+j]
                    self.wallet.record_open_order(OpenOrder(oid, q.token_id, q.side, q.price, q.size, created_ts=int(time.time())))
                    self.queue.on_place(oid, q.token_id, q.side, q.price, float(sub_lvls[j]))
            log.info("live placed %d", len(sub_orders))


    async def _cancel_all(self, live: bool):
        ids = list(self.wallet.open_orders.keys())
        if ids:
            await self._cancel_batch(ids, live=live)
