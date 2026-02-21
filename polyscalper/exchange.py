"""
exchange.py — Polymarket CLOB (REST + WSS) + optional Binance reference feed.

This module focuses on the "OpenClaw-style" microstructure needs:
- Batch cancel/replace via REST POST /orders and DELETE /orders
- Market channel for full-depth book snapshots + price level deltas
- User channel for order/trade reconciliation

Refs:
- Market WS payloads (book / price_change / last_trade_price): docs
- User WS payloads (order / trade): docs
- Batch order REST: POST /orders, max 15
- Batch cancel REST: DELETE /orders, max 3000
- HMAC signature format: timestamp + method + requestPath (+ body)
"""
from __future__ import annotations

import asyncio
import json
import base64
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import aiohttp

from .utils import safe_json_dumps, ms_ts

log = logging.getLogger("exchange")


# -----------------------------
# Polymarket L2 HMAC headers
# -----------------------------
@dataclass(slots=True)
class PolyCreds:
    api_key: str
    secret: str
    passphrase: str
    address: str  # funder/signer address (POLY_ADDRESS header)


def _b64url_to_bytes(secret: str) -> bytes:
    # secret may arrive in base64url form; pad
    s = secret.replace("-", "+").replace("_", "/")
    pad = "=" * ((4 - len(s) % 4) % 4)
    return base64.b64decode(s + pad)


def build_poly_hmac_signature(secret_b64url: str, timestamp: int, method: str, request_path: str, body: Optional[str]) -> str:
    """
    Signature is url-safe base64(HMAC_SHA256(secret, timestamp+method+requestPath+body?)).
    Keeps '=' padding (url safe base64).
    """
    key = _b64url_to_bytes(secret_b64url)
    msg = f"{timestamp}{method.upper()}{request_path}"
    if body is not None:
        msg += body
    sig = hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()
    b64 = base64.b64encode(sig).decode("ascii")
    return b64.replace("+", "-").replace("/", "_")


def l2_headers(creds: PolyCreds, method: str, request_path: str, body_obj: Optional[Any]) -> Dict[str, str]:
    # Conservative skew: some clients use a small negative skew to tolerate clock drift.
    ts = int(time.time()) - 5
    body = None
    if body_obj is not None:
        body = safe_json_dumps(body_obj)
    sig = build_poly_hmac_signature(creds.secret, ts, method, request_path, body)
    return {
        "POLY_ADDRESS": creds.address,
        "POLY_API_KEY": creds.api_key,
        "POLY_PASSPHRASE": creds.passphrase,
        "POLY_SIGNATURE": sig,
        "POLY_TIMESTAMP": str(ts),
        "Content-Type": "application/json",
    }


# -----------------------------
# HTTP client with basic retry
# -----------------------------
class HttpClient:
    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def request(self, method: str, url: str, *, headers: Optional[dict] = None, params: Optional[dict] = None, json_body: Any = None, retries: int = 3) -> Any:
        assert self.session is not None, "HttpClient not started"
        last_err = None
        for attempt in range(retries):
            try:
                async with self.session.request(method, url, headers=headers, params=params, json=json_body) as resp:
                    txt = await resp.text()
                    if resp.status >= 400:
                        raise RuntimeError(f"HTTP {resp.status} {url}: {txt[:300]}")
                    if "application/json" in (resp.headers.get("Content-Type") or ""):
                        return await resp.json()
                    return txt
            except Exception as e:
                last_err = e
                await asyncio.sleep(min(1.5 ** attempt, 5.0))
        raise last_err  # type: ignore[misc]


# -----------------------------
# Market data structures
# -----------------------------
@dataclass(slots=True)
class Level:
    price: float
    size: float


class BookTracker:
    """
    Tracks full-depth book from snapshots + price_change deltas.
    Maintains dicts for O(1) level lookup, and cached sorted top-of-book.
    """
    def __init__(self):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.best_bid: float = 0.0
        self.best_ask: float = 1.0
        self.ts_ms: int = 0

    def apply_snapshot(self, bids: Sequence[dict], asks: Sequence[dict], ts_ms: int) -> None:
        self.bids = {float(l["price"]): float(l["size"]) for l in bids}
        self.asks = {float(l["price"]): float(l["size"]) for l in asks}
        self.ts_ms = ts_ms
        self._recalc_best()

    def apply_delta(self, side: str, price: float, size: float, ts_ms: int, best_bid: Optional[float] = None, best_ask: Optional[float] = None) -> None:
        book = self.bids if side.upper() == "BUY" else self.asks
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size
        self.ts_ms = ts_ms
        # If server gave best_bid/ask, trust; else recompute.
        if best_bid is not None:
            self.best_bid = float(best_bid)
        if best_ask is not None:
            self.best_ask = float(best_ask)
        if best_bid is None and best_ask is None:
            self._recalc_best()

    def _recalc_best(self):
        self.best_bid = max(self.bids.keys()) if self.bids else 0.0
        self.best_ask = min(self.asks.keys()) if self.asks else 1.0

    def top(self) -> Tuple[float, float, float, float]:
        bb = self.best_bid
        ba = self.best_ask
        bs = self.bids.get(bb, 0.0) if bb else 0.0
        a_s = self.asks.get(ba, 0.0) if ba else 0.0
        return bb, bs, ba, a_s


# -----------------------------
# Polymarket REST wrappers
# -----------------------------
class PolymarketCLOBPublic:
    def __init__(self, base_url: str, http: HttpClient):
        self.base = base_url.rstrip("/")
        self.http = http

    async def get_book(self, token_id: str) -> dict:
        return await self.http.request("GET", f"{self.base}/book", params={"token_id": token_id})

    async def get_books(self, token_ids: Sequence[str]) -> list:
        # POST /books takes array of token_id objects (see endpoints list); keep optional.
        payload = [{"token_id": t} for t in token_ids]
        return await self.http.request("POST", f"{self.base}/books", json_body=payload)


class PolymarketCLOBTrader:
    def __init__(self, base_url: str, http: HttpClient, creds: PolyCreds, owner_uuid: str):
        self.base = base_url.rstrip("/")
        self.http = http
        self.creds = creds
        self.owner = owner_uuid

    async def post_order(self, signed_order: dict, order_type: str = "GTC", post_only: bool = False, defer_exec: bool = False) -> dict:
        """
        POST /order — place a single signed order.
        """
        payload = {"order": signed_order, "owner": self.owner, "orderType": order_type, "deferExec": defer_exec}
        # postOnly isn't documented in API reference for /order, but SDK supports it; include optionally.
        if post_only:
            payload["postOnly"] = True
        headers = l2_headers(self.creds, "POST", "/order", payload)
        return await self.http.request("POST", f"{self.base}/order", headers=headers, json_body=payload)

    async def post_orders(self, signed_orders: Sequence[dict], order_type: str = "GTC", post_only: bool = False, defer_exec: bool = False) -> list:
        """
        POST /orders — place up to 15 orders (batch).
        Body is array of {order, owner, orderType, deferExec, ...}.
        """
        batch = []
        for so in signed_orders:
            item = {"order": so, "owner": self.owner, "orderType": order_type, "deferExec": defer_exec}
            if post_only:
                item["postOnly"] = True
            batch.append(item)
        headers = l2_headers(self.creds, "POST", "/orders", batch)
        return await self.http.request("POST", f"{self.base}/orders", headers=headers, json_body=batch)

    async def cancel_order(self, order_id: str) -> dict:
        payload = {"orderID": order_id}
        headers = l2_headers(self.creds, "DELETE", "/order", payload)
        return await self.http.request("DELETE", f"{self.base}/order", headers=headers, json_body=payload)

    async def cancel_orders(self, order_ids: Sequence[str]) -> dict:
        payload = list(order_ids)
        headers = l2_headers(self.creds, "DELETE", "/orders", payload)
        return await self.http.request("DELETE", f"{self.base}/orders", headers=headers, json_body=payload)

    async def cancel_market_orders(self, market: Optional[str] = None, asset_id: Optional[str] = None) -> dict:
        payload: Dict[str, str] = {}
        if market:
            payload["market"] = market
        if asset_id:
            payload["asset_id"] = asset_id
        headers = l2_headers(self.creds, "DELETE", "/cancel-market-orders", payload)
        return await self.http.request("DELETE", f"{self.base}/cancel-market-orders", headers=headers, json_body=payload)

    async def get_open_orders(self, market: Optional[str] = None, asset_id: Optional[str] = None) -> list:
        params = {}
        if market:
            params["market"] = market
        if asset_id:
            params["asset_id"] = asset_id
        headers = l2_headers(self.creds, "GET", "/orders", None)
        return await self.http.request("GET", f"{self.base}/orders", headers=headers, params=params)

    async def heartbeat(self, heartbeat_id: str = "") -> dict:
        payload = {"heartbeat_id": heartbeat_id}
        headers = l2_headers(self.creds, "POST", "/heartbeat", payload)
        return await self.http.request("POST", f"{self.base}/heartbeat", headers=headers, json_body=payload)


# -----------------------------
# WebSocket clients
# -----------------------------
class PolymarketMarketWS:
    """
    Public market channel:
    - send {"assets_ids":[...], "type":"market"}
    - receives: book, price_change, last_trade_price, tick_size_change, etc.
    """
    def __init__(self, wss_url: str, asset_ids: Sequence[str], on_event: Callable[[dict], None]):
        self.wss_url = wss_url
        self.asset_ids = list(asset_ids)
        self.on_event = on_event
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self):
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self._ws:
            await self._ws.close()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    async def update_assets(self, asset_ids: Sequence[str]):
        self.asset_ids = list(asset_ids)
        if self._ws and not self._ws.closed:
            msg = {"operation": "subscribe", "assets_ids": self.asset_ids}
            await self._ws.send_json(msg)

    async def _run(self):
        async with aiohttp.ClientSession() as session:
            while not self._stop.is_set():
                try:
                    async with session.ws_connect(self.wss_url, heartbeat=20) as ws:
                        self._ws = ws
                        await ws.send_json({"assets_ids": self.asset_ids, "type": "market"})
                        async for msg in ws:
                            if self._stop.is_set():
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = msg.json(loads=json.loads)  # type: ignore[name-defined]
                                except Exception:
                                    import json as _json
                                    data = _json.loads(msg.data)
                                self.on_event(data)
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break
                except Exception as e:
                    log.warning("MarketWS reconnect: %s", e)
                    await asyncio.sleep(1.0)


class PolymarketUserWS:
    """
    Authenticated user channel:
    - connect wss://.../ws/user
    - send subscription request with auth {apiKey, secret, passphrase}, type:user
    - optional subscribe update with markets list
    """
    def __init__(self, wss_url: str, api_key: str, secret: str, passphrase: str, markets: Optional[Sequence[str]], on_event: Callable[[dict], None]):
        self.wss_url = wss_url
        self.api_key = api_key
        self.secret = secret
        self.passphrase = passphrase
        self.markets = list(markets) if markets else []
        self.on_event = on_event
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self):
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self._ws:
            await self._ws.close()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    async def update_markets(self, markets: Sequence[str]):
        self.markets = list(markets)
        if self._ws and not self._ws.closed:
            await self._ws.send_json({"operation": "subscribe", "markets": self.markets})

    async def _run(self):
        async with aiohttp.ClientSession() as session:
            while not self._stop.is_set():
                try:
                    async with session.ws_connect(self.wss_url, heartbeat=20) as ws:
                        self._ws = ws
                        await ws.send_json({
                            "auth": {"apiKey": self.api_key, "secret": self.secret, "passphrase": self.passphrase},
                            "type": "user",
                        })
                        if self.markets:
                            await ws.send_json({"operation": "subscribe", "markets": self.markets})
                        async for msg in ws:
                            if self._stop.is_set():
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                import json as _json
                                data = _json.loads(msg.data)
                                self.on_event(data)
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break
                except Exception as e:
                    log.warning("UserWS reconnect: %s", e)
                    await asyncio.sleep(1.0)
