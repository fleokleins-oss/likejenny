# ===================================================================
# APEX SHADOW-GLASS ENGINE v2026.3 (Port 8001)
# Hardened: Binance multiplex WS (openInterest+markPrice+depth) + LS ratio scrape (REST)
# ===================================================================

from __future__ import annotations

import asyncio
import json
import math
from collections import defaultdict
from contextlib import asynccontextmanager
import contextlib
from typing import Dict, List, Tuple

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from apex_common.config import ShadowglassConfig
from apex_common.logging import get_logger
from apex_common.security import check_env_file_permissions
from apex_common.metrics import instrument_app

load_dotenv()
log = get_logger("shadowglass")
_ok, _msg = check_env_file_permissions(".env")
if not _ok:
    log.warning(_msg)
else:
    log.info(_msg)
cfg = ShadowglassConfig()

LEVERAGE_TIERS = (25, 50, 100)
MAINTENANCE_MARGIN = 0.004

class MarketEyes:
    def __init__(self):
        self.last_ws_error: str | None = None
        self.last_ws_error_ts: float | None = None

        self.long_clusters: Dict[float, float] = defaultdict(float)
        self.short_clusters: Dict[float, float] = defaultdict(float)
        self.last_oi: float = 0.0
        self.last_price: float = 0.0

        self.micro_shift: float = 0.0
        self.imbalance: float = 0.0

        self._lock = asyncio.Lock()

    async def process_oi_tick(self, current_oi: float, current_price: float):
        async with self._lock:
            if self.last_oi == 0.0 or self.last_price == 0.0:
                self.last_oi = current_oi
                self.last_price = current_price
                return

            oi_delta = current_oi - self.last_oi
            price_delta = current_price - self.last_price

            if abs(oi_delta) >= cfg.oi_delta_threshold:
                notional_delta_usd = abs(oi_delta) * current_price
                usd_per_tier = notional_delta_usd / len(LEVERAGE_TIERS)

                if oi_delta > 0 and price_delta > 0:
                    for lev in LEVERAGE_TIERS:
                        liq_price = current_price * (1 - (1 / lev) + MAINTENANCE_MARGIN)
                        bucket = round(liq_price / cfg.bucket_usd) * cfg.bucket_usd
                        self.long_clusters[bucket] += usd_per_tier

                elif oi_delta > 0 and price_delta < 0:
                    for lev in LEVERAGE_TIERS:
                        liq_price = current_price * (1 + (1 / lev) - MAINTENANCE_MARGIN)
                        bucket = round(liq_price / cfg.bucket_usd) * cfg.bucket_usd
                        self.short_clusters[bucket] += usd_per_tier

            self.last_oi = current_oi
            self.last_price = current_price
            self._apply_decay()

    async def process_depth(self, bids: List[List[str]] | List[List[float]], asks: List[List[str]] | List[List[float]]):
        if not bids or not asks:
            return
        try:
            best_bid_px = float(bids[0][0]); best_bid_sz = float(bids[0][1])
            best_ask_px = float(asks[0][0]); best_ask_sz = float(asks[0][1])

            mid = 0.5 * (best_bid_px + best_ask_px)
            denom = max(best_bid_sz + best_ask_sz, 1e-12)
            micro = (best_ask_px * best_bid_sz + best_bid_px * best_ask_sz) / denom

            async with self._lock:
                self.micro_shift = micro - mid
                self.imbalance = (best_bid_sz - best_ask_sz) / denom
        except Exception:
            return

    def _apply_decay(self):
        for bucket in list(self.long_clusters.keys()):
            self.long_clusters[bucket] *= cfg.decay_rate
            if self.long_clusters[bucket] < cfg.min_cluster_usd:
                del self.long_clusters[bucket]
        for bucket in list(self.short_clusters.keys()):
            self.short_clusters[bucket] *= cfg.decay_rate
            if self.short_clusters[bucket] < cfg.min_cluster_usd:
                del self.short_clusters[bucket]

    async def snapshot(self) -> dict:
        async with self._lock:
            top_longs = sorted(self.long_clusters.items(), key=lambda x: x[1], reverse=True)[:3]
            top_shorts = sorted(self.short_clusters.items(), key=lambda x: x[1], reverse=True)[:3]
            return {
                "long_pain_zones": top_longs,
                "short_pain_zones": top_shorts,
                "micro_price_shift": self.micro_shift,
                "orderbook_imbalance": self.imbalance,
                "last_mark_price": self.last_price,
                "last_open_interest": self.last_oi,
            }

eyes = MarketEyes()

async def fetch_long_short_ratio(client: httpx.AsyncClient, symbol: str) -> float:
    url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    params = {"symbol": symbol.upper(), "period": "5m", "limit": 1}
    try:
        r = await client.get(url, params=params, timeout=3.0)
        r.raise_for_status()
        arr = r.json()
        if arr:
            return float(arr[0].get("longShortRatio", 1.0))
    except Exception:
        pass
    return 1.0

async def stream_binance_multiplex(symbol: str, stop_event: asyncio.Event):
    streams = f"{symbol}@openInterest/{symbol}@markPrice@1s/{symbol}@depth5@100ms"
    url = f"wss://fstream.binance.com/stream?streams={streams}"

    delay = cfg.reconnect_base_delay
    current_oi, current_price = 0.0, 0.0

    log.info(f"👁️ ShadowGlass online: {symbol.upper()} (OI + Mark + Depth)")

    while not stop_event.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                delay = cfg.reconnect_base_delay
                while not stop_event.is_set():
                    msg = await ws.recv()
                    try:
                    data = json.loads(msg)
                except Exception as e:
                    log.warning(f"WS JSON decode failed: {e}")
                    async with eyes._lock:
                        eyes.last_ws_error = f"json_decode: {e}"
                        eyes.last_ws_error_ts = asyncio.get_event_loop().time()
                    continue
                    stream_name = data.get("stream", "")
                    payload = data.get("data", {})

                    if "@openInterest" in stream_name:
                        current_oi = float(payload.get("o", current_oi))
                    elif "@markPrice" in stream_name:
                        current_price = float(payload.get("p", current_price))
                    elif "@depth" in stream_name:
                        await eyes.process_depth(payload.get("b", []), payload.get("a", []))

                    if current_oi > 0 and current_price > 0:
                        await eyes.process_oi_tick(current_oi, current_price)

        except Exception as e:
            log.warning(f"WS reconnecting in {delay:.1f}s: {e}")
            await asyncio.sleep(delay)
            delay = min(cfg.reconnect_max_delay, delay * 1.7)

stop_event = asyncio.Event()
ws_task: asyncio.Task | None = None
http_client: httpx.AsyncClient | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global ws_task, http_client
    http_client = httpx.AsyncClient(headers={"User-Agent": "ApexShadowGlass/2026.3"})
    ws_task = asyncio.create_task(stream_binance_multiplex(cfg.default_symbol.lower(), stop_event))
    yield
    stop_event.set()
    if ws_task:
        ws_task.cancel()
        with contextlib.suppress(Exception):
            await ws_task
    if http_client:
        await http_client.aclose()

\1instrument_app(app)

@app.get("/health")
async def health():
    return {"status": "ok", "service": "shadowglass", "version": app.version, "default_symbol": cfg.default_symbol}

@app.get("/get_market_state/{symbol}")
async def get_market_state(symbol: str):
    if not http_client:
        raise HTTPException(status_code=503, detail="HTTP client not ready")
    snap = await eyes.snapshot()
    ls_ratio = await fetch_long_short_ratio(http_client, symbol)

    return {
        "symbol": symbol.upper(),
        "status": "ACTIVE",
        "long_short_ratio": ls_ratio,
        "is_crowded_long": ls_ratio > 1.15,
        "is_crowded_short": ls_ratio < 0.85,
        "metrics": snap,
    }