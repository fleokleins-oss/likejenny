"""
spot_feed.py — minimal Binance spot poller (paper-safe).

Why poll?
- Keeps dependencies light and avoids WS complexity.
- Good enough for regime gating + hedge beta estimation.

If you want true low-latency, replace with ccxt.pro websockets.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Sequence

import aiohttp

from .config import Settings
from .utils import clamp

log = logging.getLogger("spot")


BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price"


@dataclass(slots=True)
class SpotState:
    price: float = 0.0
    hist: Deque[float] = None  # type: ignore[assignment]


class BinanceSpotPoller:
    def __init__(self, s: Settings, session: aiohttp.ClientSession, symbols: Sequence[str]):
        self.s = s
        self.session = session
        self.symbols = list(symbols)
        self.state: Dict[str, SpotState] = {sym: SpotState(0.0, deque(maxlen=max(300, s.HEDGE_WINDOW))) for sym in self.symbols}
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task

    async def _fetch_price(self, symbol: str) -> float:
        params = {"symbol": symbol}
        async with self.session.get(BINANCE_TICKER, params=params, timeout=5) as resp:
            data = await resp.json()
            return float(data.get("price", 0.0))

    async def _run(self) -> None:
        log.info("Binance spot poller started symbols=%s", ",".join(self.symbols))
        while not self._stop.is_set():
            try:
                for sym in self.symbols:
                    px = await self._fetch_price(sym)
                    st = self.state[sym]
                    if px > 0:
                        st.price = px
                        st.hist.append(px)
            except Exception as e:
                log.debug("spot poll error: %s", e)
            await asyncio.sleep(max(0.2, float(self.s.BINANCE_POLL_SEC)))

    def price(self, symbol: str) -> float:
        return float(self.state.get(symbol, SpotState()).price)

    def history(self, symbol: str) -> List[float]:
        st = self.state.get(symbol)
        if not st:
            return []
        return list(st.hist)
