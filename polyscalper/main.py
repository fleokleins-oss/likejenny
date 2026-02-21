"""
main.py — entrypoint (console-first).

Usage (paper):
  python -m polyscalper.main --assets <TOKEN_ID1,TOKEN_ID2,...>

Usage (live):
  set LIVE_MODE=true in .env AND pass --live
  python -m polyscalper.main --assets ... --live

Live mode is intentionally blocked unless you implement order signing.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from typing import Dict, List

from .config import load_settings
from .microstructure import MarketMakerEngine
from .inventory_manager import Pair
from .utils import setup_logging


def _parse_csv(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def _parse_kv_csv(s: str) -> Dict[str, str]:
    """
    token:market,token:market...
    """
    out: Dict[str, str] = {}
    for item in _parse_csv(s):
        if ":" in item:
            k, v = item.split(":", 1)
            out[k.strip()] = v.strip()
    return out

def _parse_pairs(s: str) -> List[Pair]:
    """
    Parse POLY_PAIRS / --pairs.
    Formats:
      - conditionId:yesTokenId:noTokenId
      - yesTokenId:noTokenId (pair_id will be synthetic)
    """
    pairs: List[Pair] = []
    for item in _parse_csv(s):
        parts = item.split(":")
        if len(parts) == 3:
            pid, yes, no = parts
        elif len(parts) == 2:
            yes, no = parts
            pid = f"pair_{yes[:8]}_{no[:8]}"
        else:
            continue
        pairs.append(Pair(pair_id=pid.strip(), yes=yes.strip(), no=no.strip()))
    return pairs


async def amain():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=".env", help="Path to .env")
    ap.add_argument("--assets", default=os.getenv("POLY_ASSET_IDS", ""), help="Comma-separated Polymarket token IDs (asset_id)")
    ap.add_argument("--markets", default=os.getenv("POLY_MARKET_IDS", ""), help="Optional token_id:market_id map (comma-separated)")
    ap.add_argument("--ticks", default=os.getenv("POLY_TICK_SIZES", ""), help="Optional token_id:tick map (comma-separated)")
    ap.add_argument("--pairs", default=os.getenv("POLY_PAIRS", ""), help="Optional YES/NO pair map: condition:yes:no,... or yes:no,...")
    ap.add_argument("--live", action="store_true", help="Enable live mode (requires LIVE_MODE=true in .env)")
    args = ap.parse_args()

    s = load_settings(args.env)
    setup_logging(s.LOG_LEVEL)
    log = logging.getLogger("main")

    assets = _parse_csv(args.assets)
    pairs = _parse_pairs(args.pairs) if args.pairs else []
    # If pairs provided, auto-include YES/NO tokens unless already in --assets
    if pairs:
        aset = set(assets)
        for p in pairs:
            aset.add(p.yes); aset.add(p.no)
        assets = list(aset)
    if not assets:
        raise SystemExit("No assets provided. Use --assets or POLY_ASSET_IDS env var.")

    market_ids = _parse_kv_csv(args.markets) if args.markets else {}
    tick_map: Dict[str, float] = {}
    if args.ticks:
        for item in _parse_csv(args.ticks):
            if ":" in item:
                k, v = item.split(":", 1)
                tick_map[k.strip()] = float(v.strip())

    live = bool(args.live and s.LIVE_MODE)
    if args.live and not s.LIVE_MODE:
        log.error("Refusing live run: set LIVE_MODE=true in .env AND pass --live.")
        live = False

    eng = MarketMakerEngine(s, token_ids=assets, market_ids=market_ids, tick_sizes=tick_map or None, pairs=pairs)
    await eng.start(live=live)

    stop_ev = asyncio.Event()

    def _stop(*_):
        stop_ev.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _stop)

    await stop_ev.wait()
    log.info("Stopping...")
    await eng.stop()


if __name__ == "__main__":
    asyncio.run(amain())
