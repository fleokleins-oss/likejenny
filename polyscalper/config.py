"""
config.py — configuration & .env loading.

Safety defaults:
- LIVE_MODE defaults to false
- MM (market maker) uses paper wallet unless BOTH:
    1) LIVE_MODE=true in .env
    2) CLI flag --live is passed
"""
from __future__ import annotations

from dataclasses import dataclass
from dotenv import load_dotenv
import os


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return int(v)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return float(v)


@dataclass(slots=True)
class Settings:
    # ---- Core ----
    LOG_LEVEL: str = "INFO"
    LIVE_MODE: bool = False

    # ---- Polymarket endpoints ----
    CLOB_BASE: str = "https://clob.polymarket.com"
    GAMMA_BASE: str = "https://gamma-api.polymarket.com"
    WSS_MARKET: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    WSS_USER: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

    # ---- Keys (only used in live mode; never commit secrets) ----
    POLY_PRIVATE_KEY: str = ""
    POLY_FUNDER: str = ""  # address that funds trades (can equal wallet address)
    POLY_API_KEY: str = ""  # UUID
    POLY_API_SECRET: str = ""
    POLY_API_PASSPHRASE: str = ""

    # Owner UUID (same as API key UUID in REST examples)
    POLY_API_OWNER: str = ""

    
    # ---- Optional: use official py-clob-client for EIP-712 order signing (recommended for live) ----
    USE_PY_CLOB_CLIENT: bool = False
    POLY_CHAIN_ID: int = 137
    POLY_SIGNATURE_TYPE: int = 1

    # ---- Fees / gas (approx; include in edge/spread) ----
    MAKER_FEE_BPS: int = 0
    TAKER_FEE_BPS: int = 0
    GAS_USDC_EST: float = 0.05

    # ---- Paper wallet ----
    PAPER_START_USDC: float = 500.0

    # ---- Econophysics regime filters ----
    USE_ECONOPHYSICS: bool = True
    HURST_MIN: float = 0.52
    ENTROPY_MAX: float = 2.2
    LLE_MAX: float = 0.0
    TE_MAX: float = 0.25  # transfer entropy threshold (cross-asset contagion proxy)

    # ---- Market maker core ----
    MM_ENABLED: bool = True
    MM_MAX_ASSETS: int = 6
    MM_REFRESH_SEC: float = 1.5
    MM_USE_WSS: bool = True

    MM_POST_ONLY: bool = True
    MM_ORDER_TYPE: str = "GTC"  # GTC or GTD (post-only requires GTC or GTD)
    MM_GTD_LIFETIME_SEC: int = 240
    MM_ORDER_TTL_SEC: int = 12
    MM_HEARTBEAT_SEC: float = 5.0

    MM_BASE_SPREAD_BPS: float = 60.0
    MM_MIN_SPREAD_BPS: float = 20.0
    MM_MAX_SPREAD_BPS: float = 220.0

    MM_TARGET_DEPTH_USDC: float = 15.0  # per-side target size notionally (paper+live)
    MM_MAX_USDC_PER_ORDER: float = 25.0
    MM_MAX_INV_USDC: float = 80.0
    MM_INV_SKEW_K: float = 0.35

    MM_COOLDOWN_SEC: int = 3
    MM_BATCH_MAX: int = 15  # API limit for post multiple orders
    MM_BATCH_CANCEL_MAX: int = 3000  # API limit for cancel multiple orders

    # ---- "True OpenClaw" upgrades ----
    # Batch cancel/replace
    MM_USE_BATCH_POST: bool = True
    MM_USE_BATCH_CANCEL: bool = True

    # Queue heuristics (approx)
    MM_QUEUE_STALE_SEC: int = 20
    MM_QUEUE_MIN_PROGRESS: float = 0.15  # fraction of queue must clear to keep same price

    # Latency-aware adverse selection model
    MM_LAT_EMA_ALPHA: float = 0.2
    MM_LAT_SHADE_K: float = 0.15  # center shift strength
    MM_AS_K_LAT: float = 0.8
    MM_AS_K_VOL: float = 0.6
    MM_AS_K_MICRO: float = 0.6
    MM_AS_K_FLOW: float = 0.4

    # ---- Risk ----

    # ---- Inventory manager (OpenClaw-style) ----
    INV_ENABLED: bool = True
    INV_SOFT_BAND_USDC: float = 0.6   # fraction of max_inv at which we begin unwind bias
    INV_HARD_BAND_USDC: float = 0.9   # fraction where we switch to unwind-only quoting
    INV_UNWIND_ONLY_SIZE_MULT: float = 1.4  # boost unwind side size when hard band hit

    # YES/NO pairing (auto exposure neutralization)
    INV_PAIR_ENABLED: bool = True
    INV_PAIR_TARGET_EXPOSURE_SHARES: float = 0.0  # target (YES_shares - NO_shares)
    INV_PAIR_EXPOSURE_BAND_SHARES: float = 40.0   # exposure band before skewing quotes
    INV_PAIR_SKEW_K: float = 0.35                 # how strongly to skew per exposure band

    # Transfer-entropy contagion -> per-token risk multiplier
    INV_TE_ENABLED: bool = True
    INV_TE_WINDOW: int = 220          # mid-price points per token
    INV_TE_RECALC_SEC: int = 15       # recompute on this cadence
    INV_TE_RISK_K: float = 0.9        # scale contagion -> risk reduction
    INV_TE_MIN_MULT: float = 0.25     # never reduce below this

    # ---- Cross-hedge (Binance) ----
    HEDGE_ENABLED: bool = False       # paper-first; live hedging requires extra wiring
    HEDGE_SYMBOLS: str = "BTCUSDT,ETHUSDT,SOLUSDT"
    HEDGE_WINDOW: int = 260
    HEDGE_REBALANCE_SEC: int = 8
    HEDGE_MAX_NOTIONAL_USDC: float = 120.0
    HEDGE_SLIPPAGE_BPS: float = 5.0

    # Binance spot polling (fallback if you don't wire a WS feed)
    BINANCE_POLL_SEC: float = 1.0

    MAX_DRAWDOWN_PCT: float = 0.10
    MAX_POSITIONS: int = 3
    MAX_RISK_PCT: float = 0.01  # per-position / per-cycle risk budget


def load_settings(env_path: str = ".env") -> Settings:
    load_dotenv(env_path, override=False)
    s = Settings(
        LOG_LEVEL=_env("LOG_LEVEL", "INFO"),
        LIVE_MODE=_env_bool("LIVE_MODE", False),

        CLOB_BASE=_env("CLOB_BASE", "https://clob.polymarket.com"),
        GAMMA_BASE=_env("GAMMA_BASE", "https://gamma-api.polymarket.com"),
        WSS_MARKET=_env("WSS_MARKET", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        WSS_USER=_env("WSS_USER", "wss://ws-subscriptions-clob.polymarket.com/ws/user"),

        POLY_PRIVATE_KEY=_env("POLY_PRIVATE_KEY", ""),
        POLY_FUNDER=_env("POLY_FUNDER", ""),
        POLY_API_KEY=_env("POLY_API_KEY", ""),
        POLY_API_SECRET=_env("POLY_API_SECRET", ""),
        POLY_API_PASSPHRASE=_env("POLY_API_PASSPHRASE", ""),
        POLY_API_OWNER=_env("POLY_API_OWNER", _env("POLY_API_KEY", "")),

        
        USE_PY_CLOB_CLIENT=_env_bool("USE_PY_CLOB_CLIENT", False),
        POLY_CHAIN_ID=_env_int("POLY_CHAIN_ID", 137),
        POLY_SIGNATURE_TYPE=_env_int("POLY_SIGNATURE_TYPE", 1),

        MAKER_FEE_BPS=_env_int("MAKER_FEE_BPS", 0),
        TAKER_FEE_BPS=_env_int("TAKER_FEE_BPS", 0),
        GAS_USDC_EST=_env_float("GAS_USDC_EST", 0.05),

        PAPER_START_USDC=_env_float("PAPER_START_USDC", 500.0),

        USE_ECONOPHYSICS=_env_bool("USE_ECONOPHYSICS", True),
        HURST_MIN=_env_float("HURST_MIN", 0.52),
        ENTROPY_MAX=_env_float("ENTROPY_MAX", 2.2),
        LLE_MAX=_env_float("LLE_MAX", 0.0),
        TE_MAX=_env_float("TE_MAX", 0.25),

        MM_ENABLED=_env_bool("MM_ENABLED", True),
        MM_MAX_ASSETS=_env_int("MM_MAX_ASSETS", 6),
        MM_REFRESH_SEC=_env_float("MM_REFRESH_SEC", 1.5),
        MM_USE_WSS=_env_bool("MM_USE_WSS", True),

        MM_POST_ONLY=_env_bool("MM_POST_ONLY", True),
        MM_ORDER_TYPE=_env("MM_ORDER_TYPE", "GTC"),
        MM_GTD_LIFETIME_SEC=_env_int("MM_GTD_LIFETIME_SEC", 240),
        MM_ORDER_TTL_SEC=_env_int("MM_ORDER_TTL_SEC", 12),
        MM_HEARTBEAT_SEC=_env_float("MM_HEARTBEAT_SEC", 5.0),

        MM_BASE_SPREAD_BPS=_env_float("MM_BASE_SPREAD_BPS", 60.0),
        MM_MIN_SPREAD_BPS=_env_float("MM_MIN_SPREAD_BPS", 20.0),
        MM_MAX_SPREAD_BPS=_env_float("MM_MAX_SPREAD_BPS", 220.0),

        MM_TARGET_DEPTH_USDC=_env_float("MM_TARGET_DEPTH_USDC", 15.0),
        MM_MAX_USDC_PER_ORDER=_env_float("MM_MAX_USDC_PER_ORDER", 25.0),
        MM_MAX_INV_USDC=_env_float("MM_MAX_INV_USDC", 80.0),
        MM_INV_SKEW_K=_env_float("MM_INV_SKEW_K", 0.35),

        MM_COOLDOWN_SEC=_env_int("MM_COOLDOWN_SEC", 3),
        MM_BATCH_MAX=_env_int("MM_BATCH_MAX", 15),
        MM_BATCH_CANCEL_MAX=_env_int("MM_BATCH_CANCEL_MAX", 3000),

        MM_USE_BATCH_POST=_env_bool("MM_USE_BATCH_POST", True),
        MM_USE_BATCH_CANCEL=_env_bool("MM_USE_BATCH_CANCEL", True),

        MM_QUEUE_STALE_SEC=_env_int("MM_QUEUE_STALE_SEC", 20),
        MM_QUEUE_MIN_PROGRESS=_env_float("MM_QUEUE_MIN_PROGRESS", 0.15),

        MM_LAT_EMA_ALPHA=_env_float("MM_LAT_EMA_ALPHA", 0.2),
        MM_LAT_SHADE_K=_env_float("MM_LAT_SHADE_K", 0.15),
        MM_AS_K_LAT=_env_float("MM_AS_K_LAT", 0.8),
        MM_AS_K_VOL=_env_float("MM_AS_K_VOL", 0.6),
        MM_AS_K_MICRO=_env_float("MM_AS_K_MICRO", 0.6),
        MM_AS_K_FLOW=_env_float("MM_AS_K_FLOW", 0.4),

        INV_ENABLED=_env_bool("INV_ENABLED", True),
        INV_SOFT_BAND_USDC=_env_float("INV_SOFT_BAND_USDC", 0.6),
        INV_HARD_BAND_USDC=_env_float("INV_HARD_BAND_USDC", 0.9),
        INV_UNWIND_ONLY_SIZE_MULT=_env_float("INV_UNWIND_ONLY_SIZE_MULT", 1.4),

        INV_PAIR_ENABLED=_env_bool("INV_PAIR_ENABLED", True),
        INV_PAIR_TARGET_EXPOSURE_SHARES=_env_float("INV_PAIR_TARGET_EXPOSURE_SHARES", 0.0),
        INV_PAIR_EXPOSURE_BAND_SHARES=_env_float("INV_PAIR_EXPOSURE_BAND_SHARES", 40.0),
        INV_PAIR_SKEW_K=_env_float("INV_PAIR_SKEW_K", 0.35),

        INV_TE_ENABLED=_env_bool("INV_TE_ENABLED", True),
        INV_TE_WINDOW=_env_int("INV_TE_WINDOW", 220),
        INV_TE_RECALC_SEC=_env_int("INV_TE_RECALC_SEC", 15),
        INV_TE_RISK_K=_env_float("INV_TE_RISK_K", 0.9),
        INV_TE_MIN_MULT=_env_float("INV_TE_MIN_MULT", 0.25),

        HEDGE_ENABLED=_env_bool("HEDGE_ENABLED", False),
        HEDGE_SYMBOLS=_env("HEDGE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"),
        HEDGE_WINDOW=_env_int("HEDGE_WINDOW", 260),
        HEDGE_REBALANCE_SEC=_env_int("HEDGE_REBALANCE_SEC", 8),
        HEDGE_MAX_NOTIONAL_USDC=_env_float("HEDGE_MAX_NOTIONAL_USDC", 120.0),
        HEDGE_SLIPPAGE_BPS=_env_float("HEDGE_SLIPPAGE_BPS", 5.0),

        BINANCE_POLL_SEC=_env_float("BINANCE_POLL_SEC", 1.0),

        MAX_DRAWDOWN_PCT=_env_float("MAX_DRAWDOWN_PCT", 0.10),
        MAX_POSITIONS=_env_int("MAX_POSITIONS", 3),
        MAX_RISK_PCT=_env_float("MAX_RISK_PCT", 0.01),
    )
    return s
