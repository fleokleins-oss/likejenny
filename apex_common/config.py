import os
from dataclasses import dataclass

def _f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _i(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _s(name: str, default: str) -> str:
    return os.getenv(name, default)

def _b(name: str, default: bool) -> bool:
    v = os.getenv(name, str(default)).strip().upper()
    return v in ("1", "TRUE", "YES", "Y", "ON")

@dataclass(frozen=True)
class BrainConfig:
    max_drawdown_pct: float = _f("BRAIN_MAX_DRAWDOWN_PCT", 8.0)
    funding_fear_threshold: float = _f("BRAIN_FUNDING_FEAR_THRESHOLD", 0.00015)
    hill_k: int = _i("BRAIN_HILL_K", 20)
    tail_alpha_warn: float = _f("BRAIN_TAIL_ALPHA_WARN", 1.5)
    tail_alpha_kill: float = _f("BRAIN_TAIL_ALPHA_KILL", 1.1)
    contagion_corr_warn: float = _f("BRAIN_CONTAGION_CORR_WARN", 0.85)

@dataclass(frozen=True)
class ShadowglassConfig:
    default_symbol: str = _s("SHADOWGLASS_DEFAULT_SYMBOL", "btcusdt")
    bucket_usd: float = _f("SHADOWGLASS_BUCKET_USD", 50.0)
    decay_rate: float = _f("SHADOWGLASS_DECAY_RATE", 0.95)
    min_cluster_usd: float = _f("SHADOWGLASS_MIN_CLUSTER_USD", 1000.0)
    oi_delta_threshold: float = _f("SHADOWGLASS_OI_DELTA_THRESHOLD", 10.0)
    reconnect_base_delay: float = _f("SHADOWGLASS_RECONNECT_BASE_DELAY", 1.0)
    reconnect_max_delay: float = _f("SHADOWGLASS_RECONNECT_MAX_DELAY", 20.0)

@dataclass(frozen=True)
class ExecutionerConfig:
    use_testnet: bool = _b("USE_TESTNET", True)
    default_risk_pct: float = _f("EXEC_DEFAULT_RISK_PCT", 0.01)
    default_sl_pct: float = _f("EXEC_DEFAULT_SL_PCT", 0.015)
    default_tp_pct: float = _f("EXEC_DEFAULT_TP_PCT", 0.045)
    max_notional_usd: float = _f("EXEC_MAX_NOTIONAL_USD", 25000.0)
