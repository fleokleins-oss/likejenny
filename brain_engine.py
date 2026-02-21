# ===================================================================
# APEX TRI-LAYER BRAIN ENGINE v2026.3 (Port 8000)
# Hardened: Hill Tail-Index, AMH Memory, Microstructure-aware confidence
# ===================================================================

from __future__ import annotations

import math
import time
from typing import List, Literal, Optional

import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel, Field

from apex_common.config import BrainConfig
from apex_common.logging import get_logger
from apex_common.security import check_env_file_permissions
from apex_common.metrics import instrument_app

load_dotenv()
log = get_logger("brain_engine")
_ok, _msg = check_env_file_permissions(".env")
if not _ok:
    log.warning(_msg)
else:
    log.info(_msg)
cfg = BrainConfig()

\1instrument_app(app)

# ----------------------------
# Data models
# ----------------------------
Heatmap = Literal["LOW", "MED", "HIGH"]
Action = Literal["EXECUTE", "KILL", "WAIT"]
Side = Literal["LONG", "SHORT", "NONE"]

class MarketData(BaseModel):
    symbol: str = Field(..., examples=["BTC/USDT:USDT", "BTCUSDT"])
    lle: float = Field(..., description="Largest Lyapunov Exponent proxy (<=0 preferred)")
    drawdown_pct: float = Field(..., ge=0.0)
    chaos_detected: bool = False

    # sentiment / crowding
    funding_rate: float = 0.0
    oi_spike: bool = False
    heatmap_intensity: Heatmap = "LOW"

    # extracted gold inputs (optional but recommended)
    recent_pnl_history: List[float] = Field(default_factory=list, description="Recent trade PnL (e.g. last 10)")
    returns_array: List[float] = Field(default_factory=list, description="Recent log-returns array (>= 2*k preferred)")
    contagion_correlation: float = Field(0.0, ge=0.0, le=1.0)

    # microstructure inputs (optional)
    micro_price_shift: float = 0.0     # micro - mid
    orderbook_imbalance: float = 0.0   # [-1, 1]
    long_short_ratio: float = 1.0      # >1 = crowd long


class TailSuggestRequest(BaseModel):
    returns_array: List[float] = Field(default_factory=list)
    k: int = Field(default=20, ge=5, le=200)

class TailSuggestResponse(BaseModel):
    hill_alpha: float
    suggested_warn: float
    suggested_kill: float


class BrainDecision(BaseModel):
    action: Action
    side: Side
    confidence: float = Field(..., ge=0.0, le=1.0)
    risk_multiplier: float = Field(..., ge=0.0, le=1.0)
    reasoning_log: List[str]

# ----------------------------
# math: Hill tail index
# ----------------------------
def hill_tail_index(returns: List[float], k: int) -> float:
    """Hill estimator for tail index (alpha). Lower alpha => fatter tails (more extreme risk).
    Returns a conservative default if insufficient data.
    """
    if len(returns) < max(10, 2 * k):
        return 2.0

    abs_rets = np.abs(np.array(returns, dtype=float))
    abs_rets.sort()
    tail = abs_rets[-k:]
    threshold = abs_rets[-k - 1]
    if threshold <= 1e-12:
        return 2.0

    # Hill estimator: alpha_hat = 1 / mean(log(X_i / X_{k+1}))
    hill = float(np.mean(np.log(np.maximum(tail, 1e-12) / threshold)))
    if not math.isfinite(hill) or hill <= 0:
        return 2.0
    return 1.0 / hill

def calculate_amh_multiplier(pnl_history: List[float]) -> float:
    """Adaptive Market Hypothesis multiplier.
    >1 increases strictness (less trading) after losses; <1 relaxes after wins.
    """
    if not pnl_history:
        return 1.0
    window = pnl_history[-10:] if len(pnl_history) > 10 else pnl_history
    mu = float(np.mean(np.array(window, dtype=float)))
    if mu < 0:
        return 1.0 + min(0.6, abs(mu) * 5.0)
    return 1.0 - min(0.2, mu * 2.0)

def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x

# ----------------------------
# Layers
# ----------------------------
class ReptilianCore:
    @staticmethod
    def check_survival(data: MarketData) -> tuple[bool, float, List[str]]:
        msgs: List[str] = []

        # Hard kills first
        if data.chaos_detected:
            return False, 0.0, ["[REPTILIAN] KILL: chaos_detected=True"]
        if data.lle > 0:
            return False, 0.0, [f"[REPTILIAN] KILL: LLE positive ({data.lle:.6f})"]
        if data.drawdown_pct > cfg.max_drawdown_pct:
            return False, 0.0, [f"[REPTILIAN] KILL: drawdown {data.drawdown_pct:.2f}% > {cfg.max_drawdown_pct:.2f}%"]

        # Tail risk
        alpha = hill_tail_index(data.returns_array, cfg.hill_k)
        risk_mult = 1.0

        if alpha <= cfg.tail_alpha_kill:
            msgs.append(f"[REPTILIAN] KILL: tail alpha {alpha:.2f} <= {cfg.tail_alpha_kill:.2f} (extreme fat tails)")
            return False, 0.0, msgs

        if alpha < cfg.tail_alpha_warn:
            # shrink size smoothly down to 20%
            risk_mult = max(0.2, alpha / cfg.tail_alpha_warn)
            msgs.append(f"[REPTILIAN] WARN: tail alpha {alpha:.2f} < {cfg.tail_alpha_warn:.2f} → risk_mult={risk_mult:.2f}")

        msgs.append("[REPTILIAN] OK: survival checks passed")
        return True, risk_mult, msgs

class LimbicSystem:
    @staticmethod
    def evaluate_sentiment(data: MarketData) -> tuple[str, float, List[str]]:
        msgs: List[str] = []
        amh = calculate_amh_multiplier(data.recent_pnl_history)
        msgs.append(f"[LIMBIC] AMH multiplier={amh:.2f}")

        # contagion
        if data.contagion_correlation >= cfg.contagion_corr_warn:
            msgs.append(f"[LIMBIC] WARN: contagion corr={data.contagion_correlation:.2f} (regime herd)")
            return "CONTAGION_ZONE", amh, msgs

        # crowding from funding/heatmap
        if data.heatmap_intensity == "HIGH" or data.funding_rate > cfg.funding_fear_threshold:
            msgs.append(f"[LIMBIC] WARN: crowded (heatmap={data.heatmap_intensity}, funding={data.funding_rate})")
            return "FEAR_ZONE", amh, msgs

        if data.oi_spike:
            msgs.append("[LIMBIC] WARN: OI spike detected (possible FOMO/forced moves)")
            return "GREED_ZONE", amh, msgs

        return "NEUTRAL", amh, msgs

class Neocortex:
    @staticmethod
    def synthesize_strategy(data: MarketData, sentiment_state: str, amh: float) -> tuple[Action, Side, float, List[str]]:
        msgs: List[str] = []

        # Microstructure confidence tilt
        micro_tilt = 0.0
        # shift in price: positive implies buy pressure; imbalance: positive implies bid-heavy
        micro_tilt += 0.25 * math.tanh(float(data.micro_price_shift) * 100.0)  # scale heuristic
        micro_tilt += 0.20 * float(max(-1.0, min(1.0, data.orderbook_imbalance)))
        # crowd ratio: >1.15 indicates crowd long; <0.85 crowd short
        if data.long_short_ratio > 1.15:
            micro_tilt -= 0.10
            msgs.append(f"[NEOCORTEX] crowd_long detected (LS={data.long_short_ratio:.2f}) → tilt -")
        elif data.long_short_ratio < 0.85:
            micro_tilt += 0.10
            msgs.append(f"[NEOCORTEX] crowd_short detected (LS={data.long_short_ratio:.2f}) → tilt +")

        msgs.append(f"[NEOCORTEX] micro_tilt={micro_tilt:.2f}")

        if sentiment_state == "CONTAGION_ZONE":
            return "WAIT", "NONE", 0.0, msgs + ["[NEOCORTEX] WAIT: contagion regime (skip directional bets)"]

        if sentiment_state == "FEAR_ZONE":
            # stop-hunt environment: only trade if microstructure is very strong
            base = 0.55 + max(0.0, micro_tilt)
            conf = clamp01((base / amh) - 0.05)
            if conf >= 0.70:
                return "EXECUTE", "LONG", conf, msgs + [f"[NEOCORTEX] EXECUTE LONG (fear-zone) conf={conf:.2f}"]
            return "WAIT", "NONE", conf, msgs + [f"[NEOCORTEX] WAIT: fear-zone conf={conf:.2f} < 0.70"]

        if sentiment_state == "GREED_ZONE":
            base = 0.75 - max(0.0, micro_tilt)  # if micro says up, avoid shorting too hard
            conf = clamp01(base / amh)
            if conf >= 0.65:
                return "EXECUTE", "SHORT", conf, msgs + [f"[NEOCORTEX] EXECUTE SHORT (greed-zone) conf={conf:.2f}"]
            return "WAIT", "NONE", conf, msgs + [f"[NEOCORTEX] WAIT: AMH/micro blocked short conf={conf:.2f}"]

        # NEUTRAL default: follow microstructure tilt
        if micro_tilt >= 0.05:
            base = 0.68 + micro_tilt
            side: Side = "LONG"
        elif micro_tilt <= -0.05:
            base = 0.68 + (-micro_tilt)
            side = "SHORT"
        else:
            base = 0.62
            side = "LONG"

        conf = clamp01(base / amh)
        if conf >= 0.55:
            return "EXECUTE", side, conf, msgs + [f"[NEOCORTEX] EXECUTE {side} conf={conf:.2f}"]
        return "WAIT", "NONE", conf, msgs + [f"[NEOCORTEX] WAIT: conf={conf:.2f} below threshold"]

# ----------------------------
# API
# ----------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "service": "brain_engine", "version": app.version}


@app.post("/suggest_tail_thresholds", response_model=TailSuggestResponse)
async def suggest_tail_thresholds(req: TailSuggestRequest):
    """Sugere thresholds de tail-risk a partir de uma série de retornos.
    Heurística simples: define WARN/KILL como funções do alpha atual.
    Para calibração séria, rode backtests e ajuste via env.
    """
    alpha = hill_tail_index(req.returns_array, req.k)
    # Heurística: warn ~ alpha*1.15 (cap), kill ~ alpha*0.90 (floor)
    suggested_warn = max(1.2, min(2.2, alpha * 1.15))
    suggested_kill = max(1.05, min(1.8, alpha * 0.90))
    return TailSuggestResponse(hill_alpha=float(alpha), suggested_warn=float(suggested_warn), suggested_kill=float(suggested_kill))


@app.post("/process_tick", response_model=BrainDecision)
async def process_tick(data: MarketData):
    start = time.perf_counter()
    logs: List[str] = []

    safe, risk_mult, rep = ReptilianCore.check_survival(data)
    logs.extend(rep)
    if not safe:
        logs.append(f"[SYSTEM] processed_ms={(time.perf_counter()-start)*1000:.3f}")
        return BrainDecision(action="KILL", side="NONE", confidence=0.0, risk_multiplier=0.0, reasoning_log=logs)

    state, amh, limb = LimbicSystem.evaluate_sentiment(data)
    logs.extend(limb)

    action, side, conf, neo = Neocortex.synthesize_strategy(data, state, amh)
    logs.extend(neo)

    logs.append(f"[SYSTEM] processed_ms={(time.perf_counter()-start)*1000:.3f} risk_mult={risk_mult:.2f}")
    return BrainDecision(action=action, side=side, confidence=float(conf), risk_multiplier=float(risk_mult), reasoning_log=logs)
