# ===================================================================
# APEX MAESTRO ORCHESTRATOR v2026.5 (Port 8004)
# Sync pipeline + Redis async queue (Streams)
# ===================================================================

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from typing import Literal, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from apex_common.logging import get_logger
from apex_common.metrics import instrument_app
from apex_common.rate_limit import AsyncRateLimiter
from apex_common.security import check_env_file_permissions
from apex_common.maestro_pipeline import run_pipeline
from apex_common.redis_queue import get_redis, get_job, set_job_status
from apex_common.redis_queue import STREAM, GROUP, CONSUMER, DLQ_STREAM, ensure_group, enqueue_job, DLQ_STREAM
from apex_common.redis_queue import enqueue_job

load_dotenv()
log = get_logger("maestro")
_ok, _msg = check_env_file_permissions(".env")
if not _ok:
    log.warning(_msg)
else:
    log.info(_msg)

def _env(name: str, default: str) -> str:
    return os.getenv(name, default)

# Upstream URLs
BRAIN_URL = _env("MAESTRO_BRAIN_URL", "http://127.0.0.1:8000")
SHADOW_URL = _env("MAESTRO_SHADOW_URL", "http://127.0.0.1:8001")
EXEC_URL = _env("MAESTRO_EXEC_URL", "http://127.0.0.1:8002")
BINANCE_FAPI = _env("MAESTRO_BINANCE_FAPI", "https://fapi.binance.com")

# Retry/Rate limit knobs
TIMEOUT_S = float(_env("MAESTRO_TIMEOUT_S", "4.0"))
ATTEMPTS = int(_env("MAESTRO_ATTEMPTS", "4"))
RPS_SHADOW = float(_env("MAESTRO_RPS_SHADOW", "5"))
RPS_BRAIN  = float(_env("MAESTRO_RPS_BRAIN",  "8"))
RPS_EXEC   = float(_env("MAESTRO_RPS_EXEC",   "3"))

# -------- symbol normalization --------
COMMON_QUOTES = ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH")

def _parse_ccxt_symbol(sym: str) -> tuple[str, str] | None:
    s = sym.strip().upper()
    if "/" in s:
        base = s.split("/")[0].strip()
        rest = s.split("/")[1].strip()
        quote = rest.split(":")[0].strip() if ":" in rest else rest
        if base and quote:
            return base, quote
    return None

def _parse_compact_symbol(sym: str) -> tuple[str, str] | None:
    s = sym.strip().upper().replace("-", "").replace("_", "")
    for q in sorted(COMMON_QUOTES, key=len, reverse=True):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    return None

def normalize_symbols(sym: str) -> tuple[str, str, str]:
    s_in = sym.strip()
    parsed = _parse_ccxt_symbol(s_in) or _parse_compact_symbol(s_in)
    if parsed:
        base, quote = parsed
        return s_in, f"{base}{quote}", f"{base}/{quote}:{quote}"
    return s_in, s_in.strip().upper(), s_in.strip()

Venue = Literal["binance", "bybit", "okx"]

class MaestroRequest(BaseModel):
    symbol: str = Field(..., description="BTCUSDT / BTC/USDT / BTC/USDT:USDT")
    venue: Venue = "binance"

    base_risk_pct: float = Field(0.01, gt=0, le=0.05)
    sl_pct: float = Field(0.015, gt=0, lt=0.25)
    tp_pct: float = Field(0.045, gt=0, lt=1.0)

    lle: float = -0.05
    drawdown_pct: float = 0.0
    chaos_detected: bool = False
    heatmap_intensity: Literal["LOW", "MED", "HIGH"] = "LOW"
    oi_spike: bool = False
    funding_rate: Optional[float] = None

    recent_pnl_history: list[float] = Field(default_factory=list)
    returns_array: list[float] = Field(default_factory=list)
    contagion_correlation: float = Field(0.0, ge=0.0, le=1.0)

    dry_run: bool = False
    min_confidence: float = Field(0.55, ge=0.0, le=1.0)
    scale_by_confidence: bool = True

    idempotency_key: Optional[str] = None

class MaestroQueuedResponse(BaseModel):
    status: str
    job_id: str

class MaestroJobResponse(BaseModel):
    status: str
    job_id: str
    payload: Optional[dict] = None
    result: Optional[dict] = None
    detail: Optional[str] = None

http: httpx.AsyncClient | None = None
redis_client = None

lim_shadow = AsyncRateLimiter(RPS_SHADOW, burst=RPS_SHADOW)
lim_brain  = AsyncRateLimiter(RPS_BRAIN,  burst=RPS_BRAIN)
lim_exec   = AsyncRateLimiter(RPS_EXEC,   burst=RPS_EXEC)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http, redis_client
    http = httpx.AsyncClient(headers={"User-Agent": "ApexMaestro/2026.5"})
    redis_client = await get_redis()
    await ensure_group(redis_client)
    yield
    if http:
        await http.aclose()
    if redis_client:
        await redis_client.aclose()

app = FastAPI(title="Apex Maestro Orchestrator", version="2026.7", lifespan=lifespan)
instrument_app(app)

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "maestro",
        "version": app.version,
        "brain": BRAIN_URL,
        "shadowglass": SHADOW_URL,
        "executioner": EXEC_URL,
        "redis_stream": STREAM,
        "redis_group": GROUP,
        "redis_consumer": CONSUMER,
        "redis_dlq_stream": DLQ_STREAM,
    }

@app.post("/orchestrate")
async def orchestrate(req: MaestroRequest):
    if not http:
        raise HTTPException(status_code=503, detail="HTTP client not ready")

    rid = uuid.uuid4().hex[:12]
    sym_in, shadow_symbol, exec_symbol = normalize_symbols(req.symbol)

    payload = req.model_dump()
    payload["shadow_symbol"] = shadow_symbol
    payload["exec_symbol"] = exec_symbol

    log.info(f"rid={rid} orchestrate(sync) symbol={sym_in} venue={req.venue}")

    try:
        result = await run_pipeline(
            http=http,
            req=payload,
            brain_url=BRAIN_URL,
            shadow_url=SHADOW_URL,
            exec_url=EXEC_URL,
            binance_fapi=BINANCE_FAPI,
            timeout_s=TIMEOUT_S,
            attempts=ATTEMPTS,
            lim_shadow=lim_shadow,
            lim_brain=lim_brain,
            lim_exec=lim_exec,
        )
        result.setdefault("notes", []).append(f"request_id={rid}")
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.post("/orchestrate_async", response_model=MaestroQueuedResponse)
async def orchestrate_async(req: MaestroRequest):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not ready")

    sym_in, shadow_symbol, exec_symbol = normalize_symbols(req.symbol)
    payload = req.model_dump()
    payload["shadow_symbol"] = shadow_symbol
    payload["exec_symbol"] = exec_symbol

    job_id = (req.idempotency_key or "").strip()
    job_id = f"idem:{job_id}" if job_id else uuid.uuid4().hex

    mode, msg_id = await enqueue_job(redis_client, job_id, payload)
    if mode == "ENQUEUED":
        await set_job_status(redis_client, job_id, "QUEUED")
        log.info(f"queued job_id={job_id} msg_id={msg_id} symbol={sym_in} venue={req.venue}")
        return MaestroQueuedResponse(status="QUEUED", job_id=job_id)

    # idempotency hit
    log.info(f"idempotent hit job_id={job_id} symbol={sym_in} venue={req.venue}")
    return MaestroQueuedResponse(status="EXISTS", job_id=job_id)

@app.get("/jobs/{job_id}", response_model=MaestroJobResponse)
async def job_status(job_id: str):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not ready")

    data = await get_job(redis_client, job_id)
    if not data:
        raise HTTPException(status_code=404, detail="job not found")

    payload = None
    result = None
    try:
        import json
        if "payload" in data:
            payload = json.loads(data["payload"])
        if "result" in data:
            result = json.loads(data["result"])
    except Exception:
        pass

    return MaestroJobResponse(
        status=data.get("status", "UNKNOWN"),
        job_id=job_id,
        payload=payload,
        result=result,
        detail=data.get("error"),
    )


@app.get("/dlq/recent")
async def dlq_recent_endpoint(count: int = 50):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not ready")
    count = max(1, min(500, int(count)))
    return {"status": "ok", "count": count, "items": await dlq_recent(redis_client, count=count)}

@app.post("/jobs/{job_id}/retry")
async def retry_job(job_id: str):
    """Manual retry: re-enqueue a job from FAILED/DLQ/RETRY_SCHEDULED."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not ready")
    data = await get_job(redis_client, job_id)
    if not data:
        raise HTTPException(status_code=404, detail="job not found")
    msg_id = await requeue_job(redis_client, job_id)
    return {"status": "QUEUED", "job_id": job_id, "msg_id": msg_id}
