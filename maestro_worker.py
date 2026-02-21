# ===================================================================
# APEX MAESTRO WORKER v2026.7
# Redis Streams consumer with:
# - retries scheduled via delayed ZSET (exponential backoff)
# - DLQ after max attempts
# - pending-claim (XCLAIM) for crashed consumers
# ===================================================================

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import httpx
from dotenv import load_dotenv

from apex_common.logging import get_logger
from apex_common.maestro_pipeline import run_pipeline
from apex_common.rate_limit import AsyncRateLimiter
from apex_common.security import check_env_file_permissions
from apex_common.retry_policy import compute_delay
from apex_common.redis_queue import (
    get_redis,
    ensure_group,
    STREAM,
    GROUP,
    CONSUMER,
    DLQ_STREAM,
    JOB_MAX_ATTEMPTS,
    bump_attempts,
    get_job,
    set_job_status,
    set_job_result,
    send_to_dlq,
    schedule_retry,
    pop_due_retry,
    requeue_job,
    DELAYED_ZSET,
)

load_dotenv()
log = get_logger("maestro_worker")
_ok, _msg = check_env_file_permissions(".env")
if not _ok:
    log.warning(_msg)
else:
    log.info(_msg)

def _env(name: str, default: str) -> str:
    return os.getenv(name, default)

BRAIN_URL = _env("MAESTRO_BRAIN_URL", "http://127.0.0.1:8000")
SHADOW_URL = _env("MAESTRO_SHADOW_URL", "http://127.0.0.1:8001")
EXEC_URL = _env("MAESTRO_EXEC_URL", "http://127.0.0.1:8002")
BINANCE_FAPI = _env("MAESTRO_BINANCE_FAPI", "https://fapi.binance.com")

TIMEOUT_S = float(_env("MAESTRO_TIMEOUT_S", "4.0"))
ATTEMPTS = int(_env("MAESTRO_ATTEMPTS", "4"))

RPS_SHADOW = float(_env("MAESTRO_RPS_SHADOW", "5"))
RPS_BRAIN  = float(_env("MAESTRO_RPS_BRAIN",  "8"))
RPS_EXEC   = float(_env("MAESTRO_RPS_EXEC",   "3"))

CLAIM_IDLE_MS = int(_env("MAESTRO_CLAIM_IDLE_MS", "60000"))
CLAIM_CHECK_INTERVAL_S = float(_env("MAESTRO_CLAIM_CHECK_INTERVAL_S", "10"))

SCHED_TICK_S = float(_env("MAESTRO_SCHEDULER_TICK_S", "1"))

lim_shadow = AsyncRateLimiter(RPS_SHADOW, burst=RPS_SHADOW)
lim_brain  = AsyncRateLimiter(RPS_BRAIN,  burst=RPS_BRAIN)
lim_exec   = AsyncRateLimiter(RPS_EXEC,   burst=RPS_EXEC)

async def process_job(http: httpx.AsyncClient, r, job_id: str):
    rid = uuid.uuid4().hex[:12]
    data = await get_job(r, job_id)
    if not data or "payload" not in data:
        await send_to_dlq(r, job_id, "payload missing")
        return

    try:
        payload = json.loads(data["payload"])
    except Exception as e:
        await send_to_dlq(r, job_id, f"bad payload json: {e}")
        return

    attempt_n = await bump_attempts(r, job_id)
    if attempt_n > JOB_MAX_ATTEMPTS:
        await send_to_dlq(r, job_id, f"max attempts exceeded ({attempt_n-1}/{JOB_MAX_ATTEMPTS})")
        return

    await set_job_status(r, job_id, "RUNNING", attempt=attempt_n)
    log.info(f"rid={rid} job_id={job_id} RUNNING attempt={attempt_n}")

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
        result.setdefault("notes", []).append(f"attempt={attempt_n}")
        await set_job_result(r, job_id, result)
        log.info(f"rid={rid} job_id={job_id} DONE status={result.get('status')}")
    except Exception as e:
        err = str(e)
        log.error(f"rid={rid} job_id={job_id} FAILED attempt={attempt_n} err={err}")

        if attempt_n >= JOB_MAX_ATTEMPTS:
            await send_to_dlq(r, job_id, f"failed after {attempt_n} attempts: {err}")
            return

        # schedule automatic retry
        delay = compute_delay(attempt_n)
        due = time.time() + delay
        await schedule_retry(r, job_id, due_ts=due, reason=f"attempt {attempt_n} failed: {err}")
        log.warning(f"job_id={job_id} scheduled retry in {delay:.1f}s (due={int(due)})")

async def claim_pending(r, consumer: str):
    try:
        summ = await r.xpending(STREAM, GROUP)
        if not summ or summ.get("count", 0) == 0:
            return
        pending = await r.xpending_range(STREAM, GROUP, min="-", max="+", count=10)
        if not pending:
            return
        to_claim = [p["message_id"] for p in pending if int(p.get("time_since_delivered", 0)) >= CLAIM_IDLE_MS]
        if not to_claim:
            return
        claimed = await r.xclaim(STREAM, GROUP, consumer, min_idle_time=CLAIM_IDLE_MS, message_ids=to_claim)
        if claimed:
            log.warning(f"claimed {len(claimed)} pending messages (idle>{CLAIM_IDLE_MS}ms)")
    except Exception as e:
        log.warning(f"claim_pending error: {e}")

async def scheduler_loop(r):
    """Moves due jobs from delayed ZSET back into main stream."""
    log.info(f"Scheduler online zset={DELAYED_ZSET} tick={SCHED_TICK_S}s")
    while True:
        try:
            job_id = await pop_due_retry(r, now_ts=time.time())
            if job_id:
                msg_id = await requeue_job(r, job_id)
                log.info(f"requeued delayed job_id={job_id} msg_id={msg_id}")
            else:
                await asyncio.sleep(SCHED_TICK_S)
        except Exception as e:
            log.warning(f"scheduler error: {e}")
            await asyncio.sleep(max(1.0, SCHED_TICK_S))

async def worker_loop():
    r = await get_redis()
    await ensure_group(r)

    http = httpx.AsyncClient(headers={"User-Agent": "ApexMaestroWorker/2026.7"})
    sched_task = asyncio.create_task(scheduler_loop(r))

    last_claim = 0.0
    try:
        log.info(f"Worker online stream={STREAM} group={GROUP} consumer={CONSUMER} dlq={DLQ_STREAM} max_attempts={JOB_MAX_ATTEMPTS}")

        while True:
            now = time.time()
            if now - last_claim >= CLAIM_CHECK_INTERVAL_S:
                await claim_pending(r, CONSUMER)
                last_claim = now

            resp = await r.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=5,
                block=5000,
            )
            if not resp:
                continue

            for (_, msgs) in resp:
                for (msg_id, fields) in msgs:
                    job_id = fields.get("job_id")
                    if not job_id:
                        await r.xack(STREAM, GROUP, msg_id)
                        continue
                    await process_job(http, r, job_id)
                    await r.xack(STREAM, GROUP, msg_id)
    finally:
        sched_task.cancel()
        try:
            await sched_task
        except Exception:
            pass
        await http.aclose()
        await r.aclose()

if __name__ == "__main__":
    asyncio.run(worker_loop())
