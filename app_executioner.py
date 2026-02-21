# ===================================================================
# APEX CCXT ASYNC EXECUTIONER NODE v2026.3 (Port 8002)
# Hardened: dynamic equity sizing, bracket SL/TP, rollback on partial failure
# ===================================================================

from __future__ import annotations

import os
from typing import Literal, Optional

import ccxt.async_support as ccxt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from apex_common.config import ExecutionerConfig
from apex_common.security import validate_api_credentials
from apex_common.logging import get_logger
from apex_common.security import check_env_file_permissions
from apex_common.metrics import instrument_app

load_dotenv()
log = get_logger("executioner")
_ok, _msg = check_env_file_permissions(".env")
if not _ok:
    log.warning(_msg)
else:
    log.info(_msg)
cfg = ExecutionerConfig()

\1instrument_app(app)

Side = Literal["buy", "sell"]
Venue = Literal["binance", "bybit", "okx"]

class StrikePayload(BaseModel):
    symbol: str = Field(..., description="Unified symbol per ccxt, ex: BTC/USDT:USDT")
    side: Side
    venue: Venue

    # sizing: choose ONE
    size_usd: Optional[float] = Field(None, gt=0, description="Notional size in USD")
    risk_pct: Optional[float] = Field(None, gt=0, le=0.05, description="Fraction of free USDT equity to allocate")

    # protection
    sl_pct: float = Field(default=0.015, gt=0, lt=0.25)
    tp_pct: float = Field(default=0.045, gt=0, lt=1.00)

    # safety
    reduce_only_brackets: bool = True


def _validate_creds(venue_l: str):
    api_key, secret = _validate_creds(venue_l)
    ok, msg = validate_api_credentials(api_key, secret)
    if not ok:
        raise HTTPException(status_code=400, detail=f"[{venue_l}] {msg}")
    return api_key, secret


def _get_exchange(venue: str):
    venue_l = venue.lower()

    if venue_l == "binance":
        ex_cls = ccxt.binance
        opts = {"defaultType": "swap"}
        params = {}
    elif venue_l == "bybit":
        ex_cls = ccxt.bybit
        opts = {"defaultType": "swap"}
        params = {}
    elif venue_l == "okx":
        ex_cls = ccxt.okx
        opts = {"defaultType": "swap"}
        params = {"password": os.getenv("OKX_PASSWORD", "")}
    else:
        raise ValueError("venue inválida")

    api_key, secret = _validate_creds(venue_l)

    exchange = ex_cls({
        "apiKey": api_key,
        "secret": secret,
        "enableRateLimit": True,
        "options": opts,
        **params
    })

    if cfg.use_testnet:
        try:
            exchange.set_sandbox_mode(True)
        except Exception:
            pass
    return exchange

async def _fetch_free_usdt(exchange) -> float:
    balance = await exchange.fetch_balance()
    # ccxt differences: sometimes 'USDT' in balance, sometimes 'total' etc.
    if "USDT" in balance:
        usdt = balance["USDT"]
        if isinstance(usdt, dict):
            return float(usdt.get("free", 0.0) or 0.0)
    # fallback: try 'free' dict
    free = balance.get("free", {})
    if isinstance(free, dict) and "USDT" in free:
        return float(free.get("USDT", 0.0) or 0.0)
    return 0.0


def _check_market_limits(exchange, symbol: str, amount: float, notional_usd: float):
    m = exchange.market(symbol)
    limits = (m or {}).get("limits", {}) or {}
    amt_min = ((limits.get("amount") or {}).get("min"))
    cost_min = ((limits.get("cost") or {}).get("min"))
    if amt_min is not None and amount < float(amt_min):
        raise HTTPException(status_code=400, detail=f"Amount {amount} abaixo do mínimo ({amt_min}) para {symbol}.")
    if cost_min is not None and notional_usd < float(cost_min):
        raise HTTPException(status_code=400, detail=f"Notional {notional_usd:.2f} abaixo do mínimo ({cost_min}) para {symbol}.")


def _validate_payload(p: StrikePayload):
    if (p.size_usd is None) == (p.risk_pct is None):
        raise HTTPException(status_code=400, detail="Envie exatamente um: size_usd OU risk_pct.")
    if p.size_usd is not None and p.size_usd > cfg.max_notional_usd:
        raise HTTPException(status_code=400, detail=f"size_usd acima do limite de segurança ({cfg.max_notional_usd}).")

@app.get("/health")
async def health():
    return {"status": "ok", "service": "executioner", "version": app.version, "testnet": cfg.use_testnet}

@app.get("/get_equity/{venue}")
async def get_equity(venue: Venue):
    exchange = None
    try:
        exchange = _get_exchange(venue)
        free_usdt = await _fetch_free_usdt(exchange)
        return {"venue": venue.upper(), "free_usdt": free_usdt, "status": "SUCCESS", "testnet": cfg.use_testnet}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if exchange:
            await exchange.close()

@app.post("/execute_strike")
async def execute_strike(payload: StrikePayload):
    _validate_payload(payload)
    exchange = None
    entry_order = None
    sl_order = None
    tp_order = None

    try:
        exchange = _get_exchange(payload.venue)
        await exchange.load_markets()

        # Resolve notional sizing
        if payload.risk_pct is not None:
            free_usdt = await _fetch_free_usdt(exchange)
            size_usd = float(free_usdt) * float(payload.risk_pct)
        else:
            size_usd = float(payload.size_usd)

        if size_usd <= 0:
            raise HTTPException(status_code=400, detail="Equity insuficiente para sizing.")

        if size_usd > cfg.max_notional_usd:
            raise HTTPException(status_code=400, detail=f"Tamanho calculado excede limite ({cfg.max_notional_usd}).")

        # Fetch price
        ticker = await exchange.fetch_ticker(payload.symbol)
        current_price = float(ticker.get("last") or ticker.get("mark") or ticker.get("close") or 0.0)
        if current_price <= 0:
            raise HTTPException(status_code=502, detail="Não foi possível obter preço atual.")

        raw_amount = size_usd / current_price
        amount = float(exchange.amount_to_precision(payload.symbol, raw_amount))

        # Entry
        entry_order = await exchange.create_order(payload.symbol, "market", payload.side, amount)
        fill_price = float(entry_order.get("average") or entry_order.get("price") or current_price)

        # Brackets: unify stop/tp sides
        close_side = "sell" if payload.side == "buy" else "buy"

        if payload.side == "buy":
            sl_price = fill_price * (1 - payload.sl_pct)
            tp_price = fill_price * (1 + payload.tp_pct)
        else:
            sl_price = fill_price * (1 + payload.sl_pct)
            tp_price = fill_price * (1 - payload.tp_pct)

        sl_price = float(exchange.price_to_precision(payload.symbol, sl_price))
        tp_price = float(exchange.price_to_precision(payload.symbol, tp_price))

        params = {}
        if payload.reduce_only_brackets:
            params["reduceOnly"] = True

        # Create SL/TP (best-effort across venues). If any fails, rollback.
        # ccxt unified types:
        # - 'stop_market' with 'stopPrice'
        # - 'take_profit_market' with 'stopPrice'

        protection = {"sl_created": False, "tp_created": False, "sl_error": None, "tp_error": None}

        try:
            sl_order = await exchange.create_order(
                payload.symbol, "stop_market", close_side, amount, None, {"stopPrice": sl_price, **params}
            )
            protection["sl_created"] = True
        except Exception as e:
            protection["sl_error"] = str(e)

        try:
            tp_order = await exchange.create_order(
                payload.symbol, "take_profit_market", close_side, amount, None, {"stopPrice": tp_price, **params}
            )
            protection["tp_created"] = True
        except Exception as e:
            protection["tp_error"] = str(e)

        if not (protection["sl_created"] and protection["tp_created"]):
            # best-effort rollback of any created protection orders
            try:
                if sl_order and sl_order.get("id"):
                    await exchange.cancel_order(sl_order["id"], payload.symbol)
            except Exception:
                pass
            try:
                if tp_order and tp_order.get("id"):
                    await exchange.cancel_order(tp_order["id"], payload.symbol)
            except Exception:
                pass

            return {
                "status": "PARTIAL_PROTECTION",
                "venue": payload.venue.upper(),
                "symbol": payload.symbol,
                "notional_usd": round(size_usd, 2),
                "amount": amount,
                "entry_fill_price": fill_price,
                "stop_loss_set": sl_price,
                "take_profit_set": tp_price,
                "testnet": cfg.use_testnet,
                "warning": "Entry executada, mas proteção (SL/TP) falhou. Verifique e feche/hedge manualmente.",
                "protection": protection,
                "orders": {
                    "entry": entry_order.get("id"),
                    "sl": sl_order.get("id") if sl_order else None,
                    "tp": tp_order.get("id") if tp_order else None,
                },
            }

        return {

            "status": "SUCCESS",
            "venue": payload.venue.upper(),
            "symbol": payload.symbol,
            "notional_usd": round(size_usd, 2),
            "amount": amount,
            "entry_fill_price": fill_price,
            "stop_loss_set": sl_price,
            "take_profit_set": tp_price,
            "testnet": cfg.use_testnet,
            "orders": {
                "entry": entry_order.get("id"),
                "sl": sl_order.get("id"),
                "tp": tp_order.get("id"),
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Execution failed: {e}")
        # rollback best-effort
        try:
            if exchange and sl_order and sl_order.get("id"):
                await exchange.cancel_order(sl_order["id"], payload.symbol)
        except Exception:
            pass
        try:
            if exchange and tp_order and tp_order.get("id"):
                await exchange.cancel_order(tp_order["id"], payload.symbol)
        except Exception:
            pass
        # NOTE: rolling back market entry is non-trivial; user may need manual close.
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if exchange:
            await exchange.close()


@app.get("/get_open_orders/{venue}/{symbol}")
async def get_open_orders(venue: Venue, symbol: str):
    exchange = None
    try:
        exchange = _get_exchange(venue)
        await exchange.load_markets()
        orders = await exchange.fetch_open_orders(symbol)
        return {"venue": venue.upper(), "symbol": symbol, "open_orders": orders, "status": "SUCCESS", "testnet": cfg.use_testnet}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if exchange:
            await exchange.close()

@app.get("/get_order/{venue}/{symbol}/{order_id}")
async def get_order(venue: Venue, symbol: str, order_id: str):
    exchange = None
    try:
        exchange = _get_exchange(venue)
        await exchange.load_markets()
        order = await exchange.fetch_order(order_id, symbol)
        return {"venue": venue.upper(), "symbol": symbol, "order": order, "status": "SUCCESS", "testnet": cfg.use_testnet}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if exchange:
            await exchange.close()
