# Polyscalper — OpenClaw-style microstructure (paper-first)

This is an educational, **paper-first** Polymarket market-making loop inspired by OpenClaw-style cancel/replace bots.

What’s included (the “next-level” upgrades):
- **Batch cancel/replace** (REST `DELETE /orders`, `POST /orders`) to reduce API load.  
- **User-channel reconciliation** (WSS `/ws/user`) to update inventory from real fills and keep open-orders in sync.  
- **Per-market queue progress heuristic** using full-depth **book snapshots** and **price level deltas** (WSS `/ws/market`).  
- **Latency-aware quote shading + dynamic spread** (adverse selection proxy from microprice, volatility, and short-horizon orderflow).

Econophysics regime gate (optional):
- Hurst exponent, entropy on returns, Largest Lyapunov Exponent, and a lightweight discrete transfer entropy proxy.

## Run (paper)

```bash
python -m polyscalper.main --env .env --assets <TOKEN_ID1,TOKEN_ID2>
```

Tip: Put your token ids in `.env` as `POLY_ASSET_IDS=...`.

## Live mode

Live trading is **disabled in this patch** unless you implement order signing (EIP-712) and wire `trader.post_orders()` with signed payloads.
You must set `LIVE_MODE=true` in `.env` **and** pass `--live`.

## Security notes
- Do not use the user websocket from client-side/mobile code (docs warn against exposing API credentials).
- Treat all parameters and estimators as research prototypes.

### Live order signing

If you want this loop to actually place orders, set `USE_PY_CLOB_CLIENT=true` and install `py-clob-client`. The engine will use it for EIP-712 order signing and still uses the REST batch endpoints for posting/canceling.


## Inventory manager layer (new)

This build adds a conservative **market-maker inventory manager**:

- **YES/NO auto-neutralization** (optional): provide a pair map and the bot will skew quotes to keep
  exposure near neutral (proxy: `YES_shares - NO_shares`).
- **Contagion-aware risk** (optional): a rolling **transfer-entropy (TE)** estimate across token mid-price
  returns reduces per-token max inventory during high-contagion regimes.
- **Unwind-only mode**: if inventory breaches a hard band (fraction of max inventory), the bot only quotes
  the side that reduces inventory.

### Pair map input

You can provide pair info via `.env`:

```
POLY_PAIRS=conditionId:yesTokenId:noTokenId,conditionId:yesTokenId:noTokenId
```

or (no condition id, synthetic group id; fill reconciliation may be less useful):

```
POLY_PAIRS=yesTokenId:noTokenId,yesTokenId:noTokenId
```

Run:

```bash
python -m polyscalper.main --assets <TOKENS...> --pairs "$POLY_PAIRS"
```

## Cross-hedge (paper-first)

Optional, **OFF by default** (`HEDGE_ENABLED=false`):
- Polls Binance spot prices (REST) for `HEDGE_SYMBOLS`
- Estimates a rolling beta between token mid returns and underlying spot returns
- Maintains a **paper hedge ledger** to offset inventory notional risk

This is a proxy; do not rely on it for real-world risk management.
