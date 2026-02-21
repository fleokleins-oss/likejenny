# ===================================================================
# APEX ANTI-RUG ML ENGINE v2026.4 (Port 8003)
# Expanded features + model versioning + optional admin retrain
# ===================================================================

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from sklearn.ensemble import RandomForestClassifier

from apex_common.logging import get_logger
from apex_common.security import check_env_file_permissions
from apex_common.metrics import instrument_app

load_dotenv()
log = get_logger("anti_rug")
_ok, _msg = check_env_file_permissions(".env")
if not _ok:
    log.warning(_msg)
else:
    log.info(_msg)

app = FastAPI(title="Apex Anti-Rug ML Engine", version="2026.4")
instrument_app(app)

DATA_DIR = Path(os.getenv("ANTI_RUG_DATA_DIR", ".")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILE = DATA_DIR / "anti_rug_model_v2.pkl"
TRAINING_CSV = os.getenv("ANTI_RUG_TRAINING_CSV", "").strip()
ADMIN_TOKEN = os.getenv("ANTI_RUG_ADMIN_TOKEN", "").strip()

FEATURES = [
    "liquidity_usd",
    "top_holder_pct",
    "dev_wallet_tx_count",
    "age_hours",
    "volume_24h",
    "holders_count",
    "buy_tax_pct",
    "sell_tax_pct",
]

def _to_row(d: dict) -> list[float]:
    return [float(d.get(k, 0.0) or 0.0) for k in FEATURES]

def load_labeled_csv(path: str) -> tuple[np.ndarray, np.ndarray]:
    """CSV must include all FEATURES columns and a label column 'is_rug' (0/1)."""
    X, y = [], []
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if "is_rug" not in row:
                raise ValueError("CSV precisa de coluna 'is_rug' (0/1).")
            X.append(_to_row(row))
            y.append(int(float(row["is_rug"])))
    return np.array(X, dtype=float), np.array(y, dtype=int)

def train_synthetic(model_path: Path) -> RandomForestClassifier:
    log.info("🧠 [ML] Training synthetic Anti-Rug model (v2)...")
    np.random.seed(42)
    n = 1200

    # Rug-like distributions
    rugs_X = np.column_stack([
        np.random.uniform(500, 15000, n),      # liquidity_usd
        np.random.uniform(35, 97, n),          # top_holder_pct
        np.random.randint(10, 250, n),         # dev_wallet_tx_count
        np.random.uniform(0.1, 24, n),         # age_hours
        np.random.uniform(1000, 90000, n),     # volume_24h
        np.random.uniform(50, 4000, n),        # holders_count
        np.random.uniform(5, 35, n),           # buy_tax_pct
        np.random.uniform(5, 45, n),           # sell_tax_pct
    ])
    rugs_y = np.ones(n)

    # Non-rug distributions
    succ_X = np.column_stack([
        np.random.uniform(50000, 1200000, n),
        np.random.uniform(3, 30, n),
        np.random.randint(0, 20, n),
        np.random.uniform(24, 5000, n),
        np.random.uniform(100000, 20000000, n),
        np.random.uniform(5000, 500000, n),
        np.random.uniform(0, 8, n),
        np.random.uniform(0, 10, n),
    ])
    succ_y = np.zeros(n)

    X = np.vstack([rugs_X, succ_X])
    y = np.concatenate([rugs_y, succ_y])

    clf = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X, y)
    joblib.dump(clf, model_path)
    log.info(f"✅ [ML] Model saved: {model_path}")
    return clf

def train_from_csv(model_path: Path, csv_path: str) -> RandomForestClassifier:
    log.info(f"🧠 [ML] Training from labeled CSV: {csv_path}")
    X, y = load_labeled_csv(csv_path)
    clf = RandomForestClassifier(
        n_estimators=500,
        max_depth=10,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X, y)
    joblib.dump(clf, model_path)
    log.info(f"✅ [ML] Model saved: {model_path}")
    return clf

def load_or_train() -> RandomForestClassifier:
    if MODEL_FILE.exists():
        try:
            m = joblib.load(MODEL_FILE)
            log.info(f"[ML] Loaded model: {MODEL_FILE}")
            return m
        except Exception as e:
            log.warning(f"[ML] Failed loading model, retraining: {e}")

    if TRAINING_CSV:
        try:
            return train_from_csv(MODEL_FILE, TRAINING_CSV)
        except Exception as e:
            log.warning(f"[ML] CSV training failed, fallback to synthetic: {e}")

    return train_synthetic(MODEL_FILE)

predator_model: RandomForestClassifier = load_or_train()

class TokenMetrics(BaseModel):
    liquidity_usd: float = Field(..., ge=0)
    top_holder_pct: float = Field(..., ge=0, le=100)
    dev_wallet_tx_count: int = Field(..., ge=0)
    age_hours: float = Field(..., ge=0)
    volume_24h: float = Field(..., ge=0)

    # Expanded v2 signals (optional)
    holders_count: float = Field(0.0, ge=0)
    buy_tax_pct: float = Field(0.0, ge=0, le=100)
    sell_tax_pct: float = Field(0.0, ge=0, le=100)

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "anti_rug",
        "version": app.version,
        "model": str(MODEL_FILE),
        "features": FEATURES,
        "csv_training": bool(TRAINING_CSV),
    }

@app.post("/analyze_token")
async def analyze_token(metrics: TokenMetrics):
    row = np.array([[
        metrics.liquidity_usd,
        metrics.top_holder_pct,
        metrics.dev_wallet_tx_count,
        metrics.age_hours,
        metrics.volume_24h,
        metrics.holders_count,
        metrics.buy_tax_pct,
        metrics.sell_tax_pct,
    ]], dtype=float)

    rug_prob = float(predator_model.predict_proba(row)[0][1])
    status = "REJEITADO" if rug_prob > 0.40 else "APROVADO"

    return {
        "status": status,
        "rug_probability_pct": round(rug_prob * 100, 2),
        "edge_directive": "Risco de honeypot/rug detectado." if status == "REJEITADO" else "Estrutura on-chain limpa.",
    }

@app.post("/admin/retrain")
async def admin_retrain(x_admin_token: Optional[str] = Header(default=None)):
    """Retreina o modelo (se ANTI_RUG_ADMIN_TOKEN estiver setado).
    Use apenas em ambiente controlado.
    """
    global predator_model
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Admin retrain desabilitado (ANTI_RUG_ADMIN_TOKEN vazio).")
    if (x_admin_token or "") != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Token admin inválido.")

    predator_model = load_or_train()
    return {"status": "retrained", "model": str(MODEL_FILE), "features": FEATURES}
