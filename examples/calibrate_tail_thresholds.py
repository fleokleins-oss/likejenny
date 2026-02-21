import csv
import math
import sys
from typing import List

import numpy as np

def hill_tail_index(returns: List[float], k: int) -> float:
    if len(returns) < max(10, 2*k):
        return 2.0
    abs_rets = np.abs(np.array(returns, dtype=float))
    abs_rets.sort()
    tail = abs_rets[-k:]
    threshold = abs_rets[-k-1]
    if threshold <= 1e-12:
        return 2.0
    hill = float(np.mean(np.log(np.maximum(tail, 1e-12) / threshold)))
    return 1.0 / hill if hill > 0 else 2.0

def load_returns_csv(path: str, col: str = "return") -> List[float]:
    out = []
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if col in row and row[col] not in (None, ""):
                out.append(float(row[col]))
    return out

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python calibrate_tail_thresholds.py returns.csv [col=return] [k=20]")
        sys.exit(1)
    path = sys.argv[1]
    col = sys.argv[2] if len(sys.argv) > 2 else "return"
    k = int(sys.argv[3]) if len(sys.argv) > 3 else 20

    rets = load_returns_csv(path, col=col)
    alpha = hill_tail_index(rets, k=k)

    # Recommend warn/kill heuristics
    warn = max(1.2, min(2.2, alpha * 1.15))
    kill = max(1.05, min(1.8, alpha * 0.90))

    print(f"n={len(rets)} k={k} hill_alpha={alpha:.3f}")
    print(f"suggested: BRAIN_TAIL_ALPHA_WARN={warn:.2f}")
    print(f"suggested: BRAIN_TAIL_ALPHA_KILL={kill:.2f}")
