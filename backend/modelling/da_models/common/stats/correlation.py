"""NaN-safe correlation primitives — Pearson, Spearman, and distance.

All three helpers accept two 1-D numeric arrays and return ``float |
None``. ``None`` signals "correlation undefined" (fewer than 3 finite
pairs or zero variance on either side); callers display these as
``n/a``.

Spearman is implemented as Pearson on average-method ranks — robust to
single-hour LMP spikes that routinely dominate Pearson on short
(n=24) DA price profiles.

Distance correlation here is the Mantel-style variant: Pearson of
pairwise ``|x_i - x_j|`` vs ``|y_i - y_j|``. It captures non-monotone
"similarity -> similarity" structure that variable Pearson and
Spearman miss — exactly the structure a like-day KNN forecaster
exploits.

We deliberately avoid scipy to keep the common-package dep surface
limited to numpy + pandas.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def pearson(x: np.ndarray, y: np.ndarray) -> float | None:
    """NaN-safe Pearson correlation. Returns None if <3 finite pairs or
    either side has zero variance."""
    mask = np.isfinite(x) & np.isfinite(y)
    if mask.sum() < 3:
        return None
    xv, yv = x[mask], y[mask]
    if np.std(xv) == 0 or np.std(yv) == 0:
        return None
    return float(np.corrcoef(xv, yv)[0, 1])


def spearman(x: np.ndarray, y: np.ndarray) -> float | None:
    """NaN-safe Spearman = Pearson on average-method ranks.

    Robust to single-hour LMP spikes (HE17 $200+ outliers) that
    routinely dominate the 24-point Pearson on DA price data.
    """
    mask = np.isfinite(x) & np.isfinite(y)
    if mask.sum() < 3:
        return None
    xv, yv = x[mask], y[mask]
    if np.std(xv) == 0 or np.std(yv) == 0:
        return None
    xr = pd.Series(xv).rank(method="average").to_numpy()
    yr = pd.Series(yv).rank(method="average").to_numpy()
    return float(np.corrcoef(xr, yr)[0, 1])


def distance_correlation(x: np.ndarray, y: np.ndarray) -> float | None:
    """Mantel-style distance correlation: Pearson of pairwise
    |x_i - x_j| vs |y_i - y_j|. Captures non-monotone
    'similarity -> similarity' relationships that variable Pearson and
    Spearman miss (e.g., V-shaped feature-vs-label patterns where
    feature=50 -> label=100, feature=100 -> label=20,
    feature=150 -> label=100; Pearson sees no signal but distance
    correlation does, and a KNN forecaster can exploit it).

    NaN-safe: drops indices where x or y is non-finite before forming
    pairs. Returns None if fewer than 3 finite values remain or either
    pair-distance vector has zero variance.

    Implementation: outer subtraction gives an n x n distance matrix;
    upper triangle (k=1) avoids double-counting and the zero diagonal.
    No scipy.
    """
    mask = np.isfinite(x) & np.isfinite(y)
    if mask.sum() < 3:
        return None
    xv, yv = x[mask], y[mask]
    n = xv.size
    iu, ju = np.triu_indices(n, k=1)
    dx = np.abs(xv[iu] - xv[ju])
    dy = np.abs(yv[iu] - yv[ju])
    if np.std(dx) == 0 or np.std(dy) == 0:
        return None
    return float(np.corrcoef(dx, dy)[0, 1])
