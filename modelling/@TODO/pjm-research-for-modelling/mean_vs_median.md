# Forecast (Mean) vs P50 (Median) in Like-Day Analog Forecasts

The "Forecast" row and the P50 row in the output of `forecast.py` are computed
from the **same set of weighted analog next-day prices** but answer different
questions. The Forecast can sit anywhere relative to the quantile bands —
including above P62.5 — whenever the analog distribution is skewed.

## How each statistic is built (per hour)

```
                Analog next-day LMPs at hour H
                (one value per analog, weighted)
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   weighted MEAN        weighted MEDIAN       weighted QUANTILE(q)
   np.average(...)      weighted_quantile     weighted_quantile
   forecast.py:371      (..., 0.50)           (..., q)
        │                     │                     │
        ▼                     ▼                     ▼
   "Forecast" row         "P50" row            "P25"…"P99" rows
```

The Forecast row is **not** constrained to lie between adjacent quantile rows.
It is spliced into the quantile table after P50 (`forecast.py:454-460`) only
for visual comparison.

## Right-skewed analog distribution → mean > median

For an evening peak hour like HE20, the analog prices typically look like:

```
weighted mass
   │
   │ ████
   │ ████
   │ ████ ███
   │ ████ ███ ██
   │ ████ ███ ██ ██
   │ ████ ███ ██ ██ ██               ▲                 ▲
   │ ████ ███ ██ ██ ██ █             ▲ rare extreme    ▲
   │ ████ ███ ██ ██ ██ █ █  ▁  ▁  ▁  ▲ analogs         ▲
   └─────────────────────────────────────────────────────►   $/MWh
       50   70   85  100 120  …  160      190
                  ▲    ▲      ▲
                  │    │      └── Forecast (MEAN)  ≈ 103.8
                  │    └────────── P62.5            ≈  96.8
                  └─────────────── P50 (MEDIAN)     ≈  85.4
```

* The **median** depends only on *how many* analogs sit above vs below it.
  The size of the tail values does not matter.
* The **mean** is pulled toward the tail by the *magnitude* of those rare
  high-price analogs. A few outliers at $160-$190 lift the mean far more
  than they lift the median.

That gap is exactly what you see in the screenshot for HE20:

| stat       | value  |
|------------|--------|
| P50        |  85.4  |
| P62.5      |  96.8  |
| **Forecast (mean)** | **103.8** |
| P75        | 122.3  |
| P95        | 163.6  |
| P99        | 192.2  |

The mean has overshot P62.5 because ~15-20% of the weighted analog mass sits
in a long upper tail.

## When does this happen?

```
Symmetric distribution                Right-skewed distribution
(off-peak hours, mild weather)        (peak hours, scarcity risk)

      ▆▆▆▆                                ████
    ▆▆████▆▆                              ████ ▇▇
  ▂▆████████▆▂                            ████ ▇▇ ▅▅ ▃▃ ▁▁ ▁  ▁
─────┼──┼──┼─────                       ──┼───┼───┼───┼───┼──┼──
   P25 P50 P75                            P50  Mean   P75      P99
        │
   Mean ≈ Median                       Mean > Median (often > P60)
```

Hours where Forecast ≈ P50 → analog distribution is roughly symmetric.
Hours where Forecast >> P50 → heavy right tail (a few hot/scarce analogs).

## Which to use?

| Use case                                                         | Pick      |
|------------------------------------------------------------------|-----------|
| Robust central estimate, ignore extreme analogs                  | **P50**   |
| Expected $/MWh for revenue / settlement math (respects the tail) | **Forecast (mean)** |
| Risk view (what's the bad tail look like?)                       | **P75 / P90 / P95** |

The two diverge precisely on the hours where the choice matters most — when
the analog set carries real upside risk.
