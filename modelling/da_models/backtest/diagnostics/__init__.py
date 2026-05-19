"""Family-specific diagnostic checks (stubs in v1).

Each module here is one diagnostic that only makes sense for one
family -- they live here rather than under the family because the
backtest harness is the consumer. Planned (one file each, added when
the v1 leaderboard is validated):

  - ``supply_stack_fuel_mix.py`` -- dispatched MW per fuel category
    versus actual ``pjm_fuel_mix_hourly``. Reveals whether the merit
    order's marginal-unit selection produces the right *mix*, not just
    the right level.
  - ``supply_stack_marginal_match.py`` -- implied heat rate
    (settled_lmp / gas_price) versus the model's ``marginal_fuel`` /
    ``marginal_heat_rate``. Cross-checks whether the unit the model
    says is on margin agrees with the price-implied regime.
  - ``linear_arx_coef_stability.py`` -- per-HE alpha and top-coefficient
    drift across the backtest window.
"""
