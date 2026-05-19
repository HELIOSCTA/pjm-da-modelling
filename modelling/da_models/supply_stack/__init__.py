"""Structural merit-order (supply-stack) DA-LMP forecaster for PJM.

Physically interpretable, forward-looking: takes hourly load + renewable
forecasts + outages + gas prices, builds a cost-ordered supply stack from
the PJM fleet, finds the marginal generator at each hour, and sets the
clearing price = marginal variable cost + congestion adder + a
reserve-utilization scarcity adder. Extrapolates by construction -- a
117 GW net-load day just dispatches further up the convex part of the
curve -- which is why it complements the data-driven models (like-day,
linear ARX) precisely during regime shifts they under-react to.

Status: v1 scaffold per
``modelling/@TODO/pjm-research-for-modelling/supply_stack_model.md``.
Fleet comes from the Energy Aspects monthly installed-capacity feed
(``load_installed_capacity``) split into technology blocks -- the EIA-860
/ PUDL upgrade with per-unit heat rates is the documented next step.
Coal/oil prices are config constants. Research / standalone: ``run(...)``
computes, prints, returns a dict; nothing here writes Postgres.
"""
