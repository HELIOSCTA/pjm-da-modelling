"""``meteo_hourly`` variant of the linear ARX DA-price forecaster.

Demand block = Meteologica regional supply-demand: load / solar / wind /
net-load forecast for RTO **and** the three sub-zones (MIDATL / WEST /
SOUTH) -- the full sub-zonal renewable + net-load detail PJM's own feeds
don't publish. Tests the ``rto_vs_regional_load.md`` hypothesis that
sub-zonal demand is a cleaner Western-Hub signal than the RTO aggregate.
Everything else (weather, gas, outages, calendar, estimator, bands) is
shared with the ``pjm_hourly`` sibling.
"""
