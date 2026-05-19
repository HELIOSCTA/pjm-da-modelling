"""``pjm_hourly`` variant of the linear ARX DA-price forecaster.

Demand block = PJM feeds: RTO supply-demand bundle (load / solar / wind /
net-load forecast) plus sub-zonal load forecasts (MIDATL / WEST / SOUTH).
PJM publishes solar/wind/net-load only system-wide, so the sub-zonal piece
is load-only here; the ``meteo_hourly`` sibling has full sub-zonal
supply-demand from Meteologica.
"""
