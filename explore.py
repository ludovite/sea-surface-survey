import warnings

import duckdb
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    import catppuccin
    mpl.style.use(catppuccin.PALETTE.macchiato.identifier)
except ModuleNotFoundError:
    pass

warnings.filterwarnings("ignore")

con = duckdb.connect("data/sea-survey.duckdb", read_only=True)

# --- Global monthly trends ---

trends = con.execute("""
    SELECT
        MAKE_DATE(sla.year, sla.month, 1) AS year_month,
        AVG(sla.sea_level_anomaly_m)      AS avg_sla_m,
        AVG(sst.sst_celsius)              AS avg_sst_celsius
    FROM raw.sea_level_anomaly AS sla
    JOIN raw.sea_surface_temperature AS sst
        USING (year, month, latitude, longitude)
    GROUP BY sla.year, sla.month
    ORDER BY year_month
""").df()
trends["year_month"] = pd.to_datetime(trends["year_month"])

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True)

x = np.arange(len(trends))
ax1.plot(trends["year_month"], trends["avg_sla_m"],
         linewidth=1.5, color="steelblue", label="Global mean SLA")
ax1.plot(trends["year_month"], np.poly1d(np.polyfit(x, trends["avg_sla_m"], 1))(x),
         "r--", linewidth=1.8, label="Trend")
ax1.set_ylabel("Sea Level Anomaly (m)")
ax1.legend()
ax1.grid(True, alpha=0.3)

ax2.plot(trends["year_month"], trends["avg_sst_celsius"],
         linewidth=1.5, color="coral", label="Global mean SST")
ax2.plot(trends["year_month"], np.poly1d(np.polyfit(x, trends["avg_sst_celsius"], 1))(x),
         "r--", linewidth=1.8, label="Trend")
ax2.set_ylabel("Sea Surface Temperature (°C)")
ax2.set_xlabel("Date")
ax2.legend()
ax2.grid(True, alpha=0.3)

fig.suptitle("Global Mean SLA & SST — 1993–2023", fontsize=14, fontweight="bold")
fig.tight_layout()

# --- Latest month — SLA map ---

latest_year, latest_month = con.execute("""
    SELECT MAX(year),
           MAX(month) FILTER (WHERE year = (SELECT MAX(year) FROM raw.sea_level_anomaly))
    FROM raw.sea_level_anomaly
""").fetchone()

sla_grid = con.execute(f"""
    SELECT latitude, longitude, sea_level_anomaly_m
    FROM raw.sea_level_anomaly
    WHERE year = {latest_year} AND month = {latest_month}
""").df()

grid = sla_grid.pivot(index="latitude", columns="longitude", values="sea_level_anomaly_m").values
lon = np.sort(sla_grid["longitude"].unique())
lat = np.sort(sla_grid["latitude"].unique())
abs_max = max(abs(np.nanpercentile(grid, 2)), abs(np.nanpercentile(grid, 98)))

fig2, ax = plt.subplots(figsize=(14, 7))
im = ax.pcolormesh(lon, lat, grid, cmap="RdBu_r", vmin=-abs_max, vmax=abs_max, shading="auto")
fig2.colorbar(im, ax=ax, label="Sea Level Anomaly (m)")
ax.set_title(f"Sea Level Anomaly — {latest_year}-{latest_month:02d}", fontsize=14, fontweight="bold")
ax.set_xlabel("Longitude (°)")
ax.set_ylabel("Latitude (°)")
fig2.tight_layout()

# --- Latest month — SST map ---

sst_grid = con.execute(f"""
    SELECT latitude, longitude, sst_celsius
    FROM raw.sea_surface_temperature
    WHERE year = {latest_year} AND month = {latest_month}
""").df()

grid3 = sst_grid.pivot(index="latitude", columns="longitude", values="sst_celsius").values
lon3 = np.sort(sst_grid["longitude"].unique())
lat3 = np.sort(sst_grid["latitude"].unique())
vmin, vmax = np.nanpercentile(grid3, [2, 98])

fig3, ax3 = plt.subplots(figsize=(14, 7))
im3 = ax3.pcolormesh(lon3, lat3, grid3, cmap="RdYlBu_r", vmin=vmin, vmax=vmax, shading="auto")
fig3.colorbar(im3, ax=ax3, label="Sea Surface Temperature (°C)")
ax3.set_title(f"Sea Surface Temperature — {latest_year}-{latest_month:02d}", fontsize=14, fontweight="bold")
ax3.set_xlabel("Longitude (°)")
ax3.set_ylabel("Latitude (°)")
fig3.tight_layout()

# --- Latitude zone breakdown ---

zones = con.execute("""
    SELECT
        MAKE_DATE(sla.year, sla.month, 1) AS year_month,
        CASE
            WHEN sla.latitude >=  66.5 THEN 'Arctic'
            WHEN sla.latitude >=  23.5 THEN 'N. Temperate'
            WHEN sla.latitude >= -23.5 THEN 'Tropical'
            WHEN sla.latitude >= -66.5 THEN 'S. Temperate'
            ELSE                             'Antarctic'
        END AS latitude_zone,
        AVG(sla.sea_level_anomaly_m) AS avg_sla_m,
        AVG(sst.sst_celsius)         AS avg_sst_celsius
    FROM raw.sea_level_anomaly AS sla
    JOIN raw.sea_surface_temperature AS sst
        USING (year, month, latitude, longitude)
    GROUP BY sla.year, sla.month, latitude_zone
    ORDER BY year_month, latitude_zone
""").df()
zones["year_month"] = pd.to_datetime(zones["year_month"])

fig4, (ax4, ax5) = plt.subplots(2, 1, figsize=(13, 9), sharex=True)

for zone in ["Arctic", "N. Temperate", "Tropical", "S. Temperate", "Antarctic"]:
    z = zones[zones["latitude_zone"] == zone]
    ax4.plot(z["year_month"], z["avg_sla_m"], linewidth=1.2, label=zone)
    ax5.plot(z["year_month"], z["avg_sst_celsius"], linewidth=1.2, label=zone)

ax4.set_ylabel("Sea Level Anomaly (m)")
ax4.legend(fontsize=8)
ax4.grid(True, alpha=0.3)

ax5.set_ylabel("Sea Surface Temperature (°C)")
ax5.set_xlabel("Date")
ax5.legend(fontsize=8)
ax5.grid(True, alpha=0.3)

fig4.suptitle("SLA & SST by Latitude Zone — 1993–2023", fontsize=14, fontweight="bold")
fig4.tight_layout()

plt.show()
