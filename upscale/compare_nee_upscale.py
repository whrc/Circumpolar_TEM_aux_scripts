#!/usr/bin/env python3
"""
Bokeh server app: side-by-side comparison of original 5km NEE (EPSG:6931)
and upscaled 0.5° NEE (EPSG:4326) with year/month navigation and animation.

Usage
-----
    bokeh serve --show visualization/compare_nee_upscale.py

    # override data directory (defaults to merge/upscale/Circumpolar/merged):
    UPSCALE_DIR=/path/to/merged bokeh serve --show visualization/compare_nee_upscale.py
"""

import os
from pathlib import Path
from functools import lru_cache

import numpy as np
import netCDF4 as nc
from bokeh.plotting import figure, curdoc
from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource,
    Slider,
    Select,
    Toggle,
    ColorBar,
    LinearColorMapper,
    Div,
)
from bokeh.palettes import (
    RdBu11, RdYlBu11, PRGn11, BrBG11,
    Viridis256, Plasma256, Inferno256, Turbo256,
    RdYlGn11,
)

# ── Configuration ──────────────────────────────────────────────────────────────

_DEFAULT_DIR = str(
    Path(__file__).resolve().parent.parent
    / "merge" / "upscale" / "Circumpolar" / "merged"
)
DATA_DIR = Path(os.environ.get("UPSCALE_DIR", _DEFAULT_DIR))

YEARS       = list(range(2000, 2025))
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

SUBSAMPLE        = 3     # downsample original 5km grid for display speed
ANIM_INTERVAL_MS = 1200  # ms per frame when animating
PLOT_W, PLOT_H   = 960, 795

# Available palettes: label → (palette_list, is_diverging)
# Diverging palettes are applied symmetrically around zero.
# Sequential palettes use the raw data range.
PALETTES = {
    "RdBu (diverging)":     (list(reversed(RdBu11)),   True),
    "RdYlBu (diverging)":   (list(reversed(RdYlBu11)), True),
    "PRGn (diverging)":     (list(reversed(PRGn11)),    True),
    "BrBG (diverging)":     (list(reversed(BrBG11)),    True),
    "RdYlGn (diverging)":   (list(reversed(RdYlGn11)),  True),
    "Viridis (sequential)": (Viridis256,                False),
    "Plasma (sequential)":  (Plasma256,                 False),
    "Inferno (sequential)": (Inferno256,                False),
    "Turbo (sequential)":   (Turbo256,                  False),
}
DEFAULT_PALETTE = "RdBu (diverging)"

# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_geotransform(ds: nc.Dataset):
    """Return [x0, dx, 0, y_top, 0, dy] from any grid-mapping variable, or None."""
    for vname in ds.variables:
        gt = getattr(ds.variables[vname], "GeoTransform", None)
        if gt:
            return [float(v) for v in str(gt).split()]
    return None


def _mask_fill(arr: np.ndarray, var) -> np.ndarray:
    """Replace fill values and non-finite entries with NaN."""
    fill = getattr(var, "_FillValue", None)
    if fill is not None:
        tol = abs(float(fill)) * 1e-5 if fill != 0 else 1e-10
        arr[np.abs(arr - float(fill)) < tol] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


# ── Data loaders ───────────────────────────────────────────────────────────────

@lru_cache(maxsize=60)
def load_orig(year: int, month: int):
    """
    Return (img, x0, y0, dw, dh) for the original 5km NEE slice.

    img  – 2D float32 array, rows ordered bottom→top (Bokeh convention)
    x0   – left edge of image in EPSG:6931 metres
    y0   – bottom edge of image in EPSG:6931 metres
    dw   – total width  in EPSG:6931 metres
    dh   – total height in EPSG:6931 metres
    """
    path = DATA_DIR / f"NEE_ssp1_2_6_mri_esm2_0_tr_monthly_{year}.nc"
    with nc.Dataset(path) as ds:
        v    = ds.variables["NEE"]
        data = _mask_fill(np.array(v[month, :, :], dtype=np.float32), v)
        ny, nx = data.shape
        gt = _parse_geotransform(ds)

    if gt:
        x0_gt, dx, _, y_top, _, dy = gt   # dy < 0 for north-up rasters
        x0 = x0_gt
        y0 = y_top + ny * dy              # bottom-left y (dy is negative)
        dw = nx * abs(dx)
        dh = ny * abs(dy)
    else:
        x0, y0, dw, dh = 0.0, 0.0, float(nx), float(ny)

    # Flip rows so row-0 = bottom  (Bokeh image plots bottom-to-top)
    img = data[::-1, :]
    # Subsample for browser performance
    img = img[::SUBSAMPLE, ::SUBSAMPLE]
    return img, x0, y0, dw, dh


@lru_cache(maxsize=60)
def load_up(year: int, month: int):
    """
    Return (img, x0, y0, dw, dh) for the upscaled 0.5° NEE slice.

    The image is cropped to lat ≥ 45°N for an Arctic focus.
    """
    path = DATA_DIR / f"NEE_ssp1_2_6_mri_esm2_0_tr_monthly_{year}_upscaled.nc"
    with nc.Dataset(path) as ds:
        v    = ds.variables["NEE"]
        data = _mask_fill(np.array(v[month, :, :], dtype=np.float32), v)
        lat  = np.array(ds.variables["latitude"][:],  dtype=np.float64)
        lon  = np.array(ds.variables["longitude"][:], dtype=np.float64)

    # Ensure latitude is ascending (south → north)
    if lat[0] > lat[-1]:
        data = data[::-1, :]
        lat  = lat[::-1]

    # Crop to Arctic (≥ 45°N)
    mask = lat >= 45.0
    data = data[mask, :]
    lat  = lat[mask]

    res  = abs(lat[1] - lat[0]) if len(lat) > 1 else 0.5
    x0   = float(lon[0]) - res / 2
    y0   = float(lat[0]) - res / 2
    dw   = float(lon[-1]) - float(lon[0]) + res
    dh   = float(lat[-1]) - float(lat[0]) + res
    return data, x0, y0, dw, dh


def compute_clim(year: int, month: int, diverging: bool = True):
    """
    Return (low, high) color limits from combined P2/P98 percentiles.
    Diverging palettes get a symmetric ±lim range; sequential use the raw span.
    """
    d_o = load_orig(year, month)[0]
    d_u = load_up(year, month)[0]
    vals = np.concatenate([
        d_o[np.isfinite(d_o)].ravel(),
        d_u[np.isfinite(d_u)].ravel(),
    ])
    if vals.size == 0:
        return (-30.0, 30.0) if diverging else (0.0, 1.0)
    if diverging:
        lim = max(float(np.percentile(np.abs(vals), 98)), 1.0)
        return -lim, lim
    else:
        lo = float(np.percentile(vals, 2))
        hi = float(np.percentile(vals, 98))
        if hi - lo < 1e-6:
            hi = lo + 1.0
        return lo, hi


# ── Initial data ───────────────────────────────────────────────────────────────

Y0, M0 = YEARS[0], 0
img_o, x0_o, y0_o, dw_o, dh_o = load_orig(Y0, M0)
img_u, x0_u, y0_u, dw_u, dh_u = load_up(Y0, M0)

_init_pal, _init_div = PALETTES[DEFAULT_PALETTE]
vmin, vmax = compute_clim(Y0, M0, diverging=_init_div)

mapper = LinearColorMapper(
    palette=_init_pal,
    low=vmin, high=vmax,
    nan_color="rgba(200,200,200,0.25)",  # semi-transparent grey for masked cells
)

src_o = ColumnDataSource({"image": [img_o], "x": [x0_o], "y": [y0_o],
                          "dw": [dw_o], "dh": [dh_o]})
src_u = ColumnDataSource({"image": [img_u], "x": [x0_u], "y": [y0_u],
                          "dw": [dw_u], "dh": [dh_u]})

# ── Figures ────────────────────────────────────────────────────────────────────

p_o = figure(
    title=f"Original — 5 km  EPSG:6931  |  {Y0}  {MONTH_NAMES[M0]}",
    width=PLOT_W, height=PLOT_H,
    x_axis_label="X  (m, EPSG:6931)",
    y_axis_label="Y  (m, EPSG:6931)",
    tools="pan,wheel_zoom,box_zoom,reset,save",
    active_scroll="wheel_zoom",
)
p_o.image("image", source=src_o, x="x", y="y", dw="dw", dh="dh",
          color_mapper=mapper)

p_u = figure(
    title=f"Upscaled — 0.5°  EPSG:4326  |  {Y0}  {MONTH_NAMES[M0]}",
    width=PLOT_W, height=PLOT_H,
    x_axis_label="Longitude  (°E)",
    y_axis_label="Latitude  (°N)",
    tools="pan,wheel_zoom,box_zoom,reset,save",
    active_scroll="wheel_zoom",
)
p_u.image("image", source=src_u, x="x", y="y", dw="dw", dh="dh",
          color_mapper=mapper)

colorbar = ColorBar(
    color_mapper=mapper,
    label_standoff=10,
    location=(0, 0),
    title="NEE  g C m⁻² month⁻¹",
    width=14,
)
p_u.add_layout(colorbar, "right")

# ── Widgets ────────────────────────────────────────────────────────────────────

year_sl    = Slider(title="Year", start=YEARS[0], end=YEARS[-1],
                    step=1, value=Y0, width=420)
month_sel  = Select(title="Month", value=MONTH_NAMES[M0],
                    options=MONTH_NAMES, width=110)
cmap_sel   = Select(title="Colormap", value=DEFAULT_PALETTE,
                    options=list(PALETTES.keys()), width=200)
anim_btn   = Toggle(label="▶  Animate years", button_type="success", width=175)
speed_sel  = Select(title="Speed", value="Normal",
                    options=["Slow (2 s)", "Normal (1.2 s)", "Fast (0.6 s)"],
                    width=130)
info_div   = Div(
    text="",
    width=220,
    styles={"font-size": "13px", "color": "#555", "margin-top": "18px"},
)

_SPEEDS = {"Slow (2 s)": 2000, "Normal (1.2 s)": 1200, "Fast (0.6 s)": 600}

# ── Callbacks ──────────────────────────────────────────────────────────────────

def _refresh():
    year  = year_sl.value
    month = MONTH_NAMES.index(month_sel.value)
    pal, is_div = PALETTES[cmap_sel.value]

    img_o_, x0, y0, dw, dh     = load_orig(year, month)
    img_u_, x0u, y0u, dwu, dhu = load_up(year, month)

    vlo, vhi = compute_clim(year, month, diverging=is_div)
    mapper.update(palette=pal, low=vlo, high=vhi)

    src_o.data = {"image": [img_o_], "x": [x0],  "y": [y0],
                  "dw": [dw],  "dh": [dh]}
    src_u.data = {"image": [img_u_], "x": [x0u], "y": [y0u],
                  "dw": [dwu], "dh": [dhu]}

    lbl = f"{year}  {month_sel.value}"
    p_o.title.text = f"Original — 5 km  EPSG:6931  |  {lbl}"
    p_u.title.text = f"Upscaled — 0.5°  EPSG:4326  |  {lbl}"
    info_div.text  = (
        f"<b>{year}</b> &nbsp;|&nbsp; <b>{month_sel.value}</b><br>"
        f"Range: [{vlo:.1f}, {vhi:.1f}]"
    )


year_sl.on_change("value",   lambda a, o, n: _refresh())
month_sel.on_change("value", lambda a, o, n: _refresh())
cmap_sel.on_change("value",  lambda a, o, n: _refresh())


_anim_cb_handle = None

def _anim_step():
    nv = year_sl.value + 1
    if nv > YEARS[-1]:
        nv = YEARS[0]
    year_sl.value = nv          # triggers _refresh via on_change


def anim_toggle(active: bool):
    global _anim_cb_handle
    interval = _SPEEDS.get(speed_sel.value, ANIM_INTERVAL_MS)
    if active:
        _anim_cb_handle = curdoc().add_periodic_callback(_anim_step, interval)
        anim_btn.label = "⏹  Stop"
    else:
        if _anim_cb_handle is not None:
            curdoc().remove_periodic_callback(_anim_cb_handle)
            _anim_cb_handle = None
        anim_btn.label = "▶  Animate years"


def speed_changed(attr, old, new):
    """Restart animation at new speed if it is running."""
    global _anim_cb_handle
    if anim_btn.active and _anim_cb_handle is not None:
        curdoc().remove_periodic_callback(_anim_cb_handle)
        _anim_cb_handle = curdoc().add_periodic_callback(
            _anim_step, _SPEEDS.get(new, ANIM_INTERVAL_MS)
        )


anim_btn.on_click(anim_toggle)
speed_sel.on_change("value", speed_changed)

# ── Layout ─────────────────────────────────────────────────────────────────────

header = Div(
    text="""
<div style="padding:4px 0 8px">
  <h2 style="margin:0 0 2px">NEE — Original 5 km vs Upscaled 0.5°</h2>
  <span style="color:#777; font-size:13px">
    SSP1-2.6 &nbsp;|&nbsp; MRI-ESM2-0 &nbsp;|&nbsp; Transient run &nbsp;|&nbsp;
    Circumpolar Arctic &nbsp;|&nbsp; g C m⁻² month⁻¹
  </span>
</div>
""",
    width=1350,
)

controls = row(year_sl, month_sel, cmap_sel, anim_btn, speed_sel, info_div, align="end")
plots    = row(p_o, p_u)

curdoc().add_root(column(header, controls, plots))
curdoc().title = "NEE Comparison"

# Populate info div on first load
_refresh()
