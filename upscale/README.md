# NEE Upscaling & Comparison — Usage Guide

This folder contains the workflow for regridding Circumpolar TEM NEE output from
the native **~4 km EPSG:6931** (EASE-Grid 2.0 North, Lambert Azimuthal Equal Area)
projection to a **0.5° EPSG:4326** (WGS84 lat/lon) grid, and for visually
comparing the two resolutions side by side.

---

## Files in this folder

| File | Description |
|------|-------------|
| `upscale_nee_05deg.py` | Main upscaling script (Python + NCO + GDAL) |
| `upscale_05deg.sh` | Equivalent Bash version (for HPC / cluster use) |
| `sample_dataset.nc` | Reference 0.5° lat/lon grid — defines output extent and resolution |
| `aoi_5k_buff_6931_2_0.nc` | Circumpolar AOI mask in EPSG:6931 (.5deg, NetCDF) |
| `Circumpolar/merged/` | Input directory — one `.nc` per year; `*_upscaled.nc` outputs land here too |

---

## Prerequisites

All tools must come from the **same environment** to avoid library conflicts.
A self-contained conda-forge environment is required — the macOS system Homebrew
NCO install frequently has broken `libnetcdf` linkage after OS or Homebrew upgrades
and will not work.

### Create the environment (first time only)

```bash
conda create -n nee_upscale -y -c conda-forge \
    python=3.11 nco gdal libgdal-hdf5 libgdal-netcdf netcdf4 numpy bokeh
```

> **`libgdal-netcdf` is required.**  Without it, `gdalinfo` cannot read
> `sample_dataset.nc` with geographic metadata (Origin / Pixel Size), causing the
> script to fall back to a broken coordinate path.  `libgdal-hdf5` is needed to
> open the source NEE files.

### Activate before each session

```bash
conda activate nee_upscale
```

### Required command-line tools

`ncks`, `ncrename`, `ncap2`, `ncatted`, `ncrcat`, `gdalinfo`, `gdalwarp`, `ncdump`

---

## Step 1 — Upscale NEE files with `upscale_nee_05deg.py`

### Basic usage (run from the repo root)

```bash
cd Circumpolar_TEM_aux_scripts
python upscale/upscale_nee_05deg.py \
  --in-dir  upscale/Circumpolar/merged \
  --grid    upscale/sample_dataset.nc \
  --mask    upscale/aoi_5k_buff_6931_2_0.nc \
  --out-dir upscale/Circumpolar/merged
```

If the environment is not activated, prefix with `conda run -n nee_upscale`.

### All arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--in-dir` | `~/Circumpolar_TEM_aux_scripts/upscale/Circumpolar/merged` | Directory containing input `NEE_*_monthly_YYYY.nc` files |
| `--grid` | `<in-dir>/NEE_ssp1_2_6_mri_esm2_0_tr_monthly_1901.nc` | Reference NetCDF that defines the 0.5° target grid extent and resolution |
| `--mask` | `~/Circumpolar_TEM_aux_scripts/upscale/aoi_5k_buff_6931.tiff` | AOI mask in EPSG:6931 (NetCDF or GeoTIFF) |
| `--out-dir` | same as `--in-dir` | Output directory for `*_upscaled.nc` files |
| `--var` | `NEE` | Variable name inside the NetCDF files |
| `--pattern` | `NEE_*_monthly_[0-9][0-9][0-9][0-9].nc` | Glob pattern to match input files |

### What the script does (per year file)

1. Renames the native `x`/`y` dimensions to `X`/`Y` for NCO compatibility.
2. Crops the AOI mask (`Y,29,1961` × `X,50,2290`) and appends coordinate metadata
   to the NEE file, excluding the `lambert_azimuthal_equal_area` grid-mapping
   variable to avoid dimension rank mismatches.
3. Renames projected coordinates to `longitude`/`latitude`.
4. Loops over each monthly time step:
   - Extracts a single time slice with `ncks`.
   - Reprojects EPSG:6931 → EPSG:4326 at 0.5° using `gdalwarp` (bilinear
     resampling).  The `-te_srs` flag is **auto-detected** from the grid file: if
     the grid extent values are in degree range the script uses `EPSG:4326`;
     otherwise `EPSG:6931`.  This means `sample_dataset.nc` (a lat/lon file) works
     correctly out of the box.
   - Restores the original time value, units, and `365_day` calendar with
     `ncap2` / `ncatted`.
   - Renames the GDAL `Band1` output to `NEE` and concatenates months with `ncrcat`.
5. Writes one `*_upscaled.nc` file per year to `--out-dir`.

### Output files

```
<out-dir>/NEE_ssp1_2_6_mri_esm2_0_tr_monthly_2000_upscaled.nc
<out-dir>/NEE_ssp1_2_6_mri_esm2_0_tr_monthly_2001_upscaled.nc
...
<out-dir>/NEE_ssp1_2_6_mri_esm2_0_tr_monthly_2024_upscaled.nc
```

Each output file contains:

- Variable: `NEE(time=12, latitude=360, longitude=720)` — g C m⁻² month⁻¹
- CRS: EPSG:4326 (WGS84), 0.5° global grid (`GeoTransform = "-180 0.5 0 90 0 -0.5"`)
- Time: days since 1901-01-01, 365-day calendar (matching the source)

### Performance

Processing 25 yearly files (12 months each) takes approximately **10–15 minutes**
on a modern laptop.  Temporary files are written to a `tmp_upscale_nee_*` directory
inside `--out-dir` and deleted automatically on exit.

---

## Step 2 — Visual comparison with `compare_nee_upscale.py`

The viewer is a **Bokeh server app** that displays the original 5 km and the
upscaled 0.5° NEE side by side with interactive controls.

The script lives at `upscale/compare_nee_upscale.py` relative to the
repo root.  Bokeh is included in the `nee_upscale` environment created above.

### Launch (run from the repo root)

```bash
bokeh serve --show upscale/compare_nee_upscale.py --port 5007
```

Or using the full path to the environment's bokeh binary (useful when the env is
not activated):

```bash
/Users/anaconda3/envs/nee_upscale/bin/bokeh serve --show \
    upscale/compare_nee_upscale.py --port 5007
```

The app opens automatically at **http://localhost:5007/compare_nee_upscale**

### Override the data directory

By default the app reads from `upscale/Circumpolar/merged/` (resolved
relative to the script file).  To point it at a different location:

```bash
UPSCALE_DIR=/path/to/your/merged \
    bokeh serve --show upscale/compare_nee_upscale.py --port 5007
```

### Interface controls

| Control | Description |
|---------|-------------|
| **Year** slider | Step through 2000–2024 |
| **Month** dropdown | Select Jan–Dec |
| **Colormap** dropdown | Choose from 9 palettes (see below) |
| **▶ Animate years** toggle | Auto-cycle through all years |
| **Speed** dropdown | Slow (2 s) / Normal (1.2 s) / Fast (0.6 s) per frame |
| Pan / Wheel zoom | Independent on each plot; scroll to zoom, drag to pan |

### Available colormaps

| Type | Palettes | Color limits |
|------|----------|--------------|
| **Diverging** (recommended for NEE — blue = sink, red = source) | RdBu, RdYlBu, PRGn, BrBG, RdYlGn | Symmetric ±P98 around zero |
| **Sequential** | Viridis, Plasma, Inferno, Turbo | P2–P98 of the data range |

Switching colormaps instantly updates both plots and adjusts the limits.

### Plot layout

- **Left plot** — original 5 km NEE in native EPSG:6931 metres.  Displayed
  at 3× subsampling (747 × 635 effective pixels) for browser performance; full
  resolution is preserved on disk.
- **Right plot** — upscaled 0.5° NEE in EPSG:4326, cropped to ≥ 45°N.
- Both plots are **960 × 795 px** and share the same colormap and color limits.
- The colorbar is shown on the right edge of the upscaled plot.
- Masked / fill-value cells are rendered as semi-transparent grey.

---

## Archiving outputs to Google Cloud Storage

Upscaled files have been uploaded to:

```
gs://circumpolar_model_output/nee_21yr/
```

To re-upload or sync new files:

```bash
gsutil -m cp upscale/Circumpolar/merged/*_upscaled.nc \
    gs://circumpolar_model_output/nee_21yr/
```

To verify what is in the bucket:

```bash
gsutil ls gs://circumpolar_model_output/nee_21yr/ | grep "_upscaled"
```

---

## Quick-reference — full workflow

```bash
# 1. Activate the environment
conda activate nee_upscale

# 2. Upscale all yearly files (run from repo root)
pythonupscale/upscale_nee_05deg.py \
  --in-dir  upscale/Circumpolar/merged \
  --grid    upscale/sample_dataset.nc \
  --mask    upscale/aoi_5k_buff_6931_2_0.nc \
  --out-dir upscale/Circumpolar/merged

# 3. Launch the comparison viewer
bokeh serve --show upscale/compare_nee_upscale.py --port 5007

# 4. (Optional) Upload results to GCS
gsutil -m cp upscale/Circumpolar/merged/*_upscaled.nc \
    gs://circumpolar_model_output/nee_21yr/
```
