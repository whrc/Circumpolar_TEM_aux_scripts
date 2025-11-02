# Circumpolar NetCDF Interactive Viewer

Interactive Bokeh-based visualization tool for NetCDF files in Lambert Azimuthal Equal Area (LAEA) projection, optimized for circumpolar datasets from Alaska and Canada.

## Features

✨ **Key Capabilities:**
- 🌍 **Correct Circumpolar Projection** - Lambert Azimuthal Equal Area (LAEA) centered at North Pole
- 📐 **Latitude/Longitude Grid Overlay** - Visual reference lines for geographic coordinates
- ⚡ **High Performance** - Fast image-based rendering (10-100x faster than point-based)
- 🖱️ **Interactive Controls** - Zoom, pan, and explore your data
- ⏱️ **Time Animation** - Slider and play/pause controls for time-series data
- 📊 **2:1 Aspect Ratio** - Optimized wide-screen display
- 🧮 **NEE Calculation** - Built-in Net Ecosystem Exchange (NEE = RECO - GPP) calculator
- 🔄 **Image Flip Correction** - Automatically flips data along X-axis for proper orientation

## Installation

### Requirements

```bash
# Required packages
pip install xarray numpy bokeh netCDF4 scipy

# Or use conda
conda install xarray numpy bokeh netCDF4 scipy
```

### Dependencies
- Python 3.7+
- xarray
- numpy
- bokeh
- netCDF4
- scipy

## Usage

### Basic Usage

```bash
# Visualize a NetCDF file (auto-detects main variable)
python plot_bokeh_circumpolar_latlon.py <file_path>

# Example with the ALD dataset
python plot_bokeh_circumpolar_latlon.py ../Alaska/merged/ALD_ssp5_8_5_mri_esm2_0_yearly.nc
```

### Specify Variable

```bash
# Explicitly specify which variable to plot
python plot_bokeh_circumpolar_latlon.py <file_path> <variable_name>

# Example
python plot_bokeh_circumpolar_latlon.py data.nc VEGC
```

### List Available Variables

```bash
# View all variables in a NetCDF file
python plot_bokeh_circumpolar_latlon.py data.nc --list
```

### Custom Port

```bash
# Run on a different port (default is 5006)
python plot_bokeh_circumpolar_latlon.py data.nc --port 5007
```

### NEE Calculation

The script includes a built-in Net Ecosystem Exchange (NEE) calculator:

```bash
# Calculate NEE from default GPP and RECO files
python plot_bokeh_circumpolar_latlon.py --nee

# Calculate NEE with custom input files
python plot_bokeh_circumpolar_latlon.py --nee \
    --gpp-file /path/to/GPP.nc \
    --reco-file /path/to/RECO.nc \
    --output-file /path/to/NEE.nc

# Then visualize the result
python plot_bokeh_circumpolar_latlon.py /path/to/NEE.nc
```

**NEE Formula:** NEE = RECO - GPP
- **GPP** (Gross Primary Production): CO₂ uptake by plants
- **RECO** (Ecosystem Respiration): CO₂ release by respiration
- **NEE** (Net Ecosystem Exchange): Net CO₂ flux (positive = source, negative = sink)

## Projection Details

### Lambert Azimuthal Equal Area (LAEA)

The viewer uses **WGS84 ellipsoid parameters** for accurate coordinate transformations:
- **Semi-major axis (a):** 6,378,137.0 meters
- **Inverse flattening:** 298.257223563
- **Projection center:** North Pole (90°N, 0°E)
- **False easting/northing:** 0.0

### Coordinate System

- **X-axis:** Easting in meters (LAEA projection coordinates)
- **Y-axis:** Northing in meters (LAEA projection coordinates)
- **Grid overlay:** Shows latitude circles (dashed) and longitude meridians (dotted)
- **Aspect ratio:** 2:1 (width:height) for optimal viewing of circumpolar data

### Geographic Coverage

The viewer automatically handles tiles from the circumpolar dataset covering:
- Alaska
- Canada (Yukon, Northwest Territories, Nunavut)
- Northern regions in LAEA projection

**Example tile coordinates:**
- LAEA: X = -4,234,000 to -4,002,000 m, Y = 802,000 to 1,002,000 m
- Geographic: ~50-52°N, 100-104°W (Central Canada)

## Interactive Controls

### Time Controls
- **Slider:** Drag to select specific time step
- **Play/Pause Button:** Animate through time automatically
- **Year Display:** Shows current year being displayed

### Visualization Controls
- **Pan:** Click and drag to move around
- **Zoom:** 
  - Mouse wheel to zoom in/out
  - Box zoom tool in toolbar
  - Reset tool to return to original view
- **Hover:** View coordinate information (if hover tool enabled)

### Toolbar Tools
- 🔍 Pan
- 📦 Box Zoom
- 🔄 Wheel Zoom
- 💾 Save (export as PNG)
- 🏠 Reset (return to original view)

## Performance Optimization

The viewer uses **image-based rendering** instead of individual pixels:
- **Fast rendering:** 10-100x speed improvement
- **Smooth interactions:** Instant zoom and pan
- **Large datasets:** Handles arrays of 1000+ × 1000+ pixels efficiently

## Output Information

When you run the script, it displays:

```
Opening file: <file_path>
Dataset dimensions: {'time': 76, 'y': 1771, 'x': 1241, ...}
Dataset variables: ['ALD', 'lambert_azimuthal_equal_area']
Auto-detected variable: ALD
ALD shape: (76, 1771, 1241)
Time dimension size: 76
Using start year: 2024

Projection info:
  Projection: lambert_azimuthal_equal_area
  Center latitude: 90.0
  Center longitude: 0.0

Data will be displayed in circumpolar projection with lat/lon grid
Image will be flipped along X-axis for correct orientation
Rendering (1771, 1241) array using fast image method

🌍 Starting Circumpolar Bokeh server with Lat/Lon grid on port 5006...
Opening browser at http://localhost:5006/
✓ Image flipped along X-axis for correct orientation
✓ Latitude/longitude grid overlay added (no axis ticks)
✓ 2:1 aspect ratio (width:height)
Press Ctrl+C to stop the server
```

## File Format Requirements

### NetCDF Structure

The script expects NetCDF files with:

1. **Coordinate variables:**
   - `x`: Easting coordinates in meters (LAEA)
   - `y`: Northing coordinates in meters (LAEA)
   - `time`: Time dimension (optional)

2. **Data variable(s):**
   - 2D arrays: `(y, x)`
   - 3D arrays: `(time, y, x)`

3. **Projection metadata:**
   - `lambert_azimuthal_equal_area` variable with projection attributes
   - `GeoTransform` attribute for coordinate reconstruction

### Example NetCDF Structure

```
Dimensions:
  time: 76
  y: 1771
  x: 1241

Coordinates:
  time (time): datetime64
  y (y): float64 - LAEA northing in meters
  x (x): float64 - LAEA easting in meters

Data variables:
  ALD (time, y, x): float64 - Active Layer Depth
  lambert_azimuthal_equal_area: grid mapping variable
    Attributes:
      - grid_mapping_name: "lambert_azimuthal_equal_area"
      - latitude_of_projection_origin: 90.0
      - longitude_of_projection_origin: 0.0
      - semi_major_axis: 6378137.0
      - inverse_flattening: 298.257223563
      - GeoTransform: "-1200000 4000 0 2400000 0 -4000"
```

## Supported Variables

The viewer works with any gridded NetCDF variable. Common TEM (Terrestrial Ecosystem Model) variables include:

- **ALD** - Active Layer Depth (m)
- **VEGC** - Vegetation Carbon (gC/m²)
- **VEGNTOT** - Total Vegetation Nitrogen (gN/m²)
- **GPP** - Gross Primary Production (gC/m²/year)
- **RECO** - Ecosystem Respiration (gC/m²/year)
- **NEE** - Net Ecosystem Exchange (gC/m²/year)
- **DEEPC** - Deep Soil Carbon (gC/m²)
- **SHLWC** - Shallow Soil Carbon (gC/m²)
- **SOCFROZEN** - Frozen Soil Organic Carbon (gC/m²)
- **SOCUNFROZEN** - Unfrozen Soil Organic Carbon (gC/m²)
- **PET** - Potential Evapotranspiration (mm)
- **INGPP** - Input GPP (gC/m²/year)

## Troubleshooting

### Browser doesn't open automatically
- Manually navigate to `http://localhost:5006/` in your web browser
- Check if the port is already in use, try a different port with `--port`

### "No suitable data variables found"
- Use `--list` flag to see available variables
- Ensure your NetCDF file contains at least one data variable

### Projection looks incorrect
- Verify your NetCDF file uses LAEA projection
- Check that `lambert_azimuthal_equal_area` variable exists with correct attributes
- The script automatically flips the image along X-axis for proper orientation

### Slow performance
- The script uses optimized image rendering for speed
- Large time series (100+ timesteps) may take a moment to load
- Animation speed is configurable via the play button

### Port already in use
```bash
# Use a different port
python plot_bokeh_circumpolar_latlon.py data.nc --port 5007
```

### Missing dependencies
```bash
pip install xarray numpy bokeh netCDF4 scipy
```

## Technical Details

### Coordinate Transformation

The viewer performs coordinate transformations between LAEA and geographic coordinates using:

**LAEA to Lat/Lon:**
```python
# Uses WGS84 authalic radius for equal-area property
e² = 2f - f²
q_p = (1 - e²) * [1/(1-e²) - (1/2e) * ln((1-e)/(1+e))]
R_q = a * √(q_p / 2)  # Authalic radius ≈ 6,371,007 m

# Inverse LAEA transformation
ρ = √(x² + y²)
c = 2 * arcsin(ρ / (2 * R_q))
lat = arcsin(cos(c) * sin(lat₀) + (y * sin(c) * cos(lat₀)) / ρ)
lon = lon₀ + arctan2(x * sin(c), ρ * cos(lat₀) * cos(c) - y * sin(lat₀) * sin(c))
```

### Grid Line Generation

Latitude circles and longitude meridians are calculated and overlaid on the map:
- **Latitude circles:** Every 5° from 40°N to 85°N (dashed lines)
- **Longitude meridians:** Every 15° from -180° to 180° (dotted lines)

### Image Flipping

The script automatically flips the data array along the X-axis to correct the orientation:
```python
flipped_data = np.flip(data, axis=1)  # Flip along X-axis
```

## Command-Line Options

```
usage: plot_bokeh_circumpolar_latlon.py [-h] [--list] [--port PORT] [--nee]
                                        [--gpp-file GPP_FILE]
                                        [--reco-file RECO_FILE]
                                        [--output-file OUTPUT_FILE]
                                        [file_path] [variable]

positional arguments:
  file_path             Path to NetCDF file
  variable              Variable name to plot (optional, auto-detected if not provided)

optional arguments:
  -h, --help            Show this help message and exit
  --list                List available variables in the NetCDF file
  --port PORT           Port for Bokeh server (default: 5006)
  --nee                 Calculate NEE (Net Ecosystem Exchange) as RECO - GPP
  --gpp-file GPP_FILE   Custom path to GPP NetCDF file (for --nee)
  --reco-file RECO_FILE Custom path to RECO NetCDF file (for --nee)
  --output-file OUTPUT_FILE
                        Custom output path for NEE file (for --nee)
```

## Examples

### Example 1: Quick Visualization
```bash
python plot_bokeh_circumpolar_latlon.py ../Alaska/merged/ALD_ssp5_8_5_mri_esm2_0_yearly.nc
```

### Example 2: View VEGC Data
```bash
python plot_bokeh_circumpolar_latlon.py ../Alaska/merged/VEGC_ssp5_8_5_mri_esm2_0_yearly.nc VEGC
```

### Example 3: Check Variables First
```bash
python plot_bokeh_circumpolar_latlon.py ../Alaska/merged/ALD_ssp5_8_5_mri_esm2_0_yearly.nc --list
```

### Example 4: Calculate and Visualize NEE
```bash
# Step 1: Calculate NEE
python plot_bokeh_circumpolar_latlon.py --nee

# Step 2: Visualize NEE
python plot_bokeh_circumpolar_latlon.py ../Alaska/merged/NEE_ssp5_8_5_mri_esm2_0_sc_yearly.nc
```

### Example 5: Custom Port
```bash
python plot_bokeh_circumpolar_latlon.py data.nc --port 8080
# Then navigate to http://localhost:8080/
```

## Output Files

### NEE Calculation Output

When using `--nee`, the script creates a new NetCDF file containing:
- **Variable:** NEE (Net Ecosystem Exchange)
- **Units:** gC/m²/year (or inherited from input files)
- **Metadata:** Complete projection information and attributes
- **Formula:** NEE = RECO - GPP

## Contributing

For issues, suggestions, or contributions, please refer to the main repository documentation.

## License

See the LICENSE file in the repository root.

## Citation

If you use this visualization tool in your research, please cite the underlying TEM model and dataset appropriately.

## Related Scripts

- `download_tiles.py` - Download tiles from data sources
- `merge.py` - Merge multiple tiles into regional datasets
- `plot_tiles.py` - Visualize individual tiles

## Version History

- **v2.0** - Added NEE calculation, 2:1 aspect ratio, WGS84 coordinate support
- **v1.5** - Added lat/lon grid overlay, removed axis ticks
- **v1.0** - Initial release with LAEA projection support

---

**Author:** TEM Circumpolar Project  
**Last Updated:** November 2025  
**Tested with:** Python 3.9+, Bokeh 3.x, xarray 2023.x

