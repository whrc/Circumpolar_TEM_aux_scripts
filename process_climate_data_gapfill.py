import warnings
warnings.filterwarnings('ignore')
import os
import sys
import xarray as xr

if len(sys.argv) < 2:
    print("Usage: python process_climate_data_gapfill.py <tile_input_path>")
    sys.exit(1)

outpath = sys.argv[1]
sclist = ['ssp1_2_6', 'ssp2_4_5', 'ssp3_7_0', 'ssp5_8_5']
modlist = ['access_cm2', 'mri_esm2_0']

# Standard attributes for output variables
VAR_ATTRS = {
    'lat': {'standard_name': 'latitude', 'units': 'degree_north', '_FillValue': -999.0},
    'lon': {'standard_name': 'longitude', 'units': 'degree_east', '_FillValue': -999.0},
    'tair': {'standard_name': 'air_temperature', 'units': 'celsius', 'grid_mapping': 'albers_conical_equal_area', '_FillValue': -999.0},
    'precip': {'standard_name': 'precipitation_amount', 'units': 'mm month-1', 'grid_mapping': 'albers_conical_equal_area', '_FillValue': -999.0},
    'nirr': {'standard_name': 'downwelling_shortwave_flux_in_air', 'units': 'W m-2', 'grid_mapping': 'albers_conical_equal_area', '_FillValue': -999.0},
    'vapor_press': {'standard_name': 'water_vapor_pressure', 'units': 'hPa', 'grid_mapping': 'albers_conical_equal_area', '_FillValue': -999.0},
}


def process_and_save(clmt, runmask, outfile):
    """Apply mask, fix negatives, set attrs, write via temp file."""
    clmt = clmt.copy()
    clmt['precip'] = clmt['precip'].where(clmt['precip'] >= 0, 0)
    clmt['vapor_press'] = clmt['vapor_press'].where(clmt['vapor_press'] >= 0, 0)
    clmt['nirr'] = clmt['nirr'].where(clmt['nirr'] >= 0, 0)
    clmt = xr.merge([clmt, runmask], compat='no_conflicts')
    for var in ['tair', 'precip', 'nirr', 'vapor_press']:
        clmt[var] = clmt[var].where(clmt['run'] == 1.0, -999.0)
    clmt = clmt.drop_vars('run')
    for var, attrs in VAR_ATTRS.items():
        if var in clmt:
            clmt[var].attrs = attrs
    tmp = outfile + '.tmp'
    clmt.to_netcdf(tmp, unlimited_dims='time')
    os.replace(tmp, outfile)


runmask = xr.open_dataset(os.path.join(outpath, 'run-mask.nc'))

# Historic climate
clmt = xr.open_dataset(os.path.join(outpath, 'historic-climate.nc'))
process_and_save(clmt, runmask, os.path.join(outpath, 'historic-climate.nc'))

# Projected climate
for mod in modlist:
    print(mod)
    for sc in sclist:
        print(sc)
        clmt = xr.open_dataset(os.path.join(outpath, f'projected-climate_{sc}_{mod}.nc'))
        process_and_save(clmt, runmask, os.path.join(outpath, f'projected-climate_{sc}_{mod}.nc'))

