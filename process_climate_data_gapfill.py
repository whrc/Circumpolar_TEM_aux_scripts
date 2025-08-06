import os
import xarray as xr
import pandas as pd
import numpy as np
import sys


if len(sys.argv) < 2:
    print("Usage: python process_climate_data_gapfill.py <tile_input_path>")
    sys.exit(1)

outpath = sys.argv[1]
sclist=['ssp1_2_6','ssp2_4_5','ssp3_7_0','ssp5_8_5']
modlist=['access_cm2','mri_esm2_0']

### CORRECT THE FEW NEGATIVES HERE AND THERE

clmt = xr.open_dataset(os.path.join(outpath,'historic-climate.nc'))
runmask = xr.open_dataset(os.path.join(outpath,'run-mask.nc'))
# Set negative values to zero
clmt['precip'] = clmt['precip'].where(clmt['precip'] >= 0, 0)
clmt['vapor_press'] = clmt['vapor_press'].where(clmt['vapor_press'] >= 0, 0)
clmt['nirr'] = clmt['nirr'].where(clmt['nirr'] >= 0, 0)
# Replace values to missing outside of the mask
clmt = xr.merge([clmt, runmask])
clmt['tair'] = clmt['tair'].where(clmt['run'] == 1.0, -999.)
clmt['precip'] = clmt['precip'].where(clmt['run'] == 1.0, -999.)
clmt['nirr'] = clmt['nirr'].where(clmt['run'] == 1.0, -999.)
clmt['vapor_press'] = clmt['vapor_press'].where(clmt['run'] == 1.0, -999.)
clmt = clmt.drop_vars('run')
# Format the output netcdf file
clmt['lat'].attrs={'standard_name':'latitude','units':'degree_north','_FillValue': -999.0}
clmt['lon'].attrs={'standard_name':'longitude','units':'degree_east','_FillValue': -999.0}
clmt['tair'].attrs={'standard_name':'air_temperature','units':'celsius','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
clmt['precip'].attrs={'standard_name':'precipitation_amount','units':'mm month-1','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
clmt['nirr'].attrs={'standard_name':'downwelling_shortwave_flux_in_air','units':'W m-2','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
clmt['vapor_press'].attrs={'standard_name':'water_vapor_pressure','units':'hPa','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
# clmt['time'].attrs={'units':'days since 1901-1-1 0:0:0','long_name':'time','calendar':'365_day'}
# clmt.time.encoding['units'] = 'days since 1901-01-01 00:00:00'
# clmt.time.encoding['calendar'] = '365_day'
# clmt.time.encoding['long_name'] = 'time'
# clmt.to_netcdf(os.path.join(outpath,'historic-climate_gf.nc'))
clmt.to_netcdf(os.path.join(outpath,'historic-climate.nc'),unlimited_dims='time')

for mod in modlist:
    print(mod)
    for sc in sclist:
        print(sc)
        clmt = xr.open_dataset(os.path.join(outpath,'projected-climate_' + sc + '_' + mod + '.nc'))
        # Set negative values to zero
        clmt['precip'] = clmt['precip'].where(clmt['precip'] >= 0, 0)
        clmt['vapor_press'] = clmt['vapor_press'].where(clmt['vapor_press'] >= 0, 0)
        clmt['nirr'] = clmt['nirr'].where(clmt['nirr'] >= 0, 0)
        # Replace values to missing outside of the mask
        clmt = xr.merge([clmt, runmask])
        clmt['tair'] = clmt['tair'].where(clmt['run'] == 1.0, -999.)
        clmt['precip'] = clmt['precip'].where(clmt['run'] == 1.0, -999.)
        clmt['nirr'] = clmt['nirr'].where(clmt['run'] == 1.0, -999.)
        clmt['vapor_press'] = clmt['vapor_press'].where(clmt['run'] == 1.0, -999.)
        clmt = clmt.drop_vars('run')
        # Format the output netcdf file
        clmt['lat'].attrs={'standard_name':'latitude','units':'degree_north','_FillValue': -999.0}
        clmt['lon'].attrs={'standard_name':'longitude','units':'degree_east','_FillValue': -999.0}
        clmt['tair'].attrs={'standard_name':'air_temperature','units':'celsius','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
        clmt['precip'].attrs={'standard_name':'precipitation_amount','units':'mm month-1','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
        clmt['nirr'].attrs={'standard_name':'downwelling_shortwave_flux_in_air','units':'W m-2','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
        clmt['vapor_press'].attrs={'standard_name':'water_vapor_pressure','units':'hPa','grid_mapping':'albers_conical_equal_area','_FillValue': -999.0}
        # clmt['time'].attrs={'units':'days since 1901-1-1 0:0:0','long_name':'time','calendar':'365_day'}
        # clmt.time.encoding['units'] = 'days since 1901-01-01 00:00:00'
        # clmt.time.encoding['calendar'] = '365_day'
        # clmt.time.encoding['long_name'] = 'time'
        clmt.to_netcdf(os.path.join(outpath,'projected-climate_' + sc + '_' + mod + '.nc'),unlimited_dims='time')
