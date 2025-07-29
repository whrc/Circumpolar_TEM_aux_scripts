#!/usr/bin/env python3
"""
NetCDF Analysis Script (analyze_input_v3.py)

This script analyzes one or multiple NetCDF (.nc) files and prints:
- Mean, Max, Min (rounded to 3 decimals)
- NaN count
- Non-NaN count

It automatically skips:
- Latitude, Longitude, and Lambert projection variables
- Non-numeric variables

USAGE:
    For a single file:
        python analyze_input_v3.py batch_0/output/ALD_yearly_sp.nc

    For a folder containing multiple .nc files:
        python analyze_input_v3.py batch_0/output
"""

import xarray as xr
import numpy as np
import os
import sys
import glob

def format_float(val):
    """Return a float formatted to 3 decimals or 'nan'."""
    return f"{val:.3f}" if np.isfinite(val) else "nan"

def analyze_netcdf(file_path):
    print(f"\n--- Analyzing file: {os.path.basename(file_path)} ---")
    ds = xr.open_dataset(file_path, decode_times=False)
    
    for var in ds.data_vars:
        # Exclude latitude, longitude, and projection variables
        if var.lower() in ["lat", "lon", "latitude", "longitude", "lambert_azimuthal_equal_area"]:
            continue
        
        data = ds[var].values
        
        # Skip non-numeric variables
        if not np.issubdtype(data.dtype, np.number):
            print(f"Variable: {var} (skipped, non-numeric)")
            continue
        
        flat_data = data.flatten()
        nan_mask = np.isnan(flat_data)
        clean_data = flat_data[~nan_mask]
        
        mean_val = np.mean(clean_data) if clean_data.size > 0 else np.nan
        max_val = np.max(clean_data) if clean_data.size > 0 else np.nan
        min_val = np.min(clean_data) if clean_data.size > 0 else np.nan
        n_nans = int(nan_mask.sum())
        n_non_nans = int(clean_data.size)
        
        print(f"Variable: {var}")
        print(f"  Mean: {format_float(mean_val)}")
        print(f"  Max: {format_float(max_val)}")
        print(f"  Min: {format_float(min_val)}")
        print(f"  NaN count: {n_nans}")
        print(f"  Non-NaN count: {n_non_nans}")
    
    ds.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python analyze_input_v3.py <path_to_directory_or_file>")
        sys.exit(1)
    
    path = sys.argv[1]
    
    # If path is a single file
    if os.path.isfile(path) and path.endswith(".nc"):
        analyze_netcdf(path)
    
    # If path is a directory
    elif os.path.isdir(path):
        nc_files = glob.glob(os.path.join(path, "*.nc"))
        if not nc_files:
            print("No NetCDF (.nc) files found in the provided directory.")
            sys.exit(0)
        for file_path in nc_files:
            analyze_netcdf(file_path)
    
    else:
        print(f"Error: {path} is not a valid .nc file or directory.")
        sys.exit(1)

if __name__ == "__main__":
    main()

