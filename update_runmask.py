#!/usr/bin/env python3
import re
import numpy as np
import xarray as xr
import sys
import shutil
import os

def extract_completed_indices(log_file):
    """Parse log file and extract completed cell indices."""
    completed = []
    with open(log_file, 'r') as f:
        for line in f:
            match = re.search(r"cell\s+\d+,\s*(\d+)\s+complete", line)
            if match:
                completed.append(int(match.group(1)))
    return sorted(set(completed))

def update_run_mask(input_nc, output_nc, completed_indices):
    """Set run=0 for cells in completed_indices and save to new NetCDF."""
    ds = xr.open_dataset(input_nc, decode_times=False)
    run_var = ds['run'].values.copy()

    # Debug: shape info
    print(f"Original run mask shape: {run_var.shape}")
    
    # Assuming run is 2D (Y, X) and indices refer to X dimension
    for idx in completed_indices:
        if idx < run_var.shape[-1]:  # ensure index is in range
            run_var[..., idx] = np.where(run_var[..., idx] == 1, 0, run_var[..., idx])
        else:
            print(f"Warning: index {idx} is out of bounds for run variable")

    # Create a copy of dataset with updated run
    ds_new = ds.copy()
    ds_new['run'].values[:] = run_var

    # Save to new NetCDF
    ds_new.to_netcdf(output_nc)
    ds.close()
    ds_new.close()
    print(f"Updated run_mask saved to: {output_nc}")

def main():
    if len(sys.argv) != 4:
        print("Usage: python update_run_mask.py <log_file> <input_run_mask.nc> <output_run_mask.nc>")
        sys.exit(1)

    log_file = sys.argv[1]
    input_nc = sys.argv[2]
    output_nc = sys.argv[3]

    # Extract completed indices from log
    completed = extract_completed_indices(log_file)
    print(f"Completed cell indices: {completed}")

    # Update run mask
    update_run_mask(input_nc, output_nc, completed)

if __name__ == "__main__":
    main()

