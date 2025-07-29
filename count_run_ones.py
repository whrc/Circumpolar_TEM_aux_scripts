#!/usr/bin/env python3
import xarray as xr
import numpy as np
import sys

def count_run_ones(file_path):
    # Open the dataset
    ds = xr.open_dataset(file_path, decode_times=False)

    # Extract the 'run' variable
    run_data = ds['run'].values

    # Flatten and remove any NaNs or fill values (-9999 if applicable)
    run_flat = run_data.flatten()
    if '_FillValue' in ds['run'].attrs:
        fill_value = ds['run'].attrs['_FillValue']
        run_flat = run_flat[run_flat != fill_value]
    run_flat = run_flat[~np.isnan(run_flat)]

    # Count how many entries are equal to 1
    count_ones = np.sum(run_flat == 1)
    count_zeros = np.sum(run_flat == 0)

    print(f"Number of 1s in 'run' variable: {count_ones}")
    print(f"Number of 0s in 'run' variable: {count_zeros}")

    ds.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python count_run_ones.py <path_to_run-mask.nc>")
        sys.exit(1)

    count_run_ones(sys.argv[1])

