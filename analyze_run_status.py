#!/usr/bin/env python3
import xarray as xr
import numpy as np
import sys

def analyze_run_status(file_path):
    # Open the dataset
    ds = xr.open_dataset(file_path, decode_times=False)

    # Extract run_status and total_runtime
    run_status = ds['run_status'].values
    total_runtime = ds['total_runtime'].values

    # Convert to 1D and remove NaNs / FillValues (-9999)
    run_status_flat = run_status.flatten()
    run_status_flat = run_status_flat[run_status_flat != -9999]

    total_runtime_flat = total_runtime.flatten()
    total_runtime_flat = total_runtime_flat[total_runtime_flat != -9999]

    # Count values in run_status
    count_0 = np.sum(run_status_flat == 0)
    count_100 = np.sum(run_status_flat == 100)
    count_neg100 = np.sum(run_status_flat == -100)

    # Count valid numbers in total_runtime
    total_runtime_count = total_runtime_flat.size

    # Print results
    print(f"run_status counts:")
    print(f"  Number of 0: {count_0}")
    print(f"  Number of 100: {count_100}")
    print(f"  Number of -100: {count_neg100}")
    print(f"\ntotal_runtime:")
    print(f"  Number of valid entries: {total_runtime_count}")

    ds.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_run_status.py <path_to_run_status.nc>")
        sys.exit(1)

    analyze_run_status(sys.argv[1])

