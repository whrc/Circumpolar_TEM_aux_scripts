#!/usr/bin/env python3
import os
import sys
import xarray as xr
import numpy as np
from netCDF4 import Dataset
from pathlib import Path

def count_run_ones(file_path):
    ds = xr.open_dataset(file_path, decode_times=False)
    run_data = ds['run'].values
    run_flat = run_data.flatten()

    if '_FillValue' in ds['run'].attrs:
        fill_value = ds['run'].attrs['_FillValue']
        run_flat = run_flat[run_flat != fill_value]

    run_flat = run_flat[~np.isnan(run_flat)]
    count_ones = np.sum(run_flat == 1)
    ds.close()
    return count_ones

def calculate_mean_runtime(nc_file):
    try:
        with Dataset(nc_file, "r") as nc:
            run_status = np.array(nc.variables['run_status'][:])
            total_runtime = np.array(nc.variables['total_runtime'][:])
            valid_runtimes = total_runtime[run_status == 100]
            if valid_runtimes.size > 0:
                return np.mean(valid_runtimes)
    except:
        pass
    return None

def check_run_status(base_folder, nc_file, i):
    m = 0
    n = 0
    try:
        with Dataset(nc_file, "r") as nc:
            batch_input_folder = os.path.join(base_folder, f"batch_{i}", "input")
            mask_file_path = os.path.join(batch_input_folder, "run-mask.nc")
            if os.path.exists(mask_file_path):
                n = count_run_ones(mask_file_path)

            run_status_array = np.array(nc.variables['run_status'][:])
            m = np.sum(run_status_array == 100)
    except:
        pass
    return m, n

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python check_status.py <base_folder>")

    base_folder = sys.argv[1]
    batch_folders = sorted([
        d for d in os.listdir(base_folder)
        if os.path.isdir(os.path.join(base_folder, d)) and d.startswith("batch_")
    ])
    n_batches = len(batch_folders)

    total_m = 0
    total_n = 0
    total_time = 0
    count_n = 0

    for i in range(n_batches):
        output_file = os.path.join(base_folder, f"batch_{i}", "output", "run_status.nc")
        input_mask = os.path.join(base_folder, f"batch_{i}", "input", "run-mask.nc")
        
        if os.path.exists(input_mask):
            n_mask = count_run_ones(input_mask)
            total_n += n_mask
        else:
            print(f"{mask_file_path}: File does not exist")
        
        if os.path.exists(output_file):
            m, n_i = check_run_status(base_folder, output_file, i)
            total_m += m

            runtime = calculate_mean_runtime(output_file)
            if runtime is not None:
                total_time += runtime
                count_n += 1

    if total_n > 0:
        completion = (total_m / total_n) * 100
        print(total_m, total_n)
        print(f"\nOverall Completion: {completion:.2f}%")
        if count_n > 0:
            avg_runtime = total_time / count_n
            print(f"Mean total runtime: {avg_runtime:.2f} seconds")
    else:
        print("\nNo valid data found for processing.")

