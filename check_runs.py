#!/usr/bin/env python3
import os
import sys
import xarray as xr
import numpy as np
from netCDF4 import Dataset

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

    #print(f"Number of 1s in 'run' variable: {count_ones}")
    #print(f"Number of 0s in 'run' variable: {count_zeros}")

    ds.close()

    return count_ones

def calculate_mean_runtime(nc_file):
    """
    Reads the NetCDF file and calculates the mean of `total_runtime`
    for entries where `run_status` is 100.
    """
    try:
        with Dataset(nc_file, "r") as nc:
            # Extract the variables
            run_status = nc.variables['run_status'][:]
            total_runtime = nc.variables['total_runtime'][:]

            # Convert to NumPy arrays
            run_status_array = np.array(run_status)
            total_runtime_array = np.array(total_runtime)

            # Apply mask for `run_status == 100`
            valid_mask = (run_status_array == 100)
            valid_runtimes = total_runtime_array[valid_mask]

            # Compute mean if there are valid values
            if valid_runtimes.size > 0:
                mean_runtime = np.mean(valid_runtimes)
                return mean_runtime
            else:
                return None  # No valid data

    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        return None


def check_run_status(base_folder, nc_file):
    try:
        # Open the NetCDF file
        with Dataset(nc_file, "r") as nc:
            # Extract dimensions
            Y = nc.dimensions['Y'].size
            X = nc.dimensions['X'].size
            batch_input_folder = os.path.join(base_folder, f"batch_{i}", "input")
            mask_file_path = os.path.join(batch_input_folder, "run-mask.nc")
            if os.path.exists(mask_file_path):
                n = count_run_ones(mask_file_path)
            else:
                print(f"{mask_file_path}: File does not exist")
            #n = X * Y  # Total number of elements

            # Extract run_status variable
            run_status = nc.variables['run_status'][:]

            # Convert to NumPy array and count occurrences of 100
            run_status_array = np.array(run_status)
            m = np.sum(run_status_array == 100)  # Count where run_status == 100

            # Check if all values are 100
            if m == n:
                print(f"{nc_file}: finished")
            else:
                print(f"{nc_file}: m = {m}, n = {n}")

    except Exception as e:
        print(f"Error processing {nc_file}: {e}")

    return m, n

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_status.py <base_folder>")
        sys.exit(1)

    base_folder = sys.argv[1]
    batch_folders = [d for d in os.listdir(base_folder) if os.path.isdir(os.path.join(base_folder, d)) and d.startswith("batch_")]
    n_batches = len(batch_folders)
    total_m = 0  # Sum of completed gridcells
    total_time = 0
    count_n = 0
    total_n = 0
    for i in range(n_batches):  # Looping over batch_0 to batch_9
        batch_folder = os.path.join(base_folder, f"batch_{i}", "output")
        nc_file_path = os.path.join(batch_folder, "run_status.nc")

        batch_input_folder = os.path.join(base_folder, f"batch_{i}", "input")
        mask_file_path = os.path.join(batch_input_folder, "run-mask.nc")


        if os.path.exists(nc_file_path):
            m,n1 = check_run_status(base_folder,nc_file_path)
            total_m += m
        else:
            print(f"{nc_file_path}: File does not exist")

        if os.path.exists(mask_file_path):
            n = count_run_ones(mask_file_path)
        else:
            print(f"{mask_file_path}: File does not exist")

        mean_runtime = calculate_mean_runtime(nc_file_path)
        if mean_runtime is not None:
            total_time += mean_runtime 
            count_n += 1

        total_n=total_n+n

    # Calculate and print the percentage of completion
    #total_n = n*n_batches
    
    if total_n > 0:
        completion_percentage = (total_m / total_n) * 100
        if count_n == 0:
            print(f"\nOverall Completion: {completion_percentage:.2f}%")
        else:
            average_run_time = total_time / count_n
            print(f"\nOverall Completion: {completion_percentage:.2f}%")
            print(f"\nMean total runtime: {average_run_time:.2f} seconds")
    else:
        print("\nNo valid data found for processing.")

