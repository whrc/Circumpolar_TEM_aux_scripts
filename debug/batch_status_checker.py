#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
from pathlib import Path
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


def check_run_status(base_folder, nc_file, batch_folder_name):
    """
    Check the run status of a batch.
    
    Args:
        base_folder: Base folder containing batches
        nc_file: Path to run_status.nc file
        batch_folder_name: Name of the batch folder (e.g., "batch_0")
        
    Returns:
        tuple: (m, n) where m is successful cells and n is total cells to run
    """
    m = 0
    n = 0
    try:
        # Open the NetCDF file
        with Dataset(nc_file, "r") as nc:
            # Extract dimensions
            Y = nc.dimensions['Y'].size
            X = nc.dimensions['X'].size
            batch_input_folder = os.path.join(base_folder, batch_folder_name, "input")
            mask_file_path = os.path.join(batch_input_folder, "run-mask.nc")
            if os.path.exists(mask_file_path):
                n = count_run_ones(mask_file_path)
            else:
                print(f"{mask_file_path}: File does not exist")
                return m, n
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

def run_extract_failed_cells(batch_path, script_path=None):
    """
    Run extract_failed_cells.py on a batch.
    
    Args:
        batch_path: Path to the batch directory
        script_path: Path to extract_failed_cells.py script (if None, tries to find it)
        
    Returns:
        bool: True if successful, False otherwise
    """
    if script_path is None:
        # Try to find the script in the same directory as this script (debug folder)
        current_script_dir = Path(__file__).parent
        script_path = current_script_dir / "extract_failed_cells.py"
        
        # If not found, try parent directory
        if not script_path.exists():
            script_path = current_script_dir.parent / "extract_failed_cells.py"
    
    if not os.path.exists(script_path):
        print(f"Error: extract_failed_cells.py not found at {script_path}", file=sys.stderr)
        return False
    
    try:
        # Run the script with --force flag to overwrite existing retry directory
        cmd = [sys.executable, str(script_path), str(batch_path), "--force"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ Created retry batch for {batch_path}")
            return True
        else:
            print(f"✗ Failed to create retry batch for {batch_path}", file=sys.stderr)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            return False
    except Exception as e:
        print(f"Error running extract_failed_cells.py for {batch_path}: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Check run status of batches and optionally create retry batches for unfinished ones',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'base_folder',
        help='Base folder containing batch directories'
    )
    parser.add_argument(
        '--individual-retry',
        action='store_true',
        help='Create retry batches for all unfinished batches using extract_failed_cells.py'
    )
    
    args = parser.parse_args()
    
    base_folder = args.base_folder
    batch_folders = [d for d in os.listdir(base_folder) if os.path.isdir(os.path.join(base_folder, d)) and d.startswith("batch_")]
    
    # Sort batch folders to process in order
    batch_folders.sort(key=lambda x: int(x.split('_')[1]) if '_' in x and x.split('_')[1].isdigit() else 999)
    
    n_batches = len(batch_folders)
    total_m = 0  # Sum of completed gridcells
    total_time = 0
    count_n = 0
    total_n = 0
    unfinished_batches = []
    
    for batch_folder_name in batch_folders:
        batch_folder = os.path.join(base_folder, batch_folder_name, "output")
        nc_file_path = os.path.join(batch_folder, "run_status.nc")

        batch_input_folder = os.path.join(base_folder, batch_folder_name, "input")
        mask_file_path = os.path.join(batch_input_folder, "run-mask.nc")

        # Get number of cells that should run
        if os.path.exists(mask_file_path):
            n = count_run_ones(mask_file_path)
        else:
            print(f"{mask_file_path}: File does not exist")
            n = 0

        # Check run status
        if os.path.exists(nc_file_path):
            m, n1 = check_run_status(base_folder, nc_file_path, batch_folder_name)
            total_m += m
            
            # Check if batch is finished (m != n means not finished)
            if m != n:
                # Batch is not finished
                batch_path = os.path.join(base_folder, batch_folder_name)
                unfinished_batches.append(batch_path)
            
            mean_runtime = calculate_mean_runtime(nc_file_path)
            if mean_runtime is not None:
                total_time += mean_runtime 
                count_n += 1
        else:
            print(f"{nc_file_path}: File does not exist")
            # If run_status.nc doesn't exist, consider batch unfinished
            batch_path = os.path.join(base_folder, batch_folder_name)
            unfinished_batches.append(batch_path)

        total_n = total_n + n

    # Calculate and print the percentage of completion
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
    
    # Handle individual retry flag
    if args.individual_retry:
        if unfinished_batches:
            print(f"\n{'='*80}")
            print(f"Creating retry batches for {len(unfinished_batches)} unfinished batch(es)...")
            print(f"{'='*80}")
            
            for batch_path in unfinished_batches:
                print(f"\nProcessing: {batch_path}")
                run_extract_failed_cells(batch_path)
            
            print(f"\n{'='*80}")
            print(f"Finished processing {len(unfinished_batches)} unfinished batch(es)")
            print(f"{'='*80}")
        else:
            print("\n✓ All batches are finished - no retry batches needed.")


# add --dry-run flag to check if a tile will be retried. percentage > 70%, we'll submit. no job submission.
# make --submit flag to submit the retry batches
# automate the job checking and merging like automation_script.py
