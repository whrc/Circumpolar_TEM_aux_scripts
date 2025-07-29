"""
Usage:

python find_runtime_matches.py <directory_with_run_status_nc> <total_runtime_seconds>

Arguments:

directory_with_run_status_nc : str
    Path to directory containing the 'run_status.nc' file
total_runtime_seconds : int
    Target runtime in seconds to search for

Examples:

python find_runtime_matches.py ../batch_0/output/ 215
python find_runtime_matches.py ./ 300
"""

import xarray as xr
import sys
import os
import numpy as np


def validate_inputs(args):
    if len(args) < 3:
        print("Usage: python find_runtime_matches.py <directory_with_run_status_nc> <total_runtime>")
        sys.exit(1)
    
    run_status_dir = args[1]
    total_runtime = args[2]
    
    try:
        user_runtime = int(total_runtime)
    except ValueError:
        print(f"Provided total_runtime '{total_runtime}' is not a valid integer.")
        sys.exit(1)
    
    return run_status_dir, user_runtime


def get_run_status_path(run_status_dir):
    run_status_path = os.path.join(run_status_dir, "run_status.nc")
    
    if not os.path.isabs(run_status_path):
        run_status_path = os.path.abspath(run_status_path)
    
    if not os.path.exists(run_status_path):
        print(f"File not found: {run_status_path}")
        sys.exit(1)
    
    return run_status_path


def load_runtime_data(run_status_path):
    try:
        ds = xr.open_dataset(run_status_path, engine="netcdf4")
        
        if "total_runtime" not in ds.variables:
            print(f"'total_runtime' variable not found in {run_status_path}")
            ds.close()
            sys.exit(1)
        
        runtime_data = ds["total_runtime"]
        return ds, runtime_data
    
    except Exception as e:
        print(f"Error reading {run_status_path}: {e}")
        sys.exit(1)


def find_matching_coordinates(runtime_data, user_runtime):
    runtime_timedelta = np.timedelta64(user_runtime, 's')
    matches = (runtime_data == runtime_timedelta)
    
    if not matches.any():
        return []
    
    # Find all coordinates where matches is True
    y_indices, x_indices = np.where(matches.values)
    
    if len(y_indices) == 0:
        return []
    
    y_coords = [matches.Y.values[y_idx] for y_idx in y_indices]
    x_coords = [matches.X.values[x_idx] for x_idx in x_indices]
    
    return list(zip(y_coords, x_coords))


def print_results(coordinates, user_runtime):
    if not coordinates:
        print(f"No matches found for total_runtime = {user_runtime}")
        return
    
    print(f"Found {len(coordinates)} matches for total_runtime = {user_runtime}:")
    for y, x in coordinates:
        print(f"x: {x}, y: {y}")


def main():
    run_status_dir, user_runtime = validate_inputs(sys.argv)
    run_status_path = get_run_status_path(run_status_dir)
    
    ds, runtime_data = load_runtime_data(run_status_path)
    
    try:
        coordinates = find_matching_coordinates(runtime_data, user_runtime)
        print_results(coordinates, user_runtime)
    finally:
        ds.close()


if __name__ == "__main__":
    main()
