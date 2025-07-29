#!/usr/bin/env python3
"""
This script checks for negative values in the 'nirr' variable.
It validates both projected and historic climate data files in NetCDF format and reports any issues found.

Usage:

python check_nirr.py <path_to_directory>
"""

import xarray as xr
import sys
import numpy as np
import os

def check_nirr(path, label):
    try:
        ds = xr.open_dataset(path)
        if "nirr" in ds.variables:
            nirr = ds["nirr"]
            has_negative = (nirr < 0).any()
            return bool(has_negative)
        else:
            print(f"  [!] {label}: 'nirr' variable not found")
            return True  # treat as error/negative for reporting
    except Exception as e:
        print(f"  [X] {label}: Error reading file: {e}")
        sys.exit(1)
    finally:
        try:
            ds.close()
        except Exception:
            pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <path>")
        sys.exit(1)

    input_path = sys.argv[1]
    abs_path = os.path.abspath(input_path) if not os.path.isabs(input_path) else input_path

    projected_climate_path = os.path.join(abs_path, "projected-climate.nc")
    historic_climate_path = os.path.join(abs_path, "historic-climate.nc")

    neg_proj = check_nirr(projected_climate_path, "projected-climate.nc")
    neg_hist = check_nirr(historic_climate_path, "historic-climate.nc")

    any_negative = False

    if neg_proj:
        print(f"[!] projected-climate.nc: Negative values found in 'nirr' or 'nirr' variable missing.")
        any_negative = True
    if neg_hist:
        print(f"[!] historic-climate.nc: Negative values found in 'nirr' or 'nirr' variable missing.")
        any_negative = True

    if not any_negative:
        print("Success: No negative values found in 'nirr' for either file.")
