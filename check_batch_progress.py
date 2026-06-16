#!/usr/bin/env python3
import os
import glob
import argparse
import warnings
import numpy as np
from pathlib import Path
from netCDF4 import Dataset

# Suppress warnings from numpy masked arrays
warnings.filterwarnings('ignore', category=FutureWarning, message='.*Format strings passed to MaskedConstant.*')
warnings.filterwarnings('ignore', category=UserWarning, message='.*converting a masked element to nan.*')

def get_progress(batch_dir):
    output_dir = Path(batch_dir) / 'output'
    input_dir = Path(batch_dir) / 'input'
    
    if not output_dir.exists():
        return 'Not started'
        
    # Check fail log
    if (output_dir / 'fail_log.txt').exists():
        return 'Failed'
        
    run_status_file = output_dir / 'run_status.nc'
    run_mask_file = input_dir / 'run-mask.nc'
    
    if not run_status_file.exists() or not run_mask_file.exists():
        return 'Starting...'
        
    try:
        # Get total number of active cells to run
        with Dataset(run_mask_file, 'r') as ds_mask:
            run_mask = ds_mask.variables['run'][:]
            active_cells = np.sum(run_mask == 1)
            
        # Get number of cells that finished successfully
        with Dataset(run_status_file, 'r') as ds_status:
            run_status = ds_status.variables['run_status'][:]
            completed_cells = np.sum(run_status == 100)
            
        if active_cells == 0:
            return 'Completed (0 active cells)'
            
        active_val = int(active_cells)
        completed_val = int(completed_cells)
            
        if completed_val == active_val:
            return f'Completed ({active_val}/{active_val} cells)'
            
        percentage = float(completed_val) / float(active_val) * 100.0
        return f'Running: {completed_val}/{active_val} cells completed ({percentage:.1f}%)'
    except Exception as e:
        return f'Error reading status: {e}'

def main():
    parser = argparse.ArgumentParser(description="Check the progress of dvmdostem batches.")
    parser.add_argument("batch_dir", type=str, help="Path to the main batch directory (e.g., /path/to/Exp_spin_noFire_noWetland_split)")
    parser.add_argument("--details", action="store_true", help="Print details for every running/starting batch")
    args = parser.parse_args()

    base_dir = args.batch_dir
    if not os.path.exists(base_dir):
        print(f"Error: Directory {base_dir} does not exist.")
        return

    batch_dirs = [d for d in glob.glob(os.path.join(base_dir, 'batch_*')) if os.path.isdir(d)]
    
    if not batch_dirs:
        print(f"No batch directories found in {base_dir}")
        return

    running_batches = []
    starting_batches = []
    completed_batches = []
    failed_batches = []
    not_started_batches = []

    print(f'Found {len(batch_dirs)} batch directories. Analyzing progress...\n')

    for b in sorted(batch_dirs, key=lambda x: int(os.path.basename(x).split('_')[1])):
        prog = get_progress(b)
        if 'Running' in prog:
            running_batches.append((os.path.basename(b), prog))
        elif 'Starting' in prog:
            starting_batches.append((os.path.basename(b), prog))
        elif 'Completed' in prog:
            completed_batches.append((os.path.basename(b), prog))
        elif 'Failed' in prog:
            failed_batches.append(os.path.basename(b))
        elif 'Not started' in prog:
            not_started_batches.append(os.path.basename(b))

    print("-" * 40)
    print("SUMMARY")
    print("-" * 40)
    print(f'Completed batches:   {len(completed_batches)}')
    print(f'Failed batches:      {len(failed_batches)}')
    print(f'Not started batches: {len(not_started_batches)}')
    print(f'Starting:            {len(starting_batches)}')
    print(f'Running:             {len(running_batches)}')
    print("-" * 40)

    running_batches_to_show = []
    for b, p in running_batches:
        if not args.details and "(0.0%)" in p:
            continue
        running_batches_to_show.append((b, p))

    if running_batches_to_show:
        if not args.details:
            print("\nRunning batches details (excluding 0.0%):")
        else:
            print("\nRunning batches details:")
        for b, p in running_batches_to_show:
            print(f"  {b}: {p}")
            
    if failed_batches and args.details:
        print("\nFailed batches:")
        for b in failed_batches:
            print(f"  {b}")
                
    if completed_batches and args.details:
        print("\nCompleted batches details:")
        for b, p in completed_batches:
            print(f"  {b}: {p}")

if __name__ == "__main__":
    main()
