#!/usr/bin/env python3
import xarray as xr
import numpy as np
import subprocess
import tempfile
import os
import sys
import argparse

# Scenario list
SCENARIOS = [
    'ssp1_2_6_mri_esm2_0_split',
    'ssp5_8_5_mri_esm2_0_split'
]

# GCP bucket and base path
BUCKET = 'circumpolar_model_output'
BASE_PATH = 'recent2'

def download_file(gcp_path, local_path):
    """Download a file from GCP bucket using gsutil."""
    try:
        result = subprocess.run(
            ['gsutil', 'cp', gcp_path, local_path],
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error downloading {gcp_path}: {e.stderr}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print("Error: gsutil not found. Please ensure gsutil is installed and in PATH.", file=sys.stderr)
        return False

def calculate_completion_percentage(run_status_path, run_mask_path):
    """Calculate completion percentage using run-mask to filter cells.
    
    Only includes cells where run-mask value is 1.
    Percentage = (cells with status=100 AND run-mask=1) / (total cells with run-mask=1) * 100
    """
    try:
        # Open run_status dataset
        ds_status = xr.open_dataset(run_status_path, decode_times=False)
        run_status = ds_status['run_status'].values
        
        # Open run-mask dataset
        ds_mask = xr.open_dataset(run_mask_path, decode_times=False)
        run_mask = ds_mask['run'].values
        
        # Ensure arrays have the same shape
        if run_status.shape != run_mask.shape:
            print(f"Error: Shape mismatch - run_status: {run_status.shape}, run_mask: {run_mask.shape}", file=sys.stderr)
            ds_status.close()
            ds_mask.close()
            return None, None, None
        
        # Create mask for cells that should be run (run-mask == 1)
        # Also exclude fill values from run_status (-9999) and run-mask (-999)
        valid_mask = (run_mask == 1) & (run_status != -9999) & (run_mask != -999)
        
        # Filter run_status to only include cells where run-mask == 1
        run_status_filtered = run_status[valid_mask]
        
        # Count cells with status=100 (success) among cells that should be run
        count_100 = np.sum(run_status_filtered == 100)
        
        # Total cells that should be run (where run-mask == 1)
        total_cells_to_run = run_status_filtered.size
        
        # Calculate completion percentage
        if total_cells_to_run > 0:
            completion_percentage = (count_100 / total_cells_to_run) * 100
        else:
            completion_percentage = 0.0
        
        ds_status.close()
        ds_mask.close()
        return completion_percentage, count_100, total_cells_to_run
        
    except Exception as e:
        print(f"Error reading files: {e}", file=sys.stderr)
        return None, None, None

def analyze_run_status_batch(tile_name):
    """Download and analyze run_status.nc files for a specific tile/scenario combinations.
    
    Uses run-mask.nc to filter which cells to include in the calculation.
    
    Args:
        tile_name: Tile name to process.
    """
    
    # Create temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        results = []
        
        for scenario in SCENARIOS:
            # Construct GCP paths
            # run_status path includes "_split" and "all_merged" subdirectory
            run_status_gcp_path = f"gs://{BUCKET}/{BASE_PATH}/{tile_name}/{scenario}/all_merged/run_status.nc"
            
            # run-mask path: remove "_split" from scenario name
            scenario_base = scenario.replace('_split', '')
            run_mask_gcp_path = f"gs://{BUCKET}/{BASE_PATH}/{tile_name}/{scenario_base}/run-mask.nc"
            
            # Local temporary file paths
            run_status_local = os.path.join(temp_dir, f"{tile_name}_{scenario}_run_status.nc")
            run_mask_local = os.path.join(temp_dir, f"{tile_name}_{scenario}_run_mask.nc")
            
            # Download run_status file
            # print(f"Downloading {run_status_gcp_path}...", file=sys.stderr)
            if not download_file(run_status_gcp_path, run_status_local):
                print(f"{tile_name},{scenario}", file=sys.stdout)
                continue
            
            # Download run-mask file
            # print(f"Downloading {run_mask_gcp_path}...", file=sys.stderr)
            if not download_file(run_mask_gcp_path, run_mask_local):
                print(f"{tile_name},{scenario}", file=sys.stdout)
                continue
            
            # Calculate completion percentage using both files
            completion_pct, count_100, total_valid = calculate_completion_percentage(
                run_status_local, run_mask_local
            )
            
            if completion_pct is not None:
                # Output: tile, scenario, and completion percentage
                print(f"{tile_name},{scenario},{completion_pct:.2f}", file=sys.stdout)
                results.append({
                    'tile': tile_name,
                    'scenario': scenario,
                    'completion': completion_pct,
                    'count_100': count_100,
                    'total_valid': total_valid
                })
            else:
                print(f"{tile_name},{scenario},ERROR", file=sys.stdout)
        
        return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze run status for tiles and scenarios')
    parser.add_argument('--tile', '-t', type=str, required=True, help='Tile name to process (e.g., H13_V17)')
    args = parser.parse_args()
    
    analyze_run_status_batch(tile_name=args.tile)
