#!/usr/bin/env python3
"""
Script to check tile completion status for SSP scenarios and automatically fix failed tiles.

Usage:
    # Check tiles from a file
    python debug/fix_tile.py tiles/file_name.txt
    
    # Check a single tile
    python debug/fix_tile.py --tile H7_V8
    
    # Check and automatically fix failed tiles
    python debug/fix_tile.py --tile H7_V8 --fix

This script:
1. Reads a list of tiles from a file OR accepts a single tile name
2. For each tile, runs analyze_run_status_batch.py for ssp_1_2_6 and ssp_5_8_5
3. Checks if completion is >99%, prints "passed" or "failed" for each scenario
4. If --fix flag is set, automatically pulls failed tiles and reruns them
"""

import sys
import subprocess
import os
import argparse
import tempfile
from pathlib import Path
import xarray as xr
import numpy as np


# Scenario mappings: short name -> full scenario name
SCENARIO_MAP = {
    'ssp_1_2_6': 'ssp1_2_6_mri_esm2_0_split',
    'ssp_5_8_5': 'ssp5_8_5_mri_esm2_0_split'
}

THRESHOLD = 99.0  # Completion percentage threshold

# Default bucket configuration
DEFAULT_BUCKET = 'circumpolar_model_output/recent2'


def read_tile_list(tile_file):
    """Read list of tiles from a file.
    
    Args:
        tile_file: Path to file containing tile names (one per line)
        
    Returns:
        List of tile names
    """
    tiles = []
    try:
        with open(tile_file, 'r') as f:
            for line in f:
                tile = line.strip()
                if tile and not tile.startswith('#'):  # Skip empty lines and comments
                    tiles.append(tile)
        return tiles
    except FileNotFoundError:
        print(f"Error: Tile file '{tile_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading tile file: {e}", file=sys.stderr)
        sys.exit(1)


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
            ds_status.close()
            ds_mask.close()
            return None
        
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
        return completion_percentage
        
    except Exception as e:
        print(f"Error reading files: {e}", file=sys.stderr)
        return None


def analyze_tile_completion(tile_name, bucket_path):
    """Download and analyze run_status.nc files for a specific tile.
    
    Uses run-mask.nc to filter which cells to include in the calculation.
    
    Args:
        tile_name: Tile name to process
        bucket_path: GCS bucket path (e.g., 'circumpolar_model_output/recent2')
        
    Returns:
        Dictionary mapping scenario names to completion percentages or None if error
    """
    bucket, base_path = bucket_path.split('/', 1) if '/' in bucket_path else (bucket_path, '')
    
    # Create temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        completions = {}
        
        for scenario_full_name in SCENARIO_MAP.values():
            # Construct GCP paths
            if base_path:
                run_status_gcp_path = f"gs://{bucket}/{base_path}/{tile_name}/{scenario_full_name}/all_merged/run_status.nc"
            else:
                run_status_gcp_path = f"gs://{bucket}/{tile_name}/{scenario_full_name}/all_merged/run_status.nc"
            
            # run-mask path: remove "_split" from scenario name
            scenario_base = scenario_full_name.replace('_split', '')
            if base_path:
                run_mask_gcp_path = f"gs://{bucket}/{base_path}/{tile_name}/{scenario_base}/run-mask.nc"
            else:
                run_mask_gcp_path = f"gs://{bucket}/{tile_name}/{scenario_base}/run-mask.nc"
            
            # Local temporary file paths
            run_status_local = os.path.join(temp_dir, f"{tile_name}_{scenario_full_name}_run_status.nc")
            run_mask_local = os.path.join(temp_dir, f"{tile_name}_{scenario_full_name}_run_mask.nc")
            
            # Download run_status file
            if not download_file(run_status_gcp_path, run_status_local):
                completions[scenario_full_name] = None
                continue
            
            # Download run-mask file
            if not download_file(run_mask_gcp_path, run_mask_local):
                completions[scenario_full_name] = None
                continue
            
            # Calculate completion percentage using both files
            completion_pct = calculate_completion_percentage(run_status_local, run_mask_local)
            completions[scenario_full_name] = completion_pct
        
        return completions


def pull_tile_from_bucket(bucket_path, tile_name, working_dir=None):
    """
    Pull existing tile from GCS bucket and create tile_name_sc directory.
    
    Args:
        bucket_path: GCS bucket path (e.g., 'circumpolar_model_output/recent2')
        tile_name: Name of the tile to pull
        working_dir: Working directory where to create tile_name_sc folder (defaults to current dir)
        
    Returns:
        Path to the created tile directory or None if failed
    """
    if working_dir is None:
        working_dir = os.getcwd()
    
    tile_dir = os.path.join(working_dir, f"{tile_name}_sc")
    
    # Create the directory
    os.makedirs(tile_dir, exist_ok=True)
    
    try:
        gcs_path = f"gs://{bucket_path}/{tile_name}"
        print(f"Pulling {gcs_path}...")
        
        result = subprocess.run(
            ['gsutil', '-m', 'cp', '-r', f'{gcs_path}/*', f'{tile_dir}/'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print(f"✓ Successfully pulled tile to {tile_dir}")
        return tile_dir
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Error pulling tile from bucket: {e.stderr}", file=sys.stderr)
        return None
    except FileNotFoundError:
        print("✗ Error: gsutil not found. Please ensure gsutil is installed and in PATH.", file=sys.stderr)
        return None


def run_batch_retry(tile_dir, scenario, partition='spot', submit=False):
    """
    Run batch_status_checker.py with --individual-retry for a specific scenario.
    
    Args:
        tile_dir: Path to tile directory (e.g., H7_V8_sc)
        scenario: Scenario name (full name with _split suffix)
        partition: SLURM partition to use (default: 'spot')
        submit: If True, automatically submit SLURM jobs (default: False)
        
    Returns:
        bool: True if successful, False otherwise
    """
    tile_name = os.path.basename(tile_dir).replace('_sc', '')
    scenario_path = os.path.join(tile_dir, scenario)
    
    if not os.path.exists(scenario_path):
        print(f"✗ Scenario path not found: {scenario_path}", file=sys.stderr)
        return False
    
    # Find batch_status_checker.py
    current_script_dir = Path(__file__).parent
    batch_checker_path = current_script_dir / "batch_status_checker.py"
    
    if not batch_checker_path.exists():
        print(f"✗ batch_status_checker.py not found at {batch_checker_path}", file=sys.stderr)
        return False
    
    # Create LOG directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), 'LOG')
    os.makedirs(log_dir, exist_ok=True)
    
    # Construct log file path
    log_file = os.path.join(log_dir, f"{tile_name}_debug_{scenario}.log")
    
    print(f"\nRunning batch retry for {scenario}...")
    print(f"Log file: {log_file}")
    
    try:
        cmd = [
            sys.executable,
            str(batch_checker_path),
            '--individual-retry',
            '--log-file', log_file,
            '-p', partition,
            scenario_path
        ]
        
        # Add --submit flag if enabled
        if submit:
            cmd.insert(4, '--submit')  # Insert after --individual-retry
        
        print(f"Command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        
        if result.returncode == 0:
            print(f"✓ Batch retry completed for {scenario}")
            return True
        else:
            print(f"✗ Batch retry failed for {scenario}", file=sys.stderr)
            return False
            
    except Exception as e:
        print(f"✗ Error running batch retry: {e}", file=sys.stderr)
        return False


def check_tile_completion(tile_name, fix_failed=False, bucket_path=None, partition='spot', submit=False):
    """Check completion status for a tile across both SSP scenarios and optionally fix failures.
    
    Args:
        tile_name: Name of the tile to check
        fix_failed: If True, automatically pull and retry failed tiles
        bucket_path: GCS bucket path (required)
        partition: SLURM partition to use for retries
        submit: If True, automatically submit SLURM jobs
        
    Returns:
        Dictionary with scenario completion status
    """
    print(f"\n{'='*80}")
    print(f"Tile: {tile_name}")
    print(f"{'='*80}")
    
    # Analyze tile completion directly
    completions = analyze_tile_completion(tile_name, bucket_path)
    
    # Track failed scenarios
    failed_scenarios = []
    results = {}
    
    # Check each scenario
    for short_name, full_name in SCENARIO_MAP.items():
        status = "FAILED"
        completion_str = "N/A"
        
        if full_name in completions and completions[full_name] is not None:
            completion = completions[full_name]
            completion_str = f"{completion:.2f}%"
            
            if completion > THRESHOLD:
                status = "PASSED"
            else:
                failed_scenarios.append((short_name, full_name))
        else:
            completion_str = "Not found/Error"
            failed_scenarios.append((short_name, full_name))
        
        print(f"  {short_name:15s}: {completion_str:15s} [{status}]")
        results[short_name] = {'status': status, 'completion': completion_str}
    
    # If fix_failed is enabled and there are failures, attempt to fix them
    if fix_failed and failed_scenarios:
        print(f"\n{'='*80}")
        print(f"Attempting to fix {len(failed_scenarios)} failed scenario(s)...")
        print(f"{'='*80}")
        
        # Check if tile directory already exists (tile_name or tile_name_sc)
        working_dir = os.getcwd()
        tile_dir = os.path.join(working_dir, f"{tile_name}_sc")
        tile_dir_alt = os.path.join(working_dir, tile_name)
        
        if os.path.exists(tile_dir):
            print(f"✓ Tile directory already exists: {tile_dir}")
            print(f"  Skipping pull, using existing directory")
        elif os.path.exists(tile_dir_alt):
            print(f"✓ Tile directory already exists: {tile_dir_alt}")
            print(f"  Skipping pull, using existing directory")
            tile_dir = tile_dir_alt
        else:
            # Pull the tile
            print(f"Pulling tile from bucket...")
            tile_dir = pull_tile_from_bucket(bucket_path, tile_name)
            
            if tile_dir is None:
                print(f"✗ Failed to pull tile, cannot proceed with fix", file=sys.stderr)
                return results
        
        # Run batch retry for each failed scenario
        for short_name, full_name in failed_scenarios:
            print(f"\n--- Fixing scenario: {short_name} ({full_name}) ---")
            success = run_batch_retry(tile_dir, full_name, partition, submit)
            
            if success:
                print(f"✓ Successfully initiated retry for {short_name}")
            else:
                print(f"✗ Failed to initiate retry for {short_name}")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Check tile completion status for SSP scenarios and optionally fix failures',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check a single tile
  python debug/fix_tile.py --tile H7_V8
  
  # Check and fix a single tile (create retry batches only)
  python debug/fix_tile.py --tile H7_V8 --fix
  
  # Check, fix, and auto-submit SLURM jobs
  python debug/fix_tile.py --tile H7_V8 --fix --submit
  
  # Check tiles from a file
  python debug/fix_tile.py tiles/unfinished_ak_can.txt
  
  # Check, fix, and submit with custom partition
  python debug/fix_tile.py tiles/test_tile.txt --fix --submit --partition dask
        """
    )
    
    # Create mutually exclusive group for tile input
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        'tile_file',
        nargs='?',
        help='Path to file containing tile names (one per line)'
    )
    input_group.add_argument(
        '--tile', '-t',
        help='Single tile name to check (e.g., H7_V8)'
    )
    
    parser.add_argument(
        '--fix',
        action='store_true',
        help='Automatically pull and retry failed tiles'
    )
    
    parser.add_argument(
        '--submit',
        action='store_true',
        help='Automatically submit SLURM jobs for retry batches (requires --fix)'
    )
    
    parser.add_argument(
        '--bucket-path',
        default=DEFAULT_BUCKET,
        help=f'GCS bucket path (default: {DEFAULT_BUCKET})'
    )
    
    parser.add_argument(
        '-p', '--partition',
        default='spot',
        help='SLURM partition to use for retry jobs (default: spot)'
    )
    
    args = parser.parse_args()
    
    # Validate that --submit requires --fix
    if args.submit and not args.fix:
        print("Error: --submit flag requires --fix flag", file=sys.stderr)
        sys.exit(1)
    
    # Get tile list - either from file or single tile
    if args.tile:
        tiles = [args.tile]
        print(f"Checking single tile: {args.tile}")
    else:
        tiles = read_tile_list(args.tile_file)
        print(f"Found {len(tiles)} tiles to process from file")
    
    if not tiles:
        print("No tiles to process.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Bucket: gs://{args.bucket_path}")
    if args.fix:
        print(f"Fix mode enabled - will automatically retry failed tiles")
        print(f"Partition: {args.partition}")
        if args.submit:
            print(f"Auto-submit enabled - SLURM jobs will be submitted automatically")
    
    # Process each tile
    for tile in tiles:
        check_tile_completion(
            tile, 
            fix_failed=args.fix,
            bucket_path=args.bucket_path,
            partition=args.partition,
            submit=args.submit
        )
    
    print(f"\n{'='*80}")
    print("Completion check finished")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
