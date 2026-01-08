#!/usr/bin/env python3
"""
Script to check batch completion status for a given tile across both scenarios.
Identifies which batch folders are incomplete when overall completion is not 100%.
"""

import xarray as xr
import numpy as np
import subprocess
import tempfile
import os
import sys
import argparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading

# Constants
BUCKET = 'circumpolar_model_output'
BASE_PATH = 'recent2'
SCENARIOS = [
    'ssp1_2_6_mri_esm2_0_split',
    'ssp5_8_5_mri_esm2_0_split'
]

class SimpleProgressTracker:
    """Simple progress tracker without external dependencies."""
    
    def __init__(self, total, desc="Progress"):
        self.total = total
        self.desc = desc
        self.completed = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        
    def update(self, n=1):
        """Update progress by n items."""
        with self.lock:
            self.completed += n
            self._print_progress()
    
    def _print_progress(self):
        """Print progress bar to stderr."""
        if self.total == 0:
            return
        
        percentage = (self.completed / self.total) * 100
        elapsed = time.time() - self.start_time
        
        # Calculate rate and ETA
        if self.completed > 0:
            rate = self.completed / elapsed
            remaining = (self.total - self.completed) / rate if rate > 0 else 0
            eta_str = f", ETA: {remaining:.0f}s"
        else:
            eta_str = ""
        
        # Create simple progress bar
        bar_length = 40
        filled = int(bar_length * self.completed / self.total)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        # Print to stderr so it doesn't interfere with regular output
        print(f"\r{self.desc}: {bar} {self.completed}/{self.total} ({percentage:.1f}%){eta_str}", 
              end='', file=sys.stderr, flush=True)
        
        # Print newline when complete
        if self.completed >= self.total:
            print(file=sys.stderr)

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

def list_batch_folders(tile, scenario):
    """List all batch folders for a given tile and scenario in GCP."""
    try:
        gcp_path = f"gs://{BUCKET}/{BASE_PATH}/{tile}/{scenario}/"
        result = subprocess.run(
            ['gsutil', 'ls', gcp_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse output to find batch_* directories
        lines = result.stdout.strip().split('\n')
        batch_folders = []
        
        for line in lines:
            # Extract folder name from path like gs://bucket/path/batch_0/
            match = re.search(r'/(batch_\d+)/?$', line)
            if match:
                batch_folders.append(match.group(1))
        
        # Sort by batch number
        batch_folders.sort(key=lambda x: int(x.split('_')[1]))
        return batch_folders
        
    except subprocess.CalledProcessError as e:
        print(f"Error listing batch folders for {tile}/{scenario}: {e.stderr}", file=sys.stderr)
        return []
    except FileNotFoundError:
        print("Error: gsutil not found. Please ensure gsutil is installed and in PATH.", file=sys.stderr)
        return []

def check_overall_completion(tile, scenario, temp_dir):
    """Check overall completion percentage for a tile/scenario combination.
    
    Returns:
        tuple: (completion_percentage, count_100, total_valid) or (None, None, None) on error
    """
    # Construct GCP paths
    run_status_gcp_path = f"gs://{BUCKET}/{BASE_PATH}/{tile}/{scenario}/all_merged/run_status.nc"
    
    # run-mask path: remove "_split" from scenario name
    scenario_base = scenario.replace('_split', '')
    run_mask_gcp_path = f"gs://{BUCKET}/{BASE_PATH}/{tile}/{scenario_base}/run-mask.nc"
    
    # Local temporary file paths
    run_status_local = os.path.join(temp_dir, f"{tile}_{scenario}_overall_run_status.nc")
    run_mask_local = os.path.join(temp_dir, f"{tile}_{scenario}_overall_run_mask.nc")
    
    # Download files
    if not download_file(run_status_gcp_path, run_status_local):
        return None, None, None
    
    if not download_file(run_mask_gcp_path, run_mask_local):
        return None, None, None
    
    # Calculate completion percentage
    return calculate_completion_percentage(run_status_local, run_mask_local)

def check_batch_completion(tile, scenario, batch_name, temp_dir):
    """Check completion percentage for a specific batch.
    
    Returns:
        tuple: (batch_name, completion_percentage, count_100, total_valid, error_msg)
    """
    try:
        # Construct GCP paths
        run_status_gcp_path = f"gs://{BUCKET}/{BASE_PATH}/{tile}/{scenario}/{batch_name}/output/run_status.nc"
        run_mask_gcp_path = f"gs://{BUCKET}/{BASE_PATH}/{tile}/{scenario}/{batch_name}/input/run-mask.nc"
        
        # Use process ID and timestamp for unique temporary file names (thread-safe)
        unique_id = f"{os.getpid()}_{time.time()}_{batch_name}"
        run_status_local = os.path.join(temp_dir, f"{tile}_{scenario}_{unique_id}_run_status.nc")
        run_mask_local = os.path.join(temp_dir, f"{tile}_{scenario}_{unique_id}_run_mask.nc")
        
        # Download files
        if not download_file(run_status_gcp_path, run_status_local):
            return (batch_name, None, None, None, "Failed to download run_status.nc")
        
        if not download_file(run_mask_gcp_path, run_mask_local):
            return (batch_name, None, None, None, "Failed to download run-mask.nc")
        
        # Calculate completion percentage
        completion_pct, count_100, total_valid = calculate_completion_percentage(run_status_local, run_mask_local)
        
        # Clean up temporary files
        try:
            os.remove(run_status_local)
            os.remove(run_mask_local)
        except:
            pass
        
        if completion_pct is None:
            return (batch_name, None, None, None, "Error calculating completion percentage")
        
        return (batch_name, completion_pct, count_100, total_valid, None)
        
    except Exception as e:
        return (batch_name, None, None, None, str(e))

def process_tile(tile, max_workers=10):
    """Process a single tile across both scenarios with parallel batch processing.
    
    Args:
        tile: Tile name (e.g., 'H19_V17')
        max_workers: Maximum number of parallel workers for batch processing
    """
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for scenario in SCENARIOS:
            print(f"\n{'='*80}")
            print(f"Checking tile: {tile}, scenario: {scenario}")
            print(f"{'='*80}")
            
            start_time = time.time()
            
            # Check overall completion
            completion_pct, count_100, total_valid = check_overall_completion(tile, scenario, temp_dir)
            
            if completion_pct is None:
                print(f"ERROR: Could not check overall completion for {tile}/{scenario}")
                continue
            
            print(f"Overall completion: {completion_pct:.2f}% ({count_100}/{total_valid} cells)")
            
            # If 100% complete, no need to check batches
            if completion_pct >= 99.99:  # Use 99.99 to account for floating point precision
                print(f"✓ Tile is 100% complete!")
                continue
            
            # Get list of batch folders
            print(f"\nFinding batch folders...")
            batch_folders = list_batch_folders(tile, scenario)
            
            if not batch_folders:
                print(f"ERROR: No batch folders found for {tile}/{scenario}")
                continue
            
            print(f"Found {len(batch_folders)} batch folders")
            print(f"Checking batches in parallel (using {max_workers} workers)...")
            
            # Process batches in parallel with progress tracking
            incomplete_batches = []
            error_batches = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batch check tasks
                future_to_batch = {
                    executor.submit(check_batch_completion, tile, scenario, batch_name, temp_dir): batch_name
                    for batch_name in batch_folders
                }
                
                # Process results as they complete with progress tracking
                progress = SimpleProgressTracker(total=len(batch_folders), desc="Processing batches")
                
                for future in as_completed(future_to_batch):
                    batch_name, batch_pct, batch_count, batch_total, error_msg = future.result()
                    
                    if error_msg:
                        error_batches.append((batch_name, error_msg))
                    elif batch_pct is not None and batch_pct < 99.99:
                        incomplete_batches.append((batch_name, batch_pct, batch_count, batch_total))
                    
                    progress.update(1)
            
            elapsed_time = time.time() - start_time
            
            # Sort incomplete batches by batch number
            incomplete_batches.sort(key=lambda x: int(x[0].split('_')[1]))
            error_batches.sort(key=lambda x: int(x[0].split('_')[1]))
            
            # Print results
            print(f"\n{'='*80}")
            print(f"Results for {tile}/{scenario}:")
            print(f"  Total batches: {len(batch_folders)}")
            print(f"  Complete batches: {len(batch_folders) - len(incomplete_batches) - len(error_batches)}")
            print(f"  Incomplete batches: {len(incomplete_batches)}")
            print(f"  Error batches: {len(error_batches)}")
            print(f"  Time taken: {elapsed_time:.1f} seconds")
            
            if incomplete_batches:
                print(f"\nIncomplete batches ({len(incomplete_batches)} total):")
                for batch_name, batch_pct, batch_count, batch_total in incomplete_batches:
                    print(f"  {batch_name}: {batch_pct:.2f}% ({batch_count}/{batch_total} cells)")
            
            if error_batches:
                print(f"\nBatches with errors ({len(error_batches)} total):")
                for batch_name, error_msg in error_batches:
                    print(f"  {batch_name}: {error_msg}")
            
            if not incomplete_batches and not error_batches:
                print(f"\n✓ All batches are complete!")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Check batch completion status for a tile across both scenarios',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default (10 workers)
  %(prog)s --tile H19_V17
  
  # More aggressive parallelism
  %(prog)s --tile H19_V17 --workers 20
  
  # Conservative parallelism
  %(prog)s --tile H19_V17 --workers 5
        """
    )
    parser.add_argument(
        '--tile',
        required=True,
        help='Tile name (e.g., H19_V17)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of parallel workers for batch processing (default: 10)'
    )
    
    args = parser.parse_args()
    
    if args.workers < 1:
        print("Error: --workers must be at least 1", file=sys.stderr)
        sys.exit(1)
    
    print(f"Processing tile: {args.tile}")
    print(f"Using {args.workers} parallel workers")
    
    process_tile(args.tile, max_workers=args.workers)
    
    print(f"\n{'='*80}")
    print("Done!")

if __name__ == "__main__":
    main()

# pick a batch that's finished 100%. download it, rerun it and interrupt it in the middle of the run. use that batch to test the script.
