#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import re
import time
from pathlib import Path
import xarray as xr
import numpy as np
from netCDF4 import Dataset
from datetime import datetime

class TeeOutput:
    """Simple class that writes to both file and stdout/stderr."""
    def __init__(self, file_handle, original_stream):
        self.file = file_handle
        self.original = original_stream
    
    def write(self, data):
        self.original.write(data)
        self.file.write(data)
        self.file.flush()
    
    def flush(self):
        self.original.flush()
        self.file.flush()

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

def run_extract_failed_cells(batch_path, script_path=None, submit=False):
    """
    Run extract_failed_cells.py on a batch.
    
    Args:
        batch_path: Path to the batch directory
        script_path: Path to extract_failed_cells.py script (if None, tries to find it)
        submit: If True, pass --submit flag to extract_failed_cells.py to auto-submit the job
        
    Returns:
        tuple: (success, job_id) where success is bool and job_id is string or None
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
        return False, None
    
    try:
        # Run the script with --force flag to overwrite existing retry directory
        cmd = [sys.executable, str(script_path), str(batch_path), "--force"]
        if submit:
            cmd.append("--submit")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            job_id = None
            if submit:
                # Extract job ID from output
                # Look for patterns like "Job ID: 12345" or "Submitted batch job 12345"
                # Try multiple patterns to be robust
                job_match = re.search(r'Job ID:\s*(\d+)', result.stdout)
                if not job_match:
                    job_match = re.search(r'Submitted batch job\s+(\d+)', result.stdout)
                if not job_match:
                    # Fallback: look for any standalone number that might be a job ID
                    job_match = re.search(r'\b(\d{4,})\b', result.stdout)
                if job_match:
                    job_id = job_match.group(1)
                print(f"✓ Created and submitted retry batch for {batch_path}")
                if job_id:
                    print(f"  Job ID: {job_id}")
            else:
                print(f"✓ Created retry batch for {batch_path}")
            return True, job_id
        else:
            print(f"✗ Failed to create retry batch for {batch_path}", file=sys.stderr)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            return False, None
    except Exception as e:
        print(f"Error running extract_failed_cells.py for {batch_path}: {e}", file=sys.stderr)
        return False, None


def check_job_status(job_id):
    """
    Check the status of a SLURM job using sacct (can show both running and completed jobs).
    
    Args:
        job_id: SLURM job ID as string
        
    Returns:
        str: Job status ('RUNNING', 'PENDING', 'COMPLETED', 'FAILED', 'CANCELLED', 'UNKNOWN', or None on error)
    """
    try:
        # Use sacct -X to get only the main job (not array steps)
        # This works for both running and completed jobs
        # -n suppresses the header, -o State outputs only the State column
        result = subprocess.run(
            ['sacct', '-X', '-j', str(job_id), '-n', '-o', 'State'],
            capture_output=True,
            text=True,
            check=True
        )
        
        if not result.stdout.strip():
            # No output - job might not exist yet or hasn't been recorded
            return 'UNKNOWN'
        
        # Parse the output - sacct returns whitespace-separated values, take the first line
        # Output format: "   RUNNING " (with leading/trailing whitespace)
        status_line = result.stdout.strip().split('\n')[0]
        status = status_line.strip().upper()
        
        # Map SLURM states to our status codes
        if status in ['COMPLETED', 'CD']:
            return 'COMPLETED'
        elif status in ['RUNNING', 'R']:
            return 'RUNNING'
        elif status in ['PENDING', 'PD']:
            return 'PENDING'
        elif status in ['FAILED', 'F']:
            return 'FAILED'
        elif status in ['CANCELLED', 'CA']:
            return 'CANCELLED'
        elif status in ['TIMEOUT', 'TO']:
            return 'FAILED'
        elif status in ['NODE_FAIL', 'NF']:
            return 'FAILED'
        elif status in ['BOOT_FAIL', 'BF']:
            return 'FAILED'
        else:
            # Unknown state, return as-is but log it
            return status
        
    except subprocess.CalledProcessError as e:
        # Command failed - job might not exist or SLURM commands unavailable
        # Don't print error here as it might be normal (job not recorded yet)
        return None
    except FileNotFoundError:
        print(f"Error: 'sacct' command not found. Is SLURM installed?", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error checking job status for {job_id}: {e}", file=sys.stderr)
        return None


def merge_retry_results(batch_path, script_path=None):
    """
    Merge retry results back into original batch using extract_failed_cells.py --merge.
    
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
        # Run the script with --merge flag
        cmd = [sys.executable, str(script_path), str(batch_path), "--merge"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ Merged retry results for {batch_path}")
            return True
        else:
            print(f"✗ Failed to merge retry results for {batch_path}", file=sys.stderr)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            return False
    except Exception as e:
        print(f"Error merging retry results for {batch_path}: {e}", file=sys.stderr)
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
    parser.add_argument(
        '--submit',
        action='store_true',
        help='Automatically submit slurm jobs for retry batches after creating them (requires --individual-retry)'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        help='Path to log file where all output will be saved (also displayed on console)'
    )
    
    args = parser.parse_args()
    
    # Set up logging to file if requested (simple redirect)
    log_file_handle = None
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    
    if args.log_file:
        log_file_handle = open(args.log_file, 'a', buffering=1)
        sys.stdout = TeeOutput(log_file_handle, original_stdout)
        sys.stderr = TeeOutput(log_file_handle, original_stderr)
        print(f"Logging to file: {args.log_file}")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
    
    try:
        # Validate that --submit requires --individual-retry
        if args.submit and not args.individual_retry:
            print("Error: --submit flag requires --individual-retry flag", file=sys.stderr)
            sys.exit(1)
        
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
                if args.submit:
                    print(f"Creating and submitting retry batches for {len(unfinished_batches)} unfinished batch(es)...")
                else:
                    print(f"Creating retry batches for {len(unfinished_batches)} unfinished batch(es)...")
                print(f"{'='*80}")
                
                # Track job IDs when submitting
                job_tracking = {}  # {job_id: batch_path}
                
                for batch_path in unfinished_batches:
                    print(f"\nProcessing: {batch_path}")
                    success, job_id = run_extract_failed_cells(batch_path, submit=args.submit)
                    
                    if success and args.submit and job_id:
                        job_tracking[job_id] = batch_path
                
                print(f"\n{'='*80}")
                if args.submit:
                    print(f"Finished creating and submitting {len(unfinished_batches)} unfinished batch(es)")
                    if job_tracking:
                        print(f"Tracking {len(job_tracking)} job(s) for monitoring...")
                else:
                    print(f"Finished processing {len(unfinished_batches)} unfinished batch(es)")
                print(f"{'='*80}")
                
                # Monitor jobs and merge results when they complete
                if args.submit and job_tracking:
                    print(f"\n{'='*80}")
                    print("Starting job monitoring (checking every 5 minutes)...")
                    print(f"{'='*80}\n")
                    
                    poll_interval = 300  # 5 minutes in seconds
                    completed_count = 0
                    failed_count = 0
                    
                    while job_tracking:
                        # Check status of all tracked jobs
                        jobs_to_remove = []
                        
                        for job_id, batch_path in job_tracking.items():
                            status = check_job_status(job_id)
                            
                            if status == 'COMPLETED':
                                print(f"\n✓ Job {job_id} completed for {batch_path}")
                                # Merge retry results
                                if merge_retry_results(batch_path):
                                    completed_count += 1
                                else:
                                    print(f"  Warning: Merge failed for {batch_path}")
                                jobs_to_remove.append(job_id)
                                
                            elif status == 'FAILED':
                                print(f"\n✗ Job {job_id} failed for {batch_path}")
                                failed_count += 1
                                jobs_to_remove.append(job_id)
                                
                            elif status == 'CANCELLED':
                                print(f"\n⚠ Job {job_id} was cancelled for {batch_path}")
                                failed_count += 1
                                jobs_to_remove.append(job_id)
                                
                            elif status in ['RUNNING', 'PENDING']:
                                # Job still active, keep tracking
                                pass
                                
                            elif status is None:
                                # Error checking status, but keep tracking for now
                                print(f"  Warning: Could not check status for job {job_id}")
                        
                        # Remove completed/failed jobs from tracking
                        for job_id in jobs_to_remove:
                            del job_tracking[job_id]
                        
                        # If there are still jobs running, wait before next check
                        if job_tracking:
                            remaining_jobs = list(job_tracking.keys())
                            print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Still monitoring {len(job_tracking)} job(s): {', '.join(remaining_jobs)}")
                            print(f"Next check in {poll_interval // 60} minutes...")
                            time.sleep(poll_interval)
                    
                    # Final summary
                    print(f"\n{'='*80}")
                    print("Job monitoring complete!")
                    print(f"  Completed: {completed_count}")
                    print(f"  Failed/Cancelled: {failed_count}")
                    print(f"{'='*80}\n")
                    
            else:
                print("\n✓ All batches are finished - no retry batches needed.")
    
    finally:
        # Restore original stdout/stderr and close log file
        if log_file_handle:
            print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            log_file_handle.close()


# add --dry-run flag to check if a tile will be retried. percentage > 70%, we'll submit. no job submission.
