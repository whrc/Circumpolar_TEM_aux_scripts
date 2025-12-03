#!/usr/bin/env python3
"""
Script to extract failed cells from a batch and create a retry batch.

This script reads run_status.nc to identify cells that failed (status != 100 and != 0),
then creates a retry batch with only those cells enabled in the run-mask.nc.

Usage:
    python extract_failed_cells.py <batch_path> [options]
    
Examples:
    # Basic usage
    python extract_failed_cells.py ~/test_batches/batch_18
    
    # Dry run to preview changes
    python extract_failed_cells.py ~/test_batches/batch_18 --dry-run
    
    # Force overwrite existing retry directory
    python extract_failed_cells.py ~/test_batches/batch_18 --force
"""

import os
import sys
import argparse
import shutil
import re
import json
from pathlib import Path
import xarray as xr
import numpy as np


def validate_batch_structure(batch_path):
    """
    Validate that the batch directory has the required structure.
    
    Args:
        batch_path (Path): Path to the batch directory
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not batch_path.exists():
        return False, f"Batch directory does not exist: {batch_path}"
    
    if not batch_path.is_dir():
        return False, f"Path is not a directory: {batch_path}"
    
    # Check for required subdirectories
    input_dir = batch_path / "input"
    output_dir = batch_path / "output"
    
    if not input_dir.exists() or not input_dir.is_dir():
        return False, f"Missing input directory: {input_dir}"
    
    if not output_dir.exists() or not output_dir.is_dir():
        return False, f"Missing output directory: {output_dir}"
    
    # Check for required files
    run_status_file = output_dir / "run_status.nc"
    run_mask_file = input_dir / "run-mask.nc"
    
    if not run_status_file.exists():
        return False, f"Missing run_status.nc: {run_status_file}"
    
    if not run_mask_file.exists():
        return False, f"Missing run-mask.nc: {run_mask_file}"
    
    return True, None


def identify_failed_cells(batch_path):
    """
    Identify failed cells by reading run_status.nc and run-mask.nc.
    
    Failed cells are those where:
    - run_status != 100 (not successful)
    - run_status != 0 (not originally masked)
    
    Args:
        batch_path (Path): Path to the batch directory
        
    Returns:
        tuple: (run_status_array, run_mask_array, stats_dict) or (None, None, None) on error
        stats_dict contains: total_cells, masked_cells, successful_cells, failed_cells, 
                            failed_indices (list of tuples)
    """
    try:
        # Paths to files
        run_status_file = batch_path / "output" / "run_status.nc"
        run_mask_file = batch_path / "input" / "run-mask.nc"
        
        # Read run_status
        ds_status = xr.open_dataset(run_status_file, decode_times=False)
        run_status = ds_status['run_status'].values
        
        # Read run-mask
        ds_mask = xr.open_dataset(run_mask_file, decode_times=False)
        run_mask = ds_mask['run'].values
        
        # Validate shapes match
        if run_status.shape != run_mask.shape:
            print(f"Error: Shape mismatch!", file=sys.stderr)
            print(f"  run_status shape: {run_status.shape}", file=sys.stderr)
            print(f"  run_mask shape: {run_mask.shape}", file=sys.stderr)
            ds_status.close()
            ds_mask.close()
            return None, None, None
        
        # Calculate statistics
        total_cells = run_status.size
        
        # Masked cells (originally not meant to run)
        masked_cells = np.sum(run_status == 0)
        
        # Successful cells
        successful_cells = np.sum(run_status == 100)
        
        # Failed cells (status != 100 AND status != 0, including NaN values)
        # NaN values indicate something went wrong and should be treated as failures
        failed_mask = (run_status != 100) & (run_status != 0)
        # Also include NaN values as failures
        failed_mask = failed_mask | np.isnan(run_status)
        
        # Count all failed cells
        failed_cells_total = np.sum(failed_mask)
        
        # Count only failed cells that were supposed to run (run_mask=1)
        failed_cells_to_retry = np.sum(failed_mask & (run_mask == 1))
        failed_cells = failed_cells_total
        
        # Get indices of failed cells
        failed_indices = np.argwhere(failed_mask)
        
        # Get unique status codes for failed cells
        failed_status_codes = run_status[failed_mask]
        unique_codes = {}
        for code in np.unique(failed_status_codes):
            # Handle NaN separately
            if np.isnan(code):
                count = np.sum(np.isnan(failed_status_codes))
                unique_codes['NaN'] = int(count)
            else:
                count = np.sum(failed_status_codes == code)
                unique_codes[int(code)] = int(count)
        
        stats = {
            'total_cells': int(total_cells),
            'masked_cells': int(masked_cells),
            'successful_cells': int(successful_cells),
            'failed_cells': int(failed_cells),
            'failed_cells_to_retry': int(failed_cells_to_retry),
            'failed_indices': failed_indices.tolist(),
            'failed_status_codes': unique_codes
        }
        
        ds_status.close()
        ds_mask.close()
        
        return run_status, run_mask, stats
        
    except Exception as e:
        print(f"Error reading batch files: {e}", file=sys.stderr)
        return None, None, None


def create_retry_batch(batch_path, retry_path, force=False, dry_run=False):
    """
    Create a retry batch by copying the entire batch structure.
    
    Args:
        batch_path (Path): Path to the source batch directory
        retry_path (Path): Path to the retry directory
        force (bool): If True, overwrite existing retry directory
        dry_run (bool): If True, don't actually create files
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Check if retry directory already exists
    if retry_path.exists():
        if not force:
            print(f"Error: Retry directory already exists: {retry_path}", file=sys.stderr)
            print("Use --force to overwrite", file=sys.stderr)
            return False
        else:
            if dry_run:
                print(f"[DRY RUN] Would remove existing retry directory: {retry_path}")
            else:
                print(f"Removing existing retry directory: {retry_path}")
                shutil.rmtree(retry_path)
    
    if dry_run:
        print(f"[DRY RUN] Would copy batch structure from {batch_path} to {retry_path}")
        return True
    
    try:
        print(f"Copying batch structure to: {retry_path}")
        
        # Copy the entire batch directory, but ignore the retry directory if it exists
        def ignore_retry_dir(directory, files):
            """Ignore function to skip the retry directory during copy."""
            # If we're in the batch root and there's a 'retry' folder, ignore it
            if Path(directory) == batch_path and 'retry' in files:
                return ['retry']
            return []
        
        shutil.copytree(batch_path, retry_path, ignore=ignore_retry_dir, dirs_exist_ok=False)
        
        print(f"✓ Batch structure copied successfully")
        return True
        
    except Exception as e:
        print(f"Error copying batch structure: {e}", file=sys.stderr)
        return False


def update_retry_run_mask(retry_path, run_status, run_mask_original, dry_run=False):
    """
    Update the run-mask.nc in the retry batch to disable successful cells.
    
    Sets run=0 for cells where run_status == 100 (successful)
    Keeps run=1 for cells where run_status != 100 AND != 0 (failed)
    Keeps run=0 for cells where run_status == 0 (originally masked)
    
    Args:
        retry_path (Path): Path to the retry batch directory
        run_status (np.ndarray): Array of run status codes
        run_mask_original (np.ndarray): Original run mask array
        dry_run (bool): If True, don't actually modify files
        
    Returns:
        tuple: (success, cells_disabled, cells_enabled)
    """
    try:
        run_mask_file = retry_path / "input" / "run-mask.nc"
        
        if dry_run:
            # Use the original run_mask for calculation
            run_mask = run_mask_original
            
            # Calculate changes
            cells_to_disable = (run_status == 100) & (run_mask == 1)
            
            # Failed cells: not successful (!=100), not masked (!=0), including NaN values
            failed_cells_mask = ((run_status != 100) & (run_status != 0)) | np.isnan(run_status)
            cells_to_keep_enabled = failed_cells_mask & (run_mask == 1)
            
            cells_disabled = int(np.sum(cells_to_disable))
            cells_enabled = int(np.sum(cells_to_keep_enabled))
            
            print(f"[DRY RUN] Would update run-mask.nc:")
            print(f"  - Would disable {cells_disabled} successful cells")
            print(f"  - Would keep {cells_enabled} failed cells enabled")
            
            return True, cells_disabled, cells_enabled
        
        # Read the run-mask
        ds = xr.open_dataset(run_mask_file, decode_times=False)
        run_mask = ds['run'].values.copy()
        
        # Track changes
        cells_to_disable = (run_status == 100) & (run_mask == 1)
        
        # Failed cells: not successful (!=100), not masked (!=0), including NaN values
        # For NaN values, standard comparisons don't work, so we need to check explicitly
        failed_cells_mask = ((run_status != 100) & (run_status != 0)) | np.isnan(run_status)
        cells_to_keep_enabled = failed_cells_mask & (run_mask == 1)
        
        cells_disabled = int(np.sum(cells_to_disable))
        cells_enabled = int(np.sum(cells_to_keep_enabled))
        
        # Update run mask: set run=0 where status=100 (successful cells)
        run_mask[run_status == 100] = 0
        
        # Create updated dataset
        ds_updated = ds.copy()
        ds_updated['run'].values[:] = run_mask
        
        # Close the original dataset to release any locks
        ds.close()
        
        # Write to a temporary file first
        temp_file = run_mask_file.parent / f".{run_mask_file.name}.tmp"
        ds_updated.to_netcdf(temp_file)
        ds_updated.close()
        
        # Remove the original and rename the temp file
        run_mask_file.unlink()
        temp_file.rename(run_mask_file)
        
        print(f"✓ Updated run-mask.nc:")
        print(f"  - Disabled {cells_disabled} successful cells")
        print(f"  - Kept {cells_enabled} failed cells enabled")
        
        return True, cells_disabled, cells_enabled
        
    except Exception as e:
        print(f"Error updating run-mask: {e}", file=sys.stderr)
        return False, 0, 0


def update_retry_slurm_runner(retry_path, batch_path, dry_run=False):
    """
    Update the slurm_runner.sh file in the retry batch to use retry paths.
    
    Updates:
    - The log file path (-o) to use retry_path's logs directory
    - The config file path (-f) to use retry_path's config directory
    
    Args:
        retry_path (Path): Path to the retry batch directory
        batch_path (Path): Path to the source batch directory (for reference)
        dry_run (bool): If True, don't actually modify files
        
    Returns:
        bool: True if successful, False otherwise
    """
    slurm_runner_file = retry_path / "slurm_runner.sh"
    
    if not slurm_runner_file.exists():
        print(f"Warning: slurm_runner.sh not found: {slurm_runner_file}", file=sys.stderr)
        return False
    
    try:
        # Get absolute path of retry_path
        retry_path_abs = retry_path.resolve()
        
        # Read the file
        with open(slurm_runner_file, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Update the config file path (-f)
        # Replace entire old path with retry_path/config/config.js
        config_pattern = r'(-f\s+)([^\s]+)(\s|$)'
        
        def replace_config_path(match):
            prefix = match.group(1)  # -f 
            old_path = match.group(2)  # old full path
            suffix = match.group(3)  # whitespace or end of line
            
            # Replace with absolute retry_path + /config/config.js
            new_path = str(retry_path_abs / "config" / "config.js")
            return f"{prefix}{new_path}{suffix}"
        
        content = re.sub(config_pattern, replace_config_path, content)
        
        # Update the log file path (-o)
        # Extract batch number from the old path, then replace with retry_path's logs
        log_pattern = r'(-o\s+)([^\s]+)(\s|$)'
        
        def replace_log_path(match):
            prefix = match.group(1)  # -o 
            old_path = match.group(2)  # old full path
            suffix = match.group(3)  # whitespace or end of line
            
            # Extract batch number from old path (look for batch-XX or batch_XX)
            batch_match = re.search(r'batch[_-](\d+)', old_path)
            batch_num = batch_match.group(1) if batch_match else "retry"
            
            # Replace with retry_path's parent logs directory + batch-XX-retry
            # Logs are typically at the same level as the batch directory
            logs_dir = retry_path_abs.parent
            new_path = str(logs_dir / f"batch-{batch_num}-retry")
            return f"{prefix}{new_path}{suffix}"
        
        content = re.sub(log_pattern, replace_log_path, content)
        
        # Also update job name if it exists
        # Pattern: --job-name=...-batch-XX or --job-name=...-batch_XX
        job_name_pattern = r'(--job-name=[^-]+-batch[_-])(\d+)(\s|$)'
        
        def replace_job_name(match):
            prefix = match.group(1)  # --job-name=...-batch- or --job-name=...-batch_
            batch_num = match.group(2)  # 18
            suffix = match.group(3)  # whitespace or end of line
            
            # Add -retry to the job name
            new_name = f"{prefix}{batch_num}-retry{suffix}"
            return new_name
        
        content = re.sub(job_name_pattern, replace_job_name, content)
        
        if content == original_content:
            print(f"Warning: No changes detected in slurm_runner.sh", file=sys.stderr)
            return True
        
        if dry_run:
            print(f"[DRY RUN] Would update slurm_runner.sh:")
            config_new = str(retry_path_abs / "config" / "config.js")
            print(f"  - Would replace config path (-f) with: {config_new}")
            batch_match = re.search(r'batch[_-](\d+)', original_content)
            batch_num = batch_match.group(1) if batch_match else "retry"
            log_new = str(retry_path_abs.parent / f"batch-{batch_num}-retry")
            print(f"  - Would replace log path (-o) with: {log_new}")
            return True
        
        # Write the updated content
        with open(slurm_runner_file, 'w') as f:
            f.write(content)
        
        config_new = str(retry_path_abs / "config" / "config.js")
        batch_match = re.search(r'batch[_-](\d+)', original_content)
        batch_num = batch_match.group(1) if batch_match else "retry"
        log_new = str(retry_path_abs.parent / f"batch-{batch_num}-retry")
        
        print(f"✓ Updated slurm_runner.sh:")
        print(f"  - Replaced config path (-f) with: {config_new}")
        print(f"  - Replaced log path (-o) with: {log_new}")
        
        return True
        
    except Exception as e:
        print(f"Error updating slurm_runner.sh: {e}", file=sys.stderr)
        return False


def update_retry_config(retry_path, batch_path, dry_run=False):
    """
    Update the config.js file in the retry batch to use retry paths.
    
    Replaces all paths that point to the original batch directory with
    paths pointing to the retry directory.
    
    Args:
        retry_path (Path): Path to the retry batch directory
        batch_path (Path): Path to the source batch directory (for reference)
        dry_run (bool): If True, don't actually modify files
        
    Returns:
        bool: True if successful, False otherwise
    """
    config_file = retry_path / "config" / "config.js"
    
    if not config_file.exists():
        print(f"Warning: config.js not found: {config_file}", file=sys.stderr)
        return False
    
    try:
        # Get absolute paths
        retry_path_abs = retry_path.resolve()
        batch_path_abs = batch_path.resolve()
        
        # Read the config file
        with open(config_file, 'r') as f:
            config_data = json.load(f)
        
        original_config = json.dumps(config_data, indent=4)
        paths_updated = 0
        
        # Function to recursively update paths in the config
        def update_paths(obj, path_key=""):
            nonlocal paths_updated
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path_key}.{key}" if path_key else key
                    if isinstance(value, str) and value:
                        # Check if this is a path that needs updating
                        # Look for paths containing /batch_XX/ or /batch-XX/
                        # Pattern: .../batch_XX/... or .../batch-XX/...
                        match = re.search(r'(/batch[_-]\d+)(/.*)?$', value)
                        if match:
                            batch_part = match.group(1)  # /batch_18 or /batch-18
                            relative_part = match.group(2) if match.group(2) else ""  # /input/... or ""
                            
                            # Replace everything up to and including batch_XX with retry_path
                            # Then append the relative part (which includes the leading / if present)
                            new_path = str(retry_path_abs) + relative_part
                            if value != new_path:
                                obj[key] = new_path
                                paths_updated += 1
                        # Also handle /tmp/batch_XX pattern (standalone, not a directory path)
                        elif value.startswith('/tmp/batch_') or value.startswith('/tmp/batch-'):
                            batch_num_match = re.search(r'batch[_-](\d+)', value)
                            if batch_num_match:
                                batch_num = batch_num_match.group(1)
                                new_path = f"/tmp/batch_{batch_num}_retry"
                                if value != new_path:
                                    obj[key] = new_path
                                    paths_updated += 1
                    elif isinstance(value, (dict, list)):
                        update_paths(value, current_path)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    update_paths(item, f"{path_key}[{i}]")
        
        # Update all paths in the config
        update_paths(config_data)
        
        if paths_updated == 0:
            print(f"Warning: No paths updated in config.js", file=sys.stderr)
            return True
        
        if dry_run:
            print(f"[DRY RUN] Would update config.js:")
            print(f"  - Would update {paths_updated} path(s) to point to retry directory")
            return True
        
        # Write the updated config
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=4)
        
        print(f"✓ Updated config.js:")
        print(f"  - Updated {paths_updated} path(s) to point to retry directory")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"Error parsing config.js as JSON: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error updating config.js: {e}", file=sys.stderr)
        return False


def print_summary(batch_path, stats, retry_path, dry_run=False):
    """
    Print a summary of the operation.
    
    Args:
        batch_path (Path): Path to the source batch
        stats (dict): Statistics dictionary
        retry_path (Path): Path to the retry batch
        dry_run (bool): Whether this was a dry run
    """
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Source batch: {batch_path}")
    print(f"Retry batch: {retry_path}")
    print()
    print(f"Total cells in batch: {stats['total_cells']}")
    print(f"Originally masked cells (status=0): {stats['masked_cells']}")
    print(f"Successful cells (status=100): {stats['successful_cells']}")
    print(f"Failed cells (total): {stats['failed_cells']}")
    if stats['failed_cells'] != stats['failed_cells_to_retry']:
        print(f"Failed cells that were supposed to run: {stats['failed_cells_to_retry']}")
    print()
    
    if stats['failed_cells'] > 0:
        print("Failed cell status breakdown:")
        for code, count in sorted(stats['failed_status_codes'].items(), key=lambda x: (isinstance(x[0], str), x[0])):
            if code == 'NaN':
                print(f"  Status NaN (not computed): {count} cells")
            else:
                status_name = {
                    -100: "fail",
                    -5: "timeout",
                    -9999: "_FillValue"
                }.get(code, "unknown")
                print(f"  Status {code} ({status_name}): {count} cells")
        print()
    
    if stats['failed_cells_to_retry'] == 0:
        print("✓ No failed cells to retry - batch is already complete!")
        if not dry_run:
            print("⚠ Note: Retry batch was still created but all cells are disabled.")
    elif dry_run:
        print(f"[DRY RUN] Would create retry batch with {stats['failed_cells_to_retry']} cells enabled")
    else:
        print(f"✓ Retry batch created with {stats['failed_cells_to_retry']} cells enabled")
        print()
        print("To submit the retry batch:")
        print(f"  cd {retry_path}")
        print(f"  sbatch slurm_runner.sh  # or your submission command")
    
    print(f"{'='*80}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Extract failed cells from a batch and create a retry batch',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  %(prog)s ~/test_batches/batch_18
  
  # Dry run to preview changes
  %(prog)s ~/test_batches/batch_18 --dry-run
  
  # Force overwrite existing retry directory
  %(prog)s ~/test_batches/batch_18 --force
  
  # Verbose output with dry run
  %(prog)s ~/test_batches/batch_18 --dry-run --verbose

Description:
  This script identifies cells that failed during a batch run and creates
  a retry batch with only those cells enabled. Failed cells are defined as
  cells with run_status != 100 (success) and != 0 (masked).
  
  The retry batch is created as a subdirectory named 'retry' within the
  source batch directory, with all files copied and run-mask.nc modified
  to only run the failed cells.
        """
    )
    parser.add_argument(
        'batch_path',
        help='Path to the batch directory'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without creating files'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing retry directory if it exists'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed output including failed cell indices'
    )
    
    args = parser.parse_args()
    
    # Convert to Path object
    batch_path = Path(args.batch_path).resolve()
    retry_path = batch_path / "retry"
    
    print(f"{'='*80}")
    print("Extract Failed Cells - Create Retry Batch")
    print(f"{'='*80}")
    
    if args.dry_run:
        print("[DRY RUN MODE - No files will be modified]")
        print()
    
    # Step 1: Validate batch structure
    print("Step 1: Validating batch structure...")
    is_valid, error_msg = validate_batch_structure(batch_path)
    
    if not is_valid:
        print(f"✗ Validation failed: {error_msg}", file=sys.stderr)
        sys.exit(1)
    
    print(f"✓ Batch structure is valid")
    print()
    
    # Step 2: Identify failed cells
    print("Step 2: Identifying failed cells...")
    run_status, run_mask, stats = identify_failed_cells(batch_path)
    
    if stats is None:
        print("✗ Failed to identify cells", file=sys.stderr)
        sys.exit(1)
    
    print(f"✓ Analysis complete:")
    print(f"  Total cells: {stats['total_cells']}")
    print(f"  Masked cells: {stats['masked_cells']}")
    print(f"  Successful cells: {stats['successful_cells']}")
    print(f"  Failed cells: {stats['failed_cells']}")
    
    if stats['failed_cells'] > 0 and args.verbose:
        print(f"\n  Failed cell indices (Y, X):")
        for idx in stats['failed_indices'][:10]:  # Show first 10
            print(f"    {tuple(idx)}")
        if len(stats['failed_indices']) > 10:
            print(f"    ... and {len(stats['failed_indices']) - 10} more")
    
    print()
    
    # Step 3: Create retry batch
    print("Step 3: Creating retry batch directory...")
    success = create_retry_batch(batch_path, retry_path, args.force, args.dry_run)
    
    if not success:
        sys.exit(1)
    
    print()
    
    # Step 4: Update run-mask
    print("Step 4: Updating run-mask.nc...")
    success, cells_disabled, cells_enabled = update_retry_run_mask(
        retry_path, run_status, run_mask, args.dry_run
    )
    
    if not success:
        sys.exit(1)
    
    print()
    
    # Step 5: Update slurm_runner.sh
    print("Step 5: Updating slurm_runner.sh...")
    success = update_retry_slurm_runner(retry_path, batch_path, args.dry_run)
    
    if not success:
        print("Warning: Failed to update slurm_runner.sh, but continuing...", file=sys.stderr)
    
    print()
    
    # Step 6: Update config.js
    print("Step 6: Updating config.js...")
    success = update_retry_config(retry_path, batch_path, args.dry_run)
    
    if not success:
        print("Warning: Failed to update config.js, but continuing...", file=sys.stderr)
    
    print()
    
    # Step 7: Print summary
    print_summary(batch_path, stats, retry_path, args.dry_run)
    
    if args.dry_run:
        print("\nThis was a dry run. Use without --dry-run to create the retry batch.")


if __name__ == "__main__":
    main()
