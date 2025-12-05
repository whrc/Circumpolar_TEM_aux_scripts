#!/usr/bin/env python3
"""
Script to extract failed cells from a batch and create a retry batch.

This script reads run_status.nc to identify cells that failed (status != 100 and != 0),
then creates a retry batch with only those cells enabled in the run-mask.nc.

After the retry batch completes, use --merge to merge successful results back into
the original batch.

Usage:
    python extract_failed_cells.py <batch_path> [options]
    
Examples:
    # Basic usage
    python extract_failed_cells.py ~/test_batches/batch_18
    
    # Dry run to preview changes
    python extract_failed_cells.py ~/test_batches/batch_18 --dry-run
    
    # Force overwrite existing retry directory
    python extract_failed_cells.py ~/test_batches/batch_18 --force
    
    # Auto-submit slurm job after creating retry batch
    python extract_failed_cells.py ~/test_batches/batch_18 --submit
    
    # Merge retry results back into original batch
    python extract_failed_cells.py ~/test_batches/batch_18 --merge
"""

import os
import sys
import argparse
import shutil
import re
import json
import subprocess
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


def update_retry_slurm_runner(retry_path, batch_path, dry_run=False, partition="dask"):
    """
    Update the slurm_runner.sh file in the retry batch to use retry paths.
    
    Updates:
    - The log file path (-o) to use retry_path's logs directory
    - The config file path (-f) to use retry_path's config directory
    - The partition from "spot" to the specified partition (default: "dask") if present
    - The job name to append "-retry"
    
    Args:
        retry_path (Path): Path to the retry batch directory
        batch_path (Path): Path to the source batch directory (for reference)
        dry_run (bool): If True, don't actually modify files
        partition (str): Partition name to use (default: "dask")
        
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
        
        # Update partition from "spot" to specified partition if present
        # Match -p spot, --partition=spot, and --partition spot formats
        # Pattern matches: #SBATCH -p spot, #SBATCH --partition=spot, #SBATCH --partition spot
        partition_pattern = r'(#SBATCH\s+(?:-p\s+|--partition=))spot(\s|$)'
        
        def replace_partition(match):
            prefix = match.group(1)  # #SBATCH -p or #SBATCH --partition=
            suffix = match.group(2)  # whitespace or end of line
            return f"{prefix}{partition}{suffix}"
        
        content = re.sub(partition_pattern, replace_partition, content)
        
        # Also handle --partition spot format (with space instead of =)
        partition_space_pattern = r'(#SBATCH\s+--partition\s+)spot(\s|$)'
        
        def replace_partition_space(match):
            prefix = match.group(1)  # #SBATCH --partition 
            suffix = match.group(2)  # whitespace or end of line
            return f"{prefix}{partition}{suffix}"
        
        content = re.sub(partition_space_pattern, replace_partition_space, content)
        
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
            if 'spot' in original_content:
                print(f"  - Would change partition from 'spot' to '{partition}'")
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
        if 'spot' in original_content and partition in content:
            print(f"  - Changed partition from 'spot' to '{partition}'")
        
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


def merge_retry_results(batch_path, retry_path, dry_run=False):
    """
    Merge results from retry batch into original batch or merged directory.
    
    This function merges successful results from the retry batch. If all retried
    cells succeeded, it merges directly into the original batch. Otherwise, it
    creates a 'merged' directory in the batch_path and merges results there.
    
    This function:
    1. Determines if merged directory is needed (only if some cells failed)
    2. Creates a 'merged' directory structure (merged/output and merged/input) if needed
    3. Copies original files to target directory (merged or original)
    4. Merges run_status.nc: updates target batch with successful cells from retry
    5. Merges output files: copies/merges output NetCDF files from retry to target
    6. Updates run-mask.nc in target batch to reflect newly successful cells
    
    Args:
        batch_path (Path): Path to the original batch directory
        retry_path (Path): Path to the retry batch directory
        dry_run (bool): If True, don't actually modify files
        
    Returns:
        tuple: (success, stats_dict) where stats_dict contains merge statistics
               including 'merged_path' (None if merged directly to original) and
               'use_merged_dir' (bool indicating if merged directory was used)
    """
    try:
        # Create merged directory structure
        merged_path = batch_path / "merged"
        merged_output_dir = merged_path / "output"
        merged_input_dir = merged_path / "input"
        
        # Paths to files
        original_status_file = batch_path / "output" / "run_status.nc"
        retry_status_file = retry_path / "output" / "run_status.nc"
        original_mask_file = batch_path / "input" / "run-mask.nc"
        original_output_dir = batch_path / "output"
        retry_output_dir = retry_path / "output"
        
        # Merged file paths
        merged_status_file = merged_output_dir / "run_status.nc"
        merged_mask_file = merged_input_dir / "run-mask.nc"
        
        # Validate files exist
        if not retry_status_file.exists():
            print(f"Error: Retry run_status.nc not found: {retry_status_file}", file=sys.stderr)
            return False, {}
        
        if not original_status_file.exists():
            print(f"Error: Original run_status.nc not found: {original_status_file}", file=sys.stderr)
            return False, {}
        
        # Read original run_status
        ds_original = xr.open_dataset(original_status_file, decode_times=False)
        original_status = ds_original['run_status'].values.copy()
        
        # Read retry run_status
        ds_retry = xr.open_dataset(retry_status_file, decode_times=False)
        retry_status = ds_retry['run_status'].values
        
        # Validate shapes match
        if original_status.shape != retry_status.shape:
            print(f"Error: Shape mismatch between original and retry run_status!", file=sys.stderr)
            print(f"  Original shape: {original_status.shape}", file=sys.stderr)
            print(f"  Retry shape: {retry_status.shape}", file=sys.stderr)
            ds_original.close()
            ds_retry.close()
            return False, {}
        
        # Identify cells that became successful in retry
        # These are cells where retry_status == 100 and original_status != 100
        newly_successful_mask = (retry_status == 100) & (original_status != 100)
        newly_successful_count = int(np.sum(newly_successful_mask))
        
        # Count cells that were already successful
        already_successful = int(np.sum((original_status == 100) & (retry_status == 100)))
        
        # Count cells that failed in retry
        retry_failed = int(np.sum((retry_status != 100) & (retry_status != 0) & ~np.isnan(retry_status)))
        
        # Determine if we should use merged directory or merge directly to original
        # If all retried cells succeeded, merge directly to original batch
        # Otherwise, create merged directory
        use_merged_dir = retry_failed > 0
        
        # Set target paths based on whether we're using merged directory
        if use_merged_dir:
            target_output_dir = merged_output_dir
            target_input_dir = merged_input_dir
            target_status_file = merged_status_file
            target_mask_file = merged_mask_file
        else:
            # Merge directly to original batch
            target_output_dir = original_output_dir
            target_input_dir = batch_path / "input"
            target_status_file = original_status_file
            target_mask_file = original_mask_file
        
        # Create merged directory structure only if needed and not in dry run
        if use_merged_dir and not dry_run:
            merged_output_dir.mkdir(parents=True, exist_ok=True)
            merged_input_dir.mkdir(parents=True, exist_ok=True)
            print(f"Created merged directory: {merged_path}")
        elif not use_merged_dir and not dry_run:
            print(f"All retried cells succeeded - merging directly into original batch")
        
        if dry_run:
            if use_merged_dir:
                print(f"[DRY RUN] Would merge retry results into: {merged_path}")
            else:
                print(f"[DRY RUN] Would merge retry results directly into original batch: {batch_path}")
            print(f"  - Would update {newly_successful_count} newly successful cells")
            print(f"  - {already_successful} cells were already successful")
            print(f"  - {retry_failed} cells still failed in retry")
            
            # Count output files that would be merged
            if retry_output_dir.exists():
                output_files = list(retry_output_dir.glob("*.nc"))
                print(f"  - Would merge {len(output_files)} output NetCDF file(s)")
            
            ds_original.close()
            ds_retry.close()
            return True, {
                'newly_successful': newly_successful_count,
                'already_successful': already_successful,
                'retry_failed': retry_failed,
                'use_merged_dir': use_merged_dir,
                'merged_path': str(merged_path) if use_merged_dir else None
            }
        
        # Copy original run_status.nc to target directory first (if using merged dir)
        if use_merged_dir:
            shutil.copy2(original_status_file, target_status_file)
        
        # Update merged run_status with successful cells from retry
        merged_status = original_status.copy()
        merged_status[newly_successful_mask] = 100
        
        # Create updated dataset from original
        ds_updated = ds_original.copy()
        ds_updated['run_status'].values[:] = merged_status
        
        # Also merge other variables from retry (like total_runtime)
        # Update values where retry has valid data (not masked, not fill value)
        for var_name in ds_retry.data_vars:
            if var_name == 'run_status':
                continue  # Already handled above
            
            if var_name in ds_updated.data_vars:
                retry_var_data = ds_retry[var_name].values
                merged_var_data = ds_updated[var_name].values
                
                # Create mask for valid retry data (not masked=0, not fill value)
                if hasattr(ds_retry[var_name], '_FillValue'):
                    fill_value = ds_retry[var_name]._FillValue
                    # Valid where retry is not 0 (masked), not fill value, and not NaN (if float)
                    if np.issubdtype(retry_var_data.dtype, np.floating):
                        valid_mask = (retry_var_data != 0) & (retry_var_data != fill_value) & ~np.isnan(retry_var_data)
                    else:
                        valid_mask = (retry_var_data != 0) & (retry_var_data != fill_value)
                else:
                    # No fill value, just check not masked (0)
                    if np.issubdtype(retry_var_data.dtype, np.floating):
                        valid_mask = (retry_var_data != 0) & ~np.isnan(retry_var_data)
                    else:
                        valid_mask = (retry_var_data != 0)
                
                # Update merged data where retry has valid data
                merged_var_data[valid_mask] = retry_var_data[valid_mask]
                ds_updated[var_name].values[:] = merged_var_data
        
        # Close datasets before writing
        ds_original.close()
        ds_retry.close()
        
        # Write updated run_status.nc to target directory
        temp_file = target_status_file.parent / f".{target_status_file.name}.tmp"
        ds_updated.to_netcdf(temp_file)
        ds_updated.close()
        
        # Replace target file
        target_status_file.unlink()
        temp_file.rename(target_status_file)
        
        # Verify we wrote to the correct location
        if not target_status_file.exists():
            print(f"Error: Failed to write merged run_status.nc to {target_status_file}", file=sys.stderr)
            return False, {}
        
        print(f"✓ Merged run_status.nc to {target_status_file}:")
        print(f"  - Updated {newly_successful_count} newly successful cells")
        print(f"  - {already_successful} cells were already successful")
        print(f"  - {retry_failed} cells still failed in retry")
        
        if retry_failed > 0:
            print(f"\n⚠ Note: {retry_failed} cells still failed, but merging all output files regardless of status.")
        
        # Merge output files (merge all files regardless of success status)
        output_files_merged = 0
        if retry_output_dir.exists() and original_output_dir.exists():
            # Get list of NetCDF files in retry output
            retry_output_files = list(retry_output_dir.glob("*.nc"))
            
            for retry_file in retry_output_files:
                # Skip run_status.nc as we already handled it
                if retry_file.name == "run_status.nc":
                    continue
                
                original_file = original_output_dir / retry_file.name
                target_file = target_output_dir / retry_file.name
                
                # If file exists in original, we need to merge the data
                # Copy original to target first (only if using merged directory), then merge retry data
                if original_file.exists():
                    # Only copy if we're using a merged directory (target != original)
                    if use_merged_dir:
                        # Copy original file to target directory first
                        shutil.copy2(original_file, target_file)
                    
                    try:
                        # Merge NetCDF files: update values where retry has data
                        ds_merged = xr.open_dataset(target_file, decode_times=False)
                        ds_ret = xr.open_dataset(retry_file, decode_times=False)
                        
                        # For each data variable, update values where retry has valid data
                        # This is a simple merge: retry values overwrite merged where retry has data
                        for var_name in ds_ret.data_vars:
                            if var_name in ds_merged.data_vars:
                                # Update values where retry has non-missing data
                                retry_data = ds_ret[var_name].values
                                merged_data = ds_merged[var_name].values
                                
                                # Check if data is scalar (0-dimensional)
                                is_scalar = retry_data.ndim == 0
                                
                                if is_scalar:
                                    # For scalar values, check if retry value is valid and replace
                                    retry_dtype = retry_data.dtype
                                    is_valid = True
                                    
                                    # Check validity based on dtype
                                    if np.issubdtype(retry_dtype, np.floating):
                                        # For floating point, check for NaN
                                        if hasattr(ds_ret[var_name], '_FillValue'):
                                            fill_value = ds_ret[var_name]._FillValue
                                            is_valid = not np.isnan(retry_data) and (retry_data != fill_value)
                                        else:
                                            is_valid = not np.isnan(retry_data)
                                    else:
                                        # For integer types, check fill value
                                        if hasattr(ds_ret[var_name], '_FillValue'):
                                            fill_value = ds_ret[var_name]._FillValue
                                            is_valid = (retry_data != fill_value)
                                        # else: is_valid already True
                                    
                                    # Replace scalar value if valid
                                    if is_valid:
                                        ds_merged[var_name].values = retry_data
                                else:
                                    # For array data, use masking approach
                                    # Create mask for valid retry data (not NaN, not fill value)
                                    # Handle different data types appropriately
                                    retry_dtype = retry_data.dtype
                                    
                                    # Check if dtype supports NaN (floating point types)
                                    if np.issubdtype(retry_dtype, np.floating):
                                        # For floating point, check for NaN
                                        if hasattr(ds_ret[var_name], '_FillValue'):
                                            fill_value = ds_ret[var_name]._FillValue
                                            valid_mask = ~np.isnan(retry_data) & (retry_data != fill_value)
                                        else:
                                            valid_mask = ~np.isnan(retry_data)
                                    else:
                                        # For integer types, only check fill value
                                        if hasattr(ds_ret[var_name], '_FillValue'):
                                            fill_value = ds_ret[var_name]._FillValue
                                            valid_mask = (retry_data != fill_value)
                                        else:
                                            # No fill value specified, assume all values are valid
                                            valid_mask = np.ones_like(retry_data, dtype=bool)
                                    
                                    # Update merged data where retry has valid data
                                    merged_data[valid_mask] = retry_data[valid_mask]
                                    ds_merged[var_name].values[:] = merged_data
                        
                        # Write merged dataset
                        temp_merge_file = target_file.parent / f".{target_file.name}.tmp"
                        ds_merged.to_netcdf(temp_merge_file)
                        ds_merged.close()
                        ds_ret.close()
                        
                        # Replace target file
                        target_file.unlink()
                        temp_merge_file.rename(target_file)
                        
                        output_files_merged += 1
                    except Exception as e:
                        print(f"Warning: Could not merge {retry_file.name}: {e}", file=sys.stderr)
                        # Fallback: just copy the retry file
                        shutil.copy2(retry_file, target_file)
                        output_files_merged += 1
                else:
                    # File doesn't exist in original, just copy retry file to target
                    shutil.copy2(retry_file, target_file)
                    output_files_merged += 1
            
            if output_files_merged > 0:
                print(f"✓ Merged {output_files_merged} output file(s) to {target_output_dir}")
        
        # Update run-mask.nc in target batch to reflect newly successful cells
        # Set run=0 for cells that are now successful
        if original_mask_file.exists():
            # Copy original mask to target directory first (if using merged dir)
            if use_merged_dir:
                shutil.copy2(original_mask_file, target_mask_file)
            
            ds_mask = xr.open_dataset(target_mask_file, decode_times=False)
            run_mask = ds_mask['run'].values.copy()
            
            # Set run=0 for newly successful cells
            run_mask[newly_successful_mask] = 0
            
            ds_mask_updated = ds_mask.copy()
            ds_mask_updated['run'].values[:] = run_mask
            
            temp_mask_file = target_mask_file.parent / f".{target_mask_file.name}.tmp"
            ds_mask.close()
            ds_mask_updated.to_netcdf(temp_mask_file)
            ds_mask_updated.close()
            
            target_mask_file.unlink()
            temp_mask_file.rename(target_mask_file)
            
            print(f"✓ Updated run-mask.nc in {target_mask_file}:")
            print(f"  - Disabled {newly_successful_count} newly successful cells")
        
        stats = {
            'newly_successful': newly_successful_count,
            'already_successful': already_successful,
            'retry_failed': retry_failed,
            'output_files_merged': output_files_merged,
            'use_merged_dir': use_merged_dir,
            'merged_path': str(merged_path) if use_merged_dir else None
        }
        
        return True, stats
        
    except Exception as e:
        print(f"Error merging retry results: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False, {}


def submit_slurm_job(retry_path, dry_run=False):
    """
    Submit the slurm job for the retry batch.
    
    Args:
        retry_path (Path): Path to the retry batch directory
        dry_run (bool): If True, don't actually submit the job
        
    Returns:
        tuple: (success, job_id) where success is bool and job_id is the job ID string or None
    """
    slurm_runner_file = retry_path / "slurm_runner.sh"
    
    if not slurm_runner_file.exists():
        print(f"Error: slurm_runner.sh not found: {slurm_runner_file}", file=sys.stderr)
        return False, None
    
    if dry_run:
        print(f"[DRY RUN] Would submit slurm job:")
        print(f"  cd {retry_path}")
        print(f"  sbatch slurm_runner.sh")
        return True, None
    
    try:
        # Change to retry directory and submit the job
        result = subprocess.run(
            ['sbatch', 'slurm_runner.sh'],
            cwd=retry_path,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Extract job ID from sbatch output (format: "Submitted batch job 12345")
        job_id = None
        if result.stdout:
            match = re.search(r'(\d+)', result.stdout)
            if match:
                job_id = match.group(1)
        
        print(f"✓ Submitted slurm job")
        if job_id:
            print(f"  Job ID: {job_id}")
        print(f"  Output: {result.stdout.strip()}")
        
        return True, job_id
        
    except subprocess.CalledProcessError as e:
        print(f"Error submitting slurm job: {e}", file=sys.stderr)
        if e.stderr:
            print(f"  Error output: {e.stderr}", file=sys.stderr)
        return False, None
    except FileNotFoundError:
        print(f"Error: 'sbatch' command not found. Is SLURM installed?", file=sys.stderr)
        return False, None
    except Exception as e:
        print(f"Error submitting slurm job: {e}", file=sys.stderr)
        return False, None


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
  
  # Auto-submit slurm job after creating retry batch
  %(prog)s ~/test_batches/batch_18 --submit
  
  # Use a specific partition (default is 'dask')
  %(prog)s ~/test_batches/batch_18 -p gpu
  
  # Merge retry results back into original batch
  %(prog)s ~/test_batches/batch_18 --merge

Description:
  This script identifies cells that failed during a batch run and creates
  a retry batch with only those cells enabled. Failed cells are defined as
  cells with run_status != 100 (success) and != 0 (masked).
  
  The retry batch is created as a subdirectory named 'retry' within the
  source batch directory, with all files copied and run-mask.nc modified
  to only run the failed cells.
  
  Use --merge to merge successful results from the retry batch back into
  the original batch, updating run_status.nc and output files.
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
    parser.add_argument(
        '--merge',
        action='store_true',
        help='Merge successful results from retry batch back into original batch'
    )
    parser.add_argument(
        '--submit',
        action='store_true',
        help='Automatically submit the slurm job after creating the retry batch'
    )
    parser.add_argument(
        '-p', '--partition',
        type=str,
        default='dask',
        help='Partition to use when updating slurm_runner.sh (default: dask)'
    )
    
    args = parser.parse_args()
    
    # Convert to Path object
    batch_path = Path(args.batch_path).resolve()
    retry_path = batch_path / "retry"
    
    print(f"{'='*80}")
    if args.merge:
        print("Merge Retry Results - Update Original Batch")
    else:
        print("Extract Failed Cells - Create Retry Batch")
    print(f"{'='*80}")
    
    if args.dry_run:
        print("[DRY RUN MODE - No files will be modified]")
        print()
    
    # If --merge flag is set, merge results and exit
    if args.merge:
        if not retry_path.exists():
            print(f"Error: Retry batch directory does not exist: {retry_path}", file=sys.stderr)
            print("Please create a retry batch first before merging results.", file=sys.stderr)
            sys.exit(1)
        
        print("Merging retry results...")
        success, merge_stats = merge_retry_results(batch_path, retry_path, args.dry_run)
        
        if not success:
            print("✗ Failed to merge retry results", file=sys.stderr)
            sys.exit(1)
        
        print()
        print(f"{'='*80}")
        print("MERGE SUMMARY")
        print(f"{'='*80}")
        print(f"Original batch: {batch_path}")
        print(f"Retry batch: {retry_path}")
        if merge_stats:
            use_merged_dir = merge_stats.get('use_merged_dir', False)
            merged_path = merge_stats.get('merged_path')
            if use_merged_dir and merged_path:
                print(f"Merged results directory: {merged_path}")
            else:
                print(f"Merged results: Directly into original batch (all retried cells succeeded)")
        print()
        if merge_stats:
            print(f"Newly successful cells: {merge_stats.get('newly_successful', 0)}")
            print(f"Already successful cells: {merge_stats.get('already_successful', 0)}")
            retry_failed = merge_stats.get('retry_failed', 0)
            print(f"Still failed cells: {retry_failed}")
            print(f"Output files merged: {merge_stats.get('output_files_merged', 0)}")
        print()
        if merge_stats and retry_failed > 0:
            print(f"⚠ Warning: {retry_failed} cells still failed in the retry batch.")
            print("  Output files have been merged regardless of success status.")
            print("  Review the merged results to verify data quality.")
            print()
        if merge_stats:
            use_merged_dir = merge_stats.get('use_merged_dir', False)
            if use_merged_dir:
                print("⚠ Merged results are in the 'merged' directory.")
                print("  Review the results before copying them to the original batch.")
            else:
                print("✓ All retried cells succeeded - results merged directly into original batch.")
        print(f"{'='*80}")
        
        if args.dry_run:
            print("\nThis was a dry run. Use without --dry-run to merge the results.")
        
        return
    
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
    success = update_retry_slurm_runner(retry_path, batch_path, args.dry_run, args.partition)
    
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
    
    # Step 8: Submit slurm job if requested
    job_id = None
    if args.submit:
        if args.dry_run:
            print("\n[DRY RUN] Would submit slurm job (use --submit without --dry-run to actually submit)")
        elif stats['failed_cells_to_retry'] == 0:
            print("\n⚠ No failed cells to retry - skipping job submission")
        else:
            print()
            print("Step 8: Submitting slurm job...")
            success, job_id = submit_slurm_job(retry_path, args.dry_run)
            if success and job_id:
                print(f"\n✓ Job submitted successfully (Job ID: {job_id})")
            elif success:
                print(f"\n✓ Job submitted successfully")
            else:
                print("\n✗ Failed to submit slurm job", file=sys.stderr)
    
    if args.dry_run:
        print("\nThis was a dry run. Use without --dry-run to create the retry batch.")
    else:
        if args.submit and job_id:
            print("\nMonitor the job status with:")
            print(f"  squeue -j {job_id}")
        elif args.submit:
            print("\nMonitor the job status with:")
            print(f"  squeue -u $USER")
        else:
            print("\nAfter the retry batch completes, use --merge to merge results back:")
            print(f"  {sys.argv[0]} {batch_path} --merge")
            print("\nTo submit the retry batch now:")
            print(f"  cd {retry_path}")
            print(f"  sbatch slurm_runner.sh")
            print("\nOr use --submit flag to auto-submit:")
            print(f"  {sys.argv[0]} {batch_path} --submit")


if __name__ == "__main__":
    main()


# if there's wall time in the job script, remove it
# merge the results regardless but if there's failure, report it with batch number and cell number
