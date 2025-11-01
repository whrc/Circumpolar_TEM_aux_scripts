#!/usr/bin/env python3
"""
Sync a specific tile_id and scenario to Google Cloud Storage bucket.

This script deletes the remote folder and then uploads the local data.
Usage: python sync_tile_to_bucket.py <tile_id> <scenario> <local_base_dir>
Example: python sync_tile_to_bucket.py H18_V4 ssp1_2_6_mri_esm2_0 /path/to/data
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a shell command and handle errors."""
    print(f"\n{description}")
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        return False


def check_local_path(local_path):
    """Verify the local path exists."""
    if not os.path.exists(local_path):
        print(f"Error: Local path does not exist: {local_path}")
        return False
    if not os.path.isdir(local_path):
        print(f"Error: Path is not a directory: {local_path}")
        return False
    return True


def run_cmd(command, auto_yes=False):
    """Run a shell command with optional auto-yes."""
    print(f"[RUN] {command}")
    try:
        if auto_yes:
            subprocess.run(command, shell=True, check=True, input=b"y\n")
        else:
            subprocess.run(command, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed: {command}")
        print(f"[CODE] Exit status: {e.returncode}")
        return False


def trim_sc_files(sc_path):
    """
    Trim files in the split scenario directory.
    
    Args:
        sc_path: Path to the scenario_split directory
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"\n{'=' * 80}")
        print(f"TRIMMING files in: {sc_path}")
        print(f"{'=' * 80}")
        
        if not os.path.exists(sc_path):
            print(f"Error: Path does not exist: {sc_path}")
            return False
        
        trim_script = os.path.expanduser("~/Circumpolar_TEM_aux_scripts/trim_batch.py")
        if not os.path.exists(trim_script):
            print(f"Error: Trim script not found: {trim_script}")
            return False
        
        success = run_cmd(f"python {trim_script} {sc_path}")
        if success:
            print("✓ Trimming completed successfully.")
        else:
            print("✗ Trimming failed.")
        return success
    except Exception as e:
        print(f"An unexpected error occurred while trimming.")
        print(f"Details: {e}")
        return False


def merge_and_plot(split_path, force=False):
    """
    Merge batch outputs and generate plots.
    
    Args:
        split_path: Path to the scenario_split directory
        force: If True, merge even if summary_plots.pdf exists
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"\n{'=' * 80}")
        print(f"MERGING and PLOTTING in: {split_path}")
        if force:
            print("FORCE MODE: Will merge even if summary_plots.pdf exists")
        print(f"{'=' * 80}")
        
        if not os.path.exists(split_path):
            print(f"Error: Path does not exist: {split_path}")
            return False
        
        plot_file = Path(f"{split_path}/all_merged/summary_plots.pdf")
        if plot_file.exists() and not force:
            print(f"Skipping the merge. {plot_file} exists.")
            print("Use --force-merge to merge anyway.")
            return True
        
        # Run merge command with auto-yes
        merge_success = run_cmd(f"bp batch merge -b {split_path}", auto_yes=True)
        if not merge_success:
            print("✗ Merge failed.")
            return False
        
        # Run plotting script
        plot_script = os.path.expanduser("~/Circumpolar_TEM_aux_scripts/plot_nc_all_files.py")
        if not os.path.exists(plot_script):
            print(f"Error: Plot script not found: {plot_script}")
            return False
        
        plot_success = run_cmd(f"python {plot_script} {split_path}/all_merged/")
        if plot_success:
            print("✓ Merge and plot completed successfully.")
        else:
            print("✗ Plotting failed.")
        return plot_success
    except Exception as e:
        print(f"An unexpected error occurred while merging/plotting.")
        print(f"Details: {e}")
        return False


def sync_tile_scenario(tile_id, scenario, local_base_dir, bucket_name="circumpolar_model_output", merged_only=False, do_trim=False, do_merge=False, force_merge=False):
    """
    Sync a tile_id and scenario to the GCS bucket.
    
    Args:
        tile_id: Tile identifier (e.g., H18_V4)
        scenario: Scenario name (e.g., ssp1_2_6_mri_esm2_0)
        local_base_dir: Base directory containing scenario folders
        bucket_name: GCS bucket name (default: circumpolar_model_output)
        merged_only: If True, sync only the all_merged subdirectory
        do_trim: If True, trim files before syncing
        do_merge: If True, merge and plot before syncing
        force_merge: If True, force merge even if summary_plots.pdf exists
    """
    # Construct paths - split_path is used for trim/merge operations
    split_path = os.path.join(local_base_dir, tile_id+'_sc', scenario+'_split')
    
    # Perform trim and merge operations before syncing
    if do_trim:
        if not trim_sc_files(split_path):
            print("Warning: Trimming failed, but continuing with sync...")
    
    if do_merge:
        if not merge_and_plot(split_path,  force=force_merge):
            print("Warning: Merge/plot failed, but continuing with sync...")
    
    # Construct paths for syncing
    if merged_only:
        local_path = os.path.join(local_base_dir, tile_id+'_sc', scenario+'_split', 'all_merged')
        remote_base = f"gs://{bucket_name}/recent2/{tile_id}/{scenario}_split/all_merged"
    else:
        local_path = os.path.join(local_base_dir, tile_id+'_sc', scenario+'_split')
        remote_base = f"gs://{bucket_name}/recent2/{tile_id}/{scenario}_split"
    
    print(f"=" * 80)
    sync_type = "all_merged subdirectory" if merged_only else "full tile/scenario"
    print(f"Syncing tile {tile_id} with scenario {scenario} ({sync_type})")
    print(f"=" * 80)
    print(f"Local path: {local_path}")
    print(f"Remote path: {remote_base}")
    
    # Check if local path exists
    if not check_local_path(local_path):
        return False
    
    # Count files in local directory
    file_count = sum(len(files) for _, _, files in os.walk(local_path))
    print(f"Local directory contains {file_count} files")
    
    # Step 1: Delete the remote folder
    print(f"\n{'=' * 80}")
    print("STEP 1: Deleting remote folder")
    print(f"{'=' * 80}")
    
    # Check if remote folder exists first
    check_cmd = ["gsutil", "ls", remote_base]
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"Remote folder exists. Deleting: {remote_base}")
        delete_cmd = ["gsutil", "-m", "rm", "-r", remote_base]
        if not run_command(delete_cmd, "Deleting remote folder..."):
            print("Warning: Failed to delete remote folder. It may not exist.")
    else:
        print(f"Remote folder does not exist yet: {remote_base}")
    
    # Step 2: Upload the local folder
    print(f"\n{'=' * 80}")
    print("STEP 2: Uploading local folder to bucket")
    print(f"{'=' * 80}")
    
    if merged_only:
        # For merged_only, upload all_merged directory to create the remote all_merged folder
        # Upload to parent directory so gsutil creates the all_merged folder structure
        upload_cmd = [
            "gsutil",
            "-m",  # multithreaded for faster upload
            "cp",
            "-r",  # recursive
            local_path,  # This is already the all_merged directory
            f"gs://{bucket_name}/recent2/{tile_id}/{scenario}_split/"
        ]
    else:
        upload_cmd = [
            "gsutil",
            "-m",  # multithreaded for faster upload
            "cp",
            "-r",  # recursive
            local_path,
            f"gs://{bucket_name}/recent2/{tile_id}/{scenario}_split"
        ]
    
    if not run_command(upload_cmd, "Uploading files..."):
        print("Error: Failed to upload files")
        return False
    
    # Step 3: Verify upload
    print(f"\n{'=' * 80}")
    print("STEP 3: Verifying upload")
    print(f"{'=' * 80}")
    
    verify_cmd = ["gsutil", "du", "-s", remote_base]
    if run_command(verify_cmd, "Checking remote folder size..."):
        print("\n✓ Upload completed successfully!")
        return True
    else:
        print("\n⚠ Upload completed but verification failed")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Sync tile_id and scenario data to GCS bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync H18_V4 with ssp1_2_6_mri_esm2_0 scenario
  %(prog)s H18_V4 ssp1_2_6_mri_esm2_0 /path/to/data --sync
  
  # Sync from merge/Alaska directory
  %(prog)s H18_V4 ssp5_8_5_mri_esm2_0 merge/Alaska --sync
  
  # Specify custom bucket
  %(prog)s H18_V4 ssp1_2_6_mri_esm2_0 /path/to/data --bucket my-bucket --sync
  
  # Sync only all_merged subdirectory
  %(prog)s H15_V4 ssp1_2_6_mri_esm2_0 /path/to/data --all_merged --sync
  
  # Trim files before syncing
  %(prog)s H15_V4 ssp1_2_6_mri_esm2_0 /path/to/data --trim --sync
  
  # Merge and plot before syncing
  %(prog)s H15_V4 ssp1_2_6_mri_esm2_0 /path/to/data --merge --sync
  
  # Force merge even if summary_plots.pdf exists
  %(prog)s H15_V4 ssp1_2_6_mri_esm2_0 /path/to/data --merge --force-merge --sync

  # Trim, merge, and sync together
  %(prog)s H15_V4 ssp1_2_6_mri_esm2_0 /path/to/data --trim --merge --sync
  
  # Trim and merge only (no sync)
  %(prog)s H15_V4 ssp1_2_6_mri_esm2_0 /path/to/data --trim --merge
  
  # Sync only (no trim/merge)
  %(prog)s H15_V4 ssp1_2_6_mri_esm2_0 /path/to/data --sync
        """
    )
    
    parser.add_argument(
        "tile_id",
        help="Tile identifier (e.g., H18_V4)"
    )
    
    parser.add_argument(
        "scenario",
        help="Scenario name (e.g., ssp1_2_6_mri_esm2_0)"
    )
    
    parser.add_argument(
        "local_base_dir",
        help="Base directory containing scenario folders"
    )
    
    parser.add_argument(
        "--bucket",
        default="circumpolar_model_output",
        help="GCS bucket name (default: circumpolar_model_output)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without executing"
    )
    
    parser.add_argument(
        "--all_merged",
        action="store_true",
        help="Sync only the all_merged subdirectory"
    )
    
    parser.add_argument(
        "--trim",
        action="store_true",
        help="Trim batch files before syncing (can be used independently)"
    )
    
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge batch outputs and generate plots before syncing (can be used independently)"
    )
  
    parser.add_argument(
        "--force-merge",
        action="store_true",
        help="Force merge even when summary_plots.pdf exists (use with --merge)"
    )

    parser.add_argument(
        "--sync",
        action="store_true",
        help="Enable file syncing to GCS bucket (required for sync operations)"
    )
    
    args = parser.parse_args()
    
    # Expand path if relative
    local_base_dir = os.path.expanduser(args.local_base_dir)
    if not os.path.isabs(local_base_dir):
        local_base_dir = os.path.abspath(local_base_dir)
    
    # Get split path for trim/merge operations
    split_path = os.path.join(local_base_dir, args.tile_id+'_sc', args.scenario+'_split')
    
    if args.dry_run:
        print("DRY RUN MODE - No actual changes will be made")
        
        operations = []
        if args.trim:
            operations.append(f"1. Trim files in: {split_path}")
        if args.merge:
            operations.append(f"{len(operations)+1}. Merge and plot in: {split_path}")
        
        if args.sync:
            if args.all_merged:
                local_path = os.path.join(local_base_dir, args.tile_id+'_sc', args.scenario+'_split', 'all_merged')
                remote_path = f"gs://{args.bucket}/recent2/{args.tile_id}/{args.scenario}_split/all_merged"
                sync_type = "all_merged subdirectory only"
            else:
                local_path = os.path.join(local_base_dir, args.tile_id+'_sc', args.scenario+'_split')
                remote_path = f"gs://{args.bucket}/recent2/{args.tile_id}/{args.scenario}_split"
                sync_type = "full tile/scenario"
            
            if operations:
                print(f"\nPre-sync operations:")
                for op in operations:
                    print(f"  {op}")
            
            print(f"\nSync type: {sync_type}")
            print(f"\nWould sync:")
            print(f"  From: {local_path}")
            print(f"  To:   {remote_path}")
            print(f"\nSync operations:")
            print(f"  {len(operations)+1}. Delete remote: {remote_path}")
            print(f"  {len(operations)+2}. Upload local:  {local_path}")
        else:
            if operations:
                print(f"\nOperations (no sync):")
                for op in operations:
                    print(f"  {op}")
            else:
                print("\nNo operations specified. Use --trim, --merge, or --sync")
        return
    
    # Run trim/merge operations if requested (before sync)
    if args.trim:
        print(f"{'=' * 80}")
        print("Running trim operation")
        print(f"{'=' * 80}")
        trim_success = trim_sc_files(split_path)
        if not trim_success:
            print("✗ Trimming failed")
            sys.exit(1)
    
    if args.merge:
        print(f"{'=' * 80}")
        print("Running merge and plot operation")
        print(f"{'=' * 80}")
        merge_success = merge_and_plot(split_path,  force=args.force_merge)
        if not merge_success:
            print("✗ Merge/plot failed")
            sys.exit(1)
    
    # If sync is not enabled, exit after trim/merge operations
    if not args.sync:
        if args.trim or args.merge:
            print(f"\n{'=' * 80}")
            print("OPERATIONS COMPLETED SUCCESSFULLY (no sync)")
            print(f"{'=' * 80}")
        else:
            print("\nNo operations specified. Use --trim, --merge, or --sync")
            sys.exit(1)
        sys.exit(0)
    
    # Check if gsutil is available (only needed if syncing)
    try:
        subprocess.run(
            ["gsutil", "version"],
            check=True,
            capture_output=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: gsutil is not installed or not in PATH")
        print("Please install Google Cloud SDK: https://cloud.google.com/sdk/install")
        sys.exit(1)
    
    # Perform the sync (trim/merge already done if requested)
    success = sync_tile_scenario(
        args.tile_id,
        args.scenario,
        local_base_dir,
        args.bucket,
        args.all_merged,
        False,  # trim already done if requested
        False,  # merge already done if requested
        args.force_merge  # force_merge flag
    )
    
    if success:
        print(f"\n{'=' * 80}")
        print("SYNC COMPLETED SUCCESSFULLY")
        print(f"{'=' * 80}")
        sys.exit(0)
    else:
        print(f"\n{'=' * 80}")
        print("SYNC FAILED")
        print(f"{'=' * 80}")
        sys.exit(1)


if __name__ == "__main__":
    main()



