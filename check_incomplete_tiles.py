#!/usr/bin/env python3
"""
Script to check completion status of tiles from a list.

This script reads a list of tiles from a file, downloads each tile
from the Google Cloud Storage bucket, checks completion status for all *_split
folders, and reports any incomplete runs (< 100% completion).

Usage:
    python check_incomplete_tiles.py <tile_file>
    
    Example:
        python check_incomplete_tiles.py tiles/test_tile.txt
    
Requirements:
    - gsutil must be installed and configured
    - Tile file must exist with one tile per line
"""

import os
import sys
import subprocess
import re
from pathlib import Path
import tempfile
import shutil
import argparse


def read_tile_list(tile_file):
    """
    Read tile names from a text file.
    
    Args:
        tile_file (str): Path to the file containing tile names (one per line)
        
    Returns:
        list: List of tile names
    """
    try:
        with open(tile_file, 'r') as f:
            # Skip empty lines and lines starting with #
            tiles = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return tiles
    except FileNotFoundError:
        print(f"Error: Tile file {tile_file} not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading tile file: {e}")
        sys.exit(1)


def run_gsutil_ls(gs_path):
    """
    List contents of a Google Cloud Storage path.
    
    Args:
        gs_path (str): The GCS path to list
        
    Returns:
        list: List of items in the path, or empty list on error
    """
    try:
        result = subprocess.run(
            ["gsutil", "ls", gs_path],
            capture_output=True,
            text=True,
            check=True
        )
        # Parse output - each line is a path
        items = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
        return items
    except subprocess.CalledProcessError as e:
        print(f"Warning: Could not list {gs_path}: {e.stderr}")
        return []
    except FileNotFoundError:
        print("Error: gsutil not found. Please install Google Cloud SDK.")
        sys.exit(1)


def download_tile_split_folders(tile_name, temp_dir):
    """
    Download all *_split folders for a given tile from GCS.
    
    Args:
        tile_name (str): Name of the tile (e.g., H10_V15)
        temp_dir (str): Temporary directory to download to
        
    Returns:
        list: List of local paths to downloaded split folders
    """
    bucket_base = f"gs://circumpolar_model_output/recent2/{tile_name}"
    
    print(f"\n{'='*60}")
    print(f"Processing tile: {tile_name}")
    print(f"{'='*60}")
    
    # List contents of the tile directory in the bucket
    tile_contents = run_gsutil_ls(f"{bucket_base}/")
    
    if not tile_contents:
        print(f"Warning: No contents found for tile {tile_name}")
        return []
    
    # Find all *_split directories
    split_dirs = [item for item in tile_contents if item.endswith('_split/')]
    
    if not split_dirs:
        print(f"Info: No *_split folders found for tile {tile_name}")
        return []
    
    print(f"Found {len(split_dirs)} *_split folder(s):")
    for split_dir in split_dirs:
        print(f"  - {split_dir}")
    
    # Download each split folder
    downloaded_paths = []
    for split_dir in split_dirs:
        # Extract scenario name from path
        scenario_name = split_dir.rstrip('/').split('/')[-1]
        local_path = os.path.join(temp_dir, tile_name, scenario_name)
        
        print(f"\nDownloading {scenario_name}...")
        
        # Download batch_*/input/run-mask.nc and batch_*/output/run_status.nc files
        # We only need these files for completion checking
        try:
            # Create local directory structure
            os.makedirs(local_path, exist_ok=True)
            
            # List all batch directories
            batch_dirs = run_gsutil_ls(split_dir)
            batch_dirs = [d for d in batch_dirs if '/batch_' in d and d.endswith('/')]
            
            if not batch_dirs:
                print(f"  Warning: No batch folders found in {split_dir}")
                continue
            
            print(f"  Found {len(batch_dirs)} batch folder(s)")
            
            # Download necessary files from each batch
            for batch_dir in batch_dirs:
                batch_name = batch_dir.rstrip('/').split('/')[-1]
                local_batch_path = os.path.join(local_path, batch_name)
                
                # Download input/run-mask.nc
                input_mask_gs = f"{batch_dir}input/run-mask.nc"
                input_mask_local = os.path.join(local_batch_path, "input")
                os.makedirs(input_mask_local, exist_ok=True)
                
                # Download output/run_status.nc
                output_status_gs = f"{batch_dir}output/run_status.nc"
                output_status_local = os.path.join(local_batch_path, "output")
                os.makedirs(output_status_local, exist_ok=True)
                
                # Try to download input mask
                subprocess.run(
                    ["gsutil", "-q", "cp", input_mask_gs, input_mask_local],
                    stderr=subprocess.DEVNULL
                )
                
                # Try to download output status
                subprocess.run(
                    ["gsutil", "-q", "cp", output_status_gs, output_status_local],
                    stderr=subprocess.DEVNULL
                )
            
            downloaded_paths.append(local_path)
            print(f"  Downloaded data for {scenario_name}")
            
        except Exception as e:
            print(f"  Error downloading {scenario_name}: {e}")
            continue
    
    return downloaded_paths


def check_completion(split_folder_path):
    """
    Check completion status of a split folder by running check_tile_run_completion.py.
    
    Args:
        split_folder_path (str): Path to the *_split folder
        
    Returns:
        tuple: (completion_percentage, completed_count, total_count) or (None, None, None) if error
    """
    check_script = os.path.expanduser("~/Circumpolar_TEM_aux_scripts/check_tile_run_completion.py")
    
    if not os.path.exists(check_script):
        print(f"Error: Check script not found at {check_script}")
        return None, None, None
    
    try:
        result = subprocess.run(
            ["python", check_script, split_folder_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        
        # Parse the output for completion percentage
        # Expected format: "Overall Completion: XX.XX%"
        # Also look for "m n" format (completed total)
        lines = result.stdout.strip().split('\n')
        
        completed = None
        total = None
        completion = None
        
        for line in lines:
            # Look for "m n" pattern (two numbers on a line)
            numbers = re.findall(r'^\s*(\d+)\s+(\d+)\s*$', line)
            if numbers:
                completed = int(numbers[0][0])
                total = int(numbers[0][1])
            
            # Look for completion percentage
            match = re.search(r"Overall Completion:\s+(\d+(?:\.\d+)?)%", line)
            if match:
                completion = float(match.group(1))
        
        return completion, completed, total
        
    except subprocess.CalledProcessError as e:
        print(f"  Error running check script: {e}")
        return None, None, None
    except Exception as e:
        print(f"  Unexpected error: {e}")
        return None, None, None


def main():
    """Main function to orchestrate tile checking process."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Check completion status of TEM model runs for tiles listed in a file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python check_incomplete_tiles.py tiles/test_tile.txt
  python check_incomplete_tiles.py tiles/test_tile.txt -o my_report.txt
  python check_incomplete_tiles.py my_tiles.txt --output results.txt
        """
    )
    parser.add_argument(
        "tile_file",
        help="Path to file containing tile names (one per line, # for comments)"
    )
    parser.add_argument(
        "-o", "--output",
        default="report.txt",
        help="Output report file (default: report.txt)"
    )
    
    args = parser.parse_args()
    tile_file = args.tile_file
    output_file = args.output
    
    # Check if tile file exists
    if not os.path.exists(tile_file):
        print(f"Error: Tile file '{tile_file}' not found")
        print("Please provide a valid tile file with one tile name per line")
        sys.exit(1)
    
    # Read tile list
    print(f"Reading tile list from: {tile_file}")
    tiles = read_tile_list(tile_file)
    print(f"Found {len(tiles)} tile(s) to process: {tiles}")
    
    if not tiles:
        print("No tiles found in the file (empty lines and comments are ignored)")
        sys.exit(0)
    
    # Create temporary directory for downloads
    temp_dir = tempfile.mkdtemp(prefix="tile_check_")
    print(f"\nUsing temporary directory: {temp_dir}")
    
    # Open report file for writing
    report = open(output_file, 'w')
    
    def write_both(message):
        """Write to both console and report file."""
        print(message)
        report.write(message + '\n')
    
    try:
        # Track all runs (both complete and incomplete)
        all_runs = []
        incomplete_runs = []
        
        write_both("="*80)
        write_both("TEM MODEL RUN COMPLETION CHECK REPORT")
        write_both("="*80)
        write_both(f"Tile file: {tile_file}")
        write_both(f"Number of tiles: {len(tiles)}")
        write_both(f"Report generated: {subprocess.run(['date'], capture_output=True, text=True).stdout.strip()}")
        write_both("="*80)
        write_both("")
        
        # Process each tile
        for tile_name in tiles:
            # Download split folders for this tile
            split_folders = download_tile_split_folders(tile_name, temp_dir)
            
            if not split_folders:
                write_both(f"\nNo split folders to check for {tile_name}")
                continue
            
            # Check completion for each split folder
            write_both(f"\nChecking completion status for {tile_name}:")
            write_both("-" * 60)
            
            for split_folder in split_folders:
                scenario_name = os.path.basename(split_folder)
                write_both(f"\n  Scenario: {scenario_name}")
                
                completion, completed, total = check_completion(split_folder)
                
                if completion is not None:
                    write_both(f"    Completed: {completed}/{total}")
                    write_both(f"    Overall Completion: {completion:.2f}%")
                    
                    # Record all runs
                    run_info = {
                        'tile': tile_name,
                        'scenario': scenario_name,
                        'completion': completion,
                        'completed': completed,
                        'total': total
                    }
                    all_runs.append(run_info)
                    
                    # Record if incomplete
                    if completion < 100.0:
                        incomplete_runs.append(run_info)
                else:
                    write_both(f"    Could not determine completion status")
                    run_info = {
                        'tile': tile_name,
                        'scenario': scenario_name,
                        'completion': None,
                        'completed': 'N/A',
                        'total': 'N/A'
                    }
                    all_runs.append(run_info)
                    incomplete_runs.append(run_info)
        
            # Clean up tile folder after processing all scenarios for this tile
            tile_folder = os.path.join(temp_dir, tile_name)
            if os.path.exists(tile_folder):
                print(f"\nCleaning up tile folder: {tile_folder}")
                shutil.rmtree(tile_folder, ignore_errors=True)


        # Summary report
        write_both("\n" + "="*80)
        write_both("SUMMARY - INCOMPLETE RUNS")
        write_both("="*80)
        
        if incomplete_runs:
            write_both(f"\nFound {len(incomplete_runs)} incomplete run(s):\n")
            
            for run in incomplete_runs:
                write_both(f"Tile: {run['tile']}")
                write_both(f"  Scenario: {run['scenario']}")
                if run['completion'] is not None:
                    write_both(f"  Completion: {run['completion']:.2f}%")
                else:
                    write_both(f"  Completion: Unknown")
                write_both(f"  Progress: {run['completed']}/{run['total']}")
                write_both("")
        else:
            write_both("\n✓ All runs are 100% complete!")
        
        # Summary table
        write_both("\n" + "="*80)
        write_both("SUMMARY TABLE")
        write_both("="*80)
        write_both("")
        
        # Table header
        header = f"{'Tile':<15} | {'Scenario':<30} | {'Progress':<15} | {'Completion %':<12}"
        write_both(header)
        write_both("-" * len(header))
        
        # Sort by tile name, then scenario name
        all_runs_sorted = sorted(all_runs, key=lambda x: (x['tile'], x['scenario']))
        
        for run in all_runs_sorted:
            tile = run['tile']
            scenario = run['scenario']
            progress = f"{run['completed']}/{run['total']}"
            
            if run['completion'] is not None:
                completion = f"{run['completion']:.2f}%"
            else:
                completion = "Unknown"
            
            row = f"{tile:<15} | {scenario:<30} | {progress:<15} | {completion:<12}"
            write_both(row)
        
        write_both("")
        write_both("="*80)
        write_both(f"Total runs checked: {len(all_runs)}")
        write_both(f"Incomplete runs: {len(incomplete_runs)}")
        write_both(f"Complete runs: {len(all_runs) - len(incomplete_runs)}")
        write_both("="*80)
        
    finally:
        # Close report file
        report.close()
        
        # Clean up temporary directory
        print(f"\nCleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"\n✓ Report saved to: {output_file}")
        print("Done!")


if __name__ == "__main__":
    main()


