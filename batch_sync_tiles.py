#!/usr/bin/env python3
"""
Batch sync multiple tiles and scenarios to Google Cloud Storage bucket.

This script reads tile IDs from a file and loops over multiple scenarios,
running sync_tile_to_bucket.py for each tile/scenario combination.

Usage: python batch_sync_tiles.py <tile_file> <base_path> [options]
Example: python batch_sync_tiles.py tiles/unfinished_ak_can.txt /mnt/exacloud/ejafarov_woodwellclimate_org/test_resubmit_unfinished_sc
"""

import argparse
import os
import subprocess
import sys


def read_tile_list(tile_file):
    """
    Read tile IDs from the tile file.
    
    Args:
        tile_file: Path to the tile list file
        
    Returns:
        list: List of tile IDs (strings)
    """
    tiles = []
    if not os.path.exists(tile_file):
        print(f"Error: Tile file not found: {tile_file}")
        return tiles
    
    try:
        with open(tile_file, 'r') as f:
            for line in f:
                tile_id = line.strip()
                if tile_id and not tile_id.startswith('#'):  # Skip empty lines and comments
                    tiles.append(tile_id)
    except Exception as e:
        print(f"Error reading tile file: {e}")
    
    return tiles


def run_sync_command(tile_id, scenario, base_path, dry_run=True, extra_flags=None):
    """
    Run sync_tile_to_bucket.py for a tile/scenario combination.
    
    Args:
        tile_id: Tile identifier (e.g., H15_V4)
        scenario: Scenario name (e.g., ssp1_2_6_mri_esm2_0)
        base_path: Base path containing tile directories
        dry_run: If True, add --dry-run flag
        extra_flags: List of additional flags to pass
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Try multiple possible locations for the sync script
    possible_paths = [
        os.path.expanduser("~/Circumpolar_TEM_aux_scripts/sync_tile_to_bucket.py"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_tile_to_bucket.py"),
        "sync_tile_to_bucket.py"
    ]
    
    sync_script = None
    for path in possible_paths:
        if os.path.exists(path):
            sync_script = path
            break
    
    if not sync_script:
        print(f"Error: Sync script not found. Tried: {', '.join(possible_paths)}")
        return False
    
    # Build command
    cmd = [
        "python",
        sync_script,
        tile_id,
        scenario,
        base_path,
        "--trim",
        "--merge",
        "--force-merge",
        "--sync"
    ]
    
    if dry_run:
        cmd.append("--dry-run")
    
    if extra_flags:
        cmd.extend(extra_flags)
    
    print(f"\n{'=' * 80}")
    print(f"Processing: {tile_id} / {scenario}")
    print(f"{'=' * 80}")
    print(f"Command: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running sync command: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Batch sync multiple tiles and scenarios to GCS bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Batch sync with dry-run (default)
  %(prog)s tiles/unfinished_ak_can.txt /mnt/exacloud/ejafarov_woodwellclimate_org/test_resubmit_unfinished_sc
  
  # Actually sync (no dry-run)
  %(prog)s tiles/unfinished_ak_can.txt /mnt/exacloud/ejafarov_woodwellclimate_org/test_resubmit_unfinished_sc --no-dry-run
  
  # Use custom scenarios
  %(prog)s tiles/unfinished_ak_can.txt /path/to/data --scenarios ssp1_2_6_mri_esm2_0 ssp5_8_5_mri_esm2_0
  
  # Use custom tile file path
  %(prog)s /path/to/tiles.txt /path/to/data
        """
    )
    
    parser.add_argument(
        "tile_file",
        help="Path to tile list file (e.g., tiles/unfinished_ak_can.txt)"
    )
    
    parser.add_argument(
        "base_path",
        help="Base path containing tile directories (e.g., /mnt/exacloud/ejafarov_woodwellclimate_org/test_resubmit_unfinished_sc)"
    )
    
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=["ssp1_2_6_mri_esm2_0", "ssp5_8_5_mri_esm2_0"],
        help="List of scenarios to process (default: ssp1_2_6_mri_esm2_0 ssp5_8_5_mri_esm2_0)"
    )
    
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually perform sync operations (default is dry-run)"
    )
    
    parser.add_argument(
        "--all-merged",
        action="store_true",
        help="Sync only the all_merged subdirectory"
    )
    
    args = parser.parse_args()
    
    # Expand paths if relative
    tile_file = os.path.expanduser(args.tile_file)
    if not os.path.isabs(tile_file):
        # If relative, try relative to current directory first, then script directory
        if not os.path.exists(tile_file):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            tile_file = os.path.join(script_dir, args.tile_file)
    
    base_path = os.path.expanduser(args.base_path)
    if not os.path.isabs(base_path):
        base_path = os.path.abspath(base_path)
    
    # Read tile IDs
    tiles = read_tile_list(tile_file)
    if not tiles:
        print(f"No tiles found in {tile_file}")
        sys.exit(1)
    
    print(f"{'=' * 80}")
    print(f"BATCH SYNC OPERATION")
    print(f"{'=' * 80}")
    print(f"Tile file: {tile_file}")
    print(f"Base path: {base_path}")
    print(f"Tiles: {len(tiles)}")
    print(f"Scenarios: {', '.join(args.scenarios)}")
    print(f"Total combinations: {len(tiles) * len(args.scenarios)}")
    print(f"Dry run: {not args.no_dry_run}")
    if args.all_merged:
        print(f"Mode: all_merged only")
    print(f"{'=' * 80}\n")
    
    # Prepare extra flags
    extra_flags = []
    if args.all_merged:
        extra_flags.append("--all_merged")
    
    # Process each tile/scenario combination
    success_count = 0
    fail_count = 0
    
    for tile_id in tiles:
        for scenario in args.scenarios:
            success = run_sync_command(
                tile_id,
                scenario,
                base_path,
                dry_run=not args.no_dry_run,
                extra_flags=extra_flags
            )
            
            if success:
                success_count += 1
            else:
                fail_count += 1
    
    # Summary
    print(f"\n{'=' * 80}")
    print(f"BATCH SYNC SUMMARY")
    print(f"{'=' * 80}")
    print(f"Total processed: {len(tiles) * len(args.scenarios)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"{'=' * 80}")
    
    if fail_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()


