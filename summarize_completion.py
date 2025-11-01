#!/usr/bin/env python3
"""
Summarize completion status for multiple tiles from a tile list file.

This script reads tile IDs from tiles/unfinished_ak_can.txt, runs 
check_tile_run_completion.py on each tile's scenario_split folder, and
displays a formatted table with completion statistics.

Usage: python summarize_completion.py <path_to_folder> <scenario>
Example: python summarize_completion.py /path/to/data ssp1_2_6_mri_esm2_0
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


def check_tile_completion(split_path, check_script):
    """
    Run check_tile_run_completion.py on a split folder and parse results.
    
    Args:
        split_path: Path to the scenario_split directory
        check_script: Path to check_tile_run_completion.py
        
    Returns:
        tuple: (completed_cells, total_cells, completion, mean_runtime) or None on error
    """
    if not os.path.exists(split_path):
        return None
    
    cmd = f"python {check_script} {split_path}"
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        output = result.stdout.strip().split("\n")
        
        completed_cells = "?"
        total_cells = "?"
        completion = ""
        mean_runtime = ""
        
        for line in output:
            line = line.strip()
            
            # Look for "m n" pattern (two numbers on a line)
            if line.replace(" ", "").isdigit() and " " in line:
                parts = line.split()
                if len(parts) == 2:
                    completed_cells, total_cells = parts
            
            elif "Overall Completion" in line:
                completion = line.split(":")[-1].strip().replace("%", "")
            
            elif "Mean total runtime" in line:
                mean_runtime = line.split(":")[-1].strip().replace("seconds", "").strip()
        
        return (completed_cells, total_cells, completion, mean_runtime)
        
    except Exception as e:
        return None


def summarize_completion(path_to_folder, scenario, tile_file):
    """
    Summarize completion for all tiles in the tile file.
    
    Args:
        path_to_folder: Base path containing tile directories
        scenario: Scenario name (e.g., ssp1_2_6_mri_esm2_0)
        tile_file: Path to the tile list file
        
    Returns:
        bool: True if at least one tile was processed successfully
    """
    # Read tile IDs from file
    tiles = read_tile_list(tile_file)
    if not tiles:
        print(f"No tiles found in {tile_file}")
        return False
    
    check_script = os.path.expanduser("~/Circumpolar_TEM_aux_scripts/check_tile_run_completion.py")
    if not os.path.exists(check_script):
        print(f"Error: Check script not found: {check_script}")
        return False
    
    # Print header
    print(f"\n{'=' * 80}")
    print(f"SUMMARIZING COMPLETION for {len(tiles)} tiles")
    print(f"Base path: {path_to_folder}")
    print(f"Scenario: {scenario}")
    print(f"{'=' * 80}\n")
    
    print(f"| {'ID':^3} | {'Path to Tile':<40} | {'# Completed Cells':^19} | {'# Total Cells':^16} | {'Completion (%)':^16} | {'Mean Run Time (s)':^19} |")
    print(f"|{'-'*5}|{'-'*42}|{'-'*21}|{'-'*18}|{'-'*18}|{'-'*21}|")
    
    success_count = 0
    
    for idx, tile_id in enumerate(tiles, 1):
        # Construct paths
        tile_path = os.path.join(path_to_folder, tile_id)
        split_path = os.path.join(path_to_folder, f"{tile_id}_sc", f"{scenario}_split")
        
        # Check completion
        result = check_tile_completion(split_path, check_script)
        
        if result:
            completed_cells, total_cells, completion, mean_runtime = result
            success_count += 1
        else:
            completed_cells = "ERROR"
            total_cells = "ERROR"
            completion = "ERROR"
            mean_runtime = "ERROR"
        
        # Display path_to_folder/tile_id in the "Path to Tile" column
        display_path = os.path.join(path_to_folder, tile_id)
        if len(display_path) > 40:
            display_path = "..." + display_path[-(40-3):]
        
        print(f"| {idx:^3} | {display_path:<40} | {completed_cells:^19} | {total_cells:^16} | {completion:^16} | {mean_runtime:^19} |")
    
    return success_count > 0


def main():
    parser = argparse.ArgumentParser(
        description="Summarize completion status for tiles from a tile list file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Summarize completion for tiles in unfinished_ak_can.txt
  %(prog)s /path/to/data ssp1_2_6_mri_esm2_0
  
  # Use custom tile file
  %(prog)s /path/to/data ssp1_2_6_mri_esm2_0 --tile-file tiles/custom_tiles.txt
        """
    )
    
    parser.add_argument(
        "path_to_folder",
        help="Base path containing tile directories (e.g., /path/to/data)"
    )
    
    parser.add_argument(
        "scenario",
        help="Scenario name (e.g., ssp1_2_6_mri_esm2_0)"
    )
    
    parser.add_argument(
        "--tile-file",
        default="tiles/unfinished_ak_can.txt",
        help="Path to tile list file (default: tiles/unfinished_ak_can.txt)"
    )
    
    args = parser.parse_args()
    
    # Expand paths if relative
    path_to_folder = os.path.expanduser(args.path_to_folder)
    if not os.path.isabs(path_to_folder):
        path_to_folder = os.path.abspath(path_to_folder)
    
    tile_file = os.path.expanduser(args.tile_file)
    if not os.path.isabs(tile_file):
        # If relative, assume it's relative to script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        tile_file = os.path.join(script_dir, args.tile_file)
    
    if not os.path.exists(path_to_folder):
        print(f"Error: Path does not exist: {path_to_folder}")
        sys.exit(1)
    
    success = summarize_completion(path_to_folder, args.scenario, tile_file)
    
    if success:
        print(f"\n{'=' * 80}")
        print("SUMMARIZATION COMPLETED")
        print(f"{'=' * 80}")
        sys.exit(0)
    else:
        print(f"\n{'=' * 80}")
        print("SUMMARIZATION FAILED")
        print(f"{'=' * 80}")
        sys.exit(1)


if __name__ == "__main__":
    main()

