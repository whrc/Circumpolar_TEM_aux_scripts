#!/usr/bin/env python3
"""
Script to download tiles from Google Cloud Storage bucket for TEM model outputs.

This script creates the necessary directory structure and downloads tiles that don't already exist locally.
It downloads both the merged model outputs and the run-mask files for each tile.

Usage:
    python download_tiles.py -tile_file tile.txt -sc ssp5_8_5_mri_esm2_0
    python download_tiles.py --tile_file tiles/test_tile.txt --scenario ssp1_2_6_mri_esm2_0
    python download_tiles_v1.py -tile_file ../tiles/canada_tiles.txt -sc ssp5_8_5_mri_esm2_0 -region /tmp/Alaska 
Arguments:
    -tile_file, --tile_file: Path to file containing tile IDs (one per line)
    -sc, --scenario: Climate scenario name (e.g., ssp5_8_5_mri_esm2_0, ssp1_2_6_mri_esm2_0)

"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

def run_gsutil_command(command):
    """
    Execute a gsutil command and handle errors.
    
    Args:
        command (list): The gsutil command as a list of strings
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"Running: {' '.join(command)}")
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"Success: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        print(f"stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print("Error: gsutil not found. Please install Google Cloud SDK.")
        return False

def check_and_create_directories(region, scenario_name):
    """
    Create region and scenario directories if they don't exist.
    
    Args:
        region (str): Region name (e.g., 'Alaska')
        scenario_name (str): Scenario name (e.g., 'ssp1_2_6_mri_esm2_0')
        
    Returns:
        str: Path to the scenario directory
    """
    # Get the directory where this script is located (merge folder)
    script_dir = Path(__file__).parent
    
    # Create region directory
    region_dir = script_dir / region
    region_dir.mkdir(exist_ok=True)
    print(f"Region directory: {region_dir}")
    
    # Create scenario directory within region
    scenario_dir = region_dir / scenario_name
    scenario_dir.mkdir(exist_ok=True)
    print(f"Scenario directory: {scenario_dir}")
    
    return str(scenario_dir)

def download_tile(region, scenario_name, tile_id, scenario_dir):
    """
    Download a single tile if it doesn't already exist.
    
    Args:
        region (str): Region name
        scenario_name (str): Scenario name  
        tile_id (str): Tile identifier (e.g., 'H10_V15')
        scenario_dir (str): Path to scenario directory
        
    Returns:
        bool: True if tile exists or was successfully downloaded
    """
    tile_dir = Path(scenario_dir) / tile_id
    
    # Check if tile directory with all_merged already exists
    if (tile_dir / "all_merged").exists():
        print(f"Tile {tile_id} already exists at {tile_dir}")
        return True
    
    print(f"Downloading tile {tile_id}...")
    
    # Create tile directory if it doesn't exist
    if not tile_dir.exists():
        tile_dir.mkdir(parents=True)
    
    # Download all_merged directory
 
    all_merged_source = f"gs://circumpolar_model_output/recent2/{tile_id}/{scenario_name}_split/all_merged"

    all_merged_dest = str(tile_dir)
    
    gsutil_cmd1 = ["gsutil", "-m", "cp", "-r", all_merged_source, all_merged_dest]
    success1 = run_gsutil_command(gsutil_cmd1)
    
    if not success1:
        print(f"Failed to download all_merged for tile {tile_id}")
        return False
    
    # Download run-mask.nc
    mask_source = f"gs://regionalinputs/CIRCUMPOLAR/{tile_id}/run-mask.nc"
    mask_dest = str(tile_dir)
    
    gsutil_cmd2 = ["gsutil", "-m", "cp", mask_source, mask_dest]
    success2 = run_gsutil_command(gsutil_cmd2)
    
    if not success2:
        print(f"Failed to download run-mask.nc for tile {tile_id}")
        return False
    
    print(f"Successfully downloaded tile {tile_id}")
    return True

def read_tile_file(tile_file_path):
    """
    Read tile IDs from a text file.
    
    Args:
        tile_file_path (str): Path to the file containing tile IDs
        
    Returns:
        list: List of tile IDs (stripped of whitespace)
    """
    tile_file = Path(tile_file_path)
    
    if not tile_file.exists():
        print(f"Error: Tile file not found: {tile_file_path}")
        sys.exit(1)
    
    with open(tile_file, 'r') as f:
        tiles = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
    
    if not tiles:
        print(f"Error: No tiles found in file: {tile_file_path}")
        sys.exit(1)
    
    return tiles

def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Download TEM model output tiles from Google Cloud Storage',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_tiles.py -tile_file tile.txt -sc ssp5_8_5_mri_esm2_0
  python download_tiles.py --tile_file tiles/test_tile.txt --scenario ssp1_2_6_mri_esm2_0
        """
    )
    
    parser.add_argument(
        '-tile_file', '--tile_file',
        type=str,
        required=True,
        help='Path to file containing tile IDs (one per line)'
    )
    
    parser.add_argument(
        '-sc', '--scenario',
        type=str,
        required=True,
        help='Climate scenario name (e.g., ssp5_8_5_mri_esm2_0, ssp1_2_6_mri_esm2_0)'
    )
    
    parser.add_argument(
        '-region', '--region',
        type=str,
        default='Alaska',
        help='Region name (default: Alaska)'
    )
    
    return parser.parse_args()

def main():
    """Main function to orchestrate the tile download process."""
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Read tiles from file
    tile_list = read_tile_file(args.tile_file)
    
    # Configuration
    region = args.region
    scenario_name = args.scenario

    print(f"Starting tile download process...")
    print(f"Tile file: {args.tile_file}")
    print(f"Region: {region}")
    print(f"Scenario: {scenario_name}")
    print(f"Number of tiles to process: {len(tile_list)}")
    print(f"Tiles: {tile_list}")
    print("-" * 50)
    
    # Create directory structure
    scenario_dir = check_and_create_directories(region, scenario_name)
    
    # Process each tile
    success_count = 0
    for tile_id in tile_list:
        print(f"\nProcessing tile: {tile_id}")
        if download_tile(region, scenario_name, tile_id, scenario_dir):
            success_count += 1
        else:
            print(f"Failed to process tile {tile_id}")
    
    # Summary
    print("\n" + "=" * 50)
    print(f"Download process complete!")
    print(f"Successfully processed {success_count}/{len(tile_list)} tiles")
    
    if success_count == len(tile_list):
        print("All tiles are now available locally.")
    else:
        print(f"Warning: {len(tile_list) - success_count} tiles failed to download.")
        sys.exit(1)

if __name__ == "__main__":
    main()



