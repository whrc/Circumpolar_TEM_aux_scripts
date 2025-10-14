#!/usr/bin/env python3
"""
Script to download tiles from Google Cloud Storage bucket for TEM model outputs.

This script creates the necessary directory structure and downloads tiles that don't already exist locally.
It downloads both the merged model outputs and the run-mask files for each tile.

Usage:
    python download_tiles.py

"""

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
    #odl_tile_list = ["H5_V15", "H5_V16","H6_V15", "H6_V16","H7_V15", "H7_V16",
    #                "H8_V14", "H9_V14", "H11_V15" ]
    #if tile_id in old_tile_list:
    #    all_merged_source = f"gs://circumpolar_model_output/{region}/olt_nonconst/{tile_id}_sc_split/{scenario_name}_split/all_merged"
    #else:
    #    all_merged_source = f"gs://circumpolar_model_output/{region}-v1/merged_tiles/{scenario_name}/{tile_id}/all_merged"
 
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

def main():
    """Main function to orchestrate the tile download process."""
    
    # Configuration
    region = "Alaska"
    scenario_name = "ssp5_8_5_mri_esm2_0"#"ssp1_2_6_mri_esm2_0"
    tile_list = ['H10_V15', 'H10_V14', 'H9_V19', 'H9_V18', 'H9_V17', 'H9_V16', 'H9_V15', 'H9_V14',
                'H14_V20', 'H13_V20', 'H12_V20', 'H11_V20', 'H11_V19', 'H11_V18', 'H11_V17', 'H11_V16', 'H11_V15',  
                'H11_V14', 'H10_V19', 'H10_V18', 'H10_V17', 'H10_V16',
                'H8_V18', 'H8_V17', 'H8_V16', 'H8_V15']
#    tile_list = ['H10_V14', 'H10_V15', 'H10_V16','H10_V17','H10_V18','H10_V19',
#                'H11_V14', 'H11_V15', 'H11_V16','H11_V17','H11_V18',
#                "H8_V14", "H8_V15", "H8_V16", "H8_V17", "H8_V18",
#                "H5_V15", "H9_V14","H5_V16", "H9_V15","H6_V15", "H9_V16",
#                "H6_V16", "H9_V17","H7_V15", "H9_V18","H7_V16", "H9_V19",
#                "H5_V15", "H5_V16","H6_V15", "H6_V16","H7_V15", "H7_V16",
#                "H8_V14", "H9_V14", "H11_V15" ]

    print(f"Starting tile download process...")
    print(f"Region: {region}")
    print(f"Scenario: {scenario_name}")
    print(f"Tiles to process: {tile_list}")
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


