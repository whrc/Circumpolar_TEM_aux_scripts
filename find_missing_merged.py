#!/usr/bin/env python3
"""
Script to find and download tiles missing all_merged/ folders.
"""

import subprocess
import sys
import os
import argparse
from typing import List, Dict, Tuple
import re


def run_gsutil_command(command: List[str]) -> str:
    """Run a gsutil command and return the output."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command {' '.join(command)}: {e.stderr}", file=sys.stderr)
        return ""


def run_command(command: List[str], check: bool = True) -> Tuple[bool, str, str]:
    """Run a command and return (success, stdout, stderr)."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=check)
        return True, result.stdout.strip(), result.stderr.strip()
    except subprocess.CalledProcessError as e:
        return False, e.stdout.strip() if e.stdout else "", e.stderr.strip() if e.stderr else ""


# ============================================================================
# FIND MISSING MERGED FOLDERS
# ============================================================================

def get_all_tiles(base_path: str) -> List[str]:
    """Get all tile names from the base GCS path."""
    print("Fetching all tiles...")
    output = run_gsutil_command(['gsutil', 'ls', base_path])
    if not output:
        return []
    
    tiles = []
    for line in output.split('\n'):
        line = line.strip()
        if line.endswith('/'):
            # Extract tile name from path like gs://bucket/path/H9_V19/
            tile_name = line.rstrip('/').split('/')[-1]
            # Check if it matches the H{num}_V{num} pattern
            if re.match(r'^H\d+_V\d+$', tile_name):
                tiles.append(tile_name)
    
    return sorted(tiles)


def get_scenario_split_folders(base_path: str, tile_name: str) -> List[str]:
    """Get all scenario folders ending with '_split' for a given tile."""
    tile_path = f"{base_path}{tile_name}/"
    output = run_gsutil_command(['gsutil', 'ls', tile_path])
    if not output:
        return []
    
    split_folders = []
    for line in output.split('\n'):
        line = line.strip()
        if line.endswith('_split/'):
            # Extract just the folder name
            folder_name = line.rstrip('/').split('/')[-1]
            split_folders.append(folder_name)
    
    return split_folders


def check_all_merged_exists(base_path: str, tile_name: str, scenario_folder: str) -> bool:
    """Check if all_merged/ folder exists in the scenario folder."""
    scenario_path = f"{base_path}{tile_name}/{scenario_folder}/"
    output = run_gsutil_command(['gsutil', 'ls', scenario_path])
    if not output:
        return False
    
    for line in output.split('\n'):
        line = line.strip()
        if line.endswith('all_merged/'):
            return True
    
    return False


def find_missing_merged(base_path: str, output_file: str = "missing_merged_folders.txt") -> Dict[str, List[str]]:
    """Find tiles missing all_merged/ folders and return the results."""
    print(f"Scanning for tiles missing all_merged/ folders in {base_path}")
    print("=" * 60)
    
    # Get all tiles
    tiles = get_all_tiles(base_path)
    if not tiles:
        print("No tiles found!")
        return {}
    
    print(f"Found {len(tiles)} tiles to check")
    
    missing_merged = []
    tiles_with_issues = {}
    
    for i, tile in enumerate(tiles, 1):
        print(f"[{i}/{len(tiles)}] Checking {tile}...")
        
        # Get scenario split folders for this tile
        split_folders = get_scenario_split_folders(base_path, tile)
        
        if not split_folders:
            print(f"  WARNING: No _split folders found for {tile}")
            continue
        
        tile_issues = []
        
        for scenario_folder in split_folders:
            has_merged = check_all_merged_exists(base_path, tile, scenario_folder)
            
            if not has_merged:
                issue = f"{tile}/{scenario_folder}"
                tile_issues.append(scenario_folder)
                missing_merged.append(issue)
                print(f"  MISSING: {scenario_folder}/all_merged/")
            else:
                print(f"  OK: {scenario_folder}/all_merged/")
        
        if tile_issues:
            tiles_with_issues[tile] = tile_issues
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if not missing_merged:
        print("✓ All tiles have their all_merged/ folders!")
    else:
        print(f"✗ Found {len(missing_merged)} missing all_merged/ folders:")
        print()
        
        # Group by tile for better readability
        for tile, scenarios in tiles_with_issues.items():
            print(f"{tile}:")
            for scenario in scenarios:
                print(f"  - {scenario}/all_merged/")
        
        print(f"\nTiles with missing folders: {len(tiles_with_issues)}")
        print(f"Total missing all_merged/ folders: {len(missing_merged)}")
        
        # Save results to file
        with open(output_file, 'w') as f:
            f.write("Tiles missing all_merged/ folders:\n")
            f.write("=" * 40 + "\n\n")
            for tile, scenarios in tiles_with_issues.items():
                f.write(f"{tile}:\n")
                for scenario in scenarios:
                    f.write(f"  - {scenario}/all_merged/\n")
                f.write("\n")
        
        print(f"\nResults saved to {output_file}")
    
    return tiles_with_issues


# ============================================================================
# DOWNLOAD MISSING MERGED FOLDERS
# ============================================================================

def parse_missing_folders_file(file_path: str) -> Dict[str, List[str]]:
    """Parse the missing_merged_folders.txt file to get tiles and their missing scenarios."""
    tiles_missing = {}
    
    if not os.path.exists(file_path):
        print(f"❌ File {file_path} not found.")
        return {}
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    current_tile = None
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('=') or line.startswith('Tiles missing'):
            continue
            
        # Check if this is a tile name (doesn't start with '-')
        if not line.startswith('-') and ':' in line:
            current_tile = line.rstrip(':')
            tiles_missing[current_tile] = []
        elif line.startswith('- ') and current_tile:
            # Extract scenario folder name (remove '- ' and '/all_merged/')
            scenario = line[2:].replace('/all_merged/', '')
            tiles_missing[current_tile].append(scenario)
    
    return tiles_missing


def download_scenario_folder(base_gcs_path: str, local_base_path: str, tile: str, scenario: str) -> bool:
    """Download a scenario folder from GCS to local filesystem."""
    gcs_path = f"{base_gcs_path}{tile}/{scenario}/"
    local_path = os.path.join(local_base_path, tile, scenario)
    
    # Create local directory structure
    os.makedirs(local_path, exist_ok=True)
    
    print(f"📥 Downloading {tile}/{scenario}...")
    print(f"   From: {gcs_path}")
    print(f"   To:   {local_path}")
    
    # Use gsutil -m cp -r for parallel recursive copy
    command = ['gsutil', '-m', 'cp', '-r', gcs_path + '*', local_path + '/']
    
    success, stdout, stderr = run_command(command, check=False)
    
    if success:
        print(f"✅ Successfully downloaded {tile}/{scenario}")
        return True
    else:
        print(f"❌ Failed to download {tile}/{scenario}")
        if stderr:
            print(f"   Error: {stderr}")
        return False


def get_folder_size_estimate(gcs_path: str) -> str:
    """Get an estimate of folder size using gsutil du."""
    command = ['gsutil', 'du', '-sh', gcs_path]
    success, stdout, stderr = run_command(command, check=False)
    
    if success and stdout:
        # Extract size from output like "1.2 GiB    gs://bucket/path/"
        parts = stdout.split()
        if len(parts) >= 2:
            return f"{parts[0]} {parts[1]}"
    
    return "unknown size"


def download_missing_merged(base_gcs_path: str, local_base_path: str, 
                           missing_folders_file: str = "missing_merged_folders.txt",
                           estimate_sizes: bool = False, auto_confirm: bool = False) -> None:
    """Download scenario folders that are missing all_merged/ folders."""
    print("📦 Downloading scenario folders missing all_merged/")
    print("=" * 60)
    
    # Parse the missing folders file
    tiles_missing = parse_missing_folders_file(missing_folders_file)
    
    if not tiles_missing:
        print("❌ No missing folders found in the file.")
        return
    
    # Create base local directory
    os.makedirs(local_base_path, exist_ok=True)
    
    # Calculate total downloads
    total_downloads = sum(len(scenarios) for scenarios in tiles_missing.values())
    
    print(f"Found {len(tiles_missing)} tiles with {total_downloads} scenario folders to download")
    print(f"Download location: {local_base_path}")
    print()
    
    # Estimate sizes first (optional, can be slow)
    if not auto_confirm:
        estimate_input = input("Estimate download sizes first? (y/N): ").lower().startswith('y')
    else:
        estimate_input = estimate_sizes
    
    if estimate_input:
        print("📊 Estimating download sizes...")
        total_size_info = []
        for tile, scenarios in tiles_missing.items():
            for scenario in scenarios:
                gcs_path = f"{base_gcs_path}{tile}/{scenario}/"
                size = get_folder_size_estimate(gcs_path)
                total_size_info.append(f"  {tile}/{scenario}: {size}")
        
        print("Download size estimates:")
        for info in total_size_info:
            print(info)
        print()
    
    # Confirm download
    if not auto_confirm:
        response = input(f"Proceed with downloading {total_downloads} folders? (y/N): ")
        if not response.lower().startswith('y'):
            print("❌ Download cancelled.")
            return
    
    print("\n🚀 Starting downloads...")
    print("=" * 60)
    
    # Download each scenario folder
    downloaded = 0
    failed = 0
    
    for tile, scenarios in tiles_missing.items():
        print(f"\n📁 Processing tile {tile} ({len(scenarios)} scenarios)")
        
        for i, scenario in enumerate(scenarios, 1):
            print(f"\n[{downloaded + failed + 1}/{total_downloads}] ", end="")
            
            success = download_scenario_folder(base_gcs_path, local_base_path, tile, scenario)
            
            if success:
                downloaded += 1
            else:
                failed += 1
    
    print("\n" + "=" * 60)
    print("📊 DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"✅ Successfully downloaded: {downloaded}")
    print(f"❌ Failed downloads: {failed}")
    print(f"📂 Download location: {local_base_path}")
    
    if downloaded > 0:
        print(f"\n💡 Next steps:")
        print(f"   1. Navigate to: {local_base_path}")
        print(f"   2. Process the scenario folders to generate all_merged/")
        print(f"   3. Upload the processed results back to GCS")
    
    # Create a download log
    log_file = "download_log.txt"
    with open(log_file, 'w') as f:
        f.write("Download Log\n")
        f.write("=" * 40 + "\n\n")
        f.write(f"Total downloads attempted: {total_downloads}\n")
        f.write(f"Successful downloads: {downloaded}\n")
        f.write(f"Failed downloads: {failed}\n")
        f.write(f"Download location: {local_base_path}\n\n")
        
        f.write("Downloaded folders:\n")
        for tile, scenarios in tiles_missing.items():
            f.write(f"\n{tile}:\n")
            for scenario in scenarios:
                f.write(f"  - {scenario}/\n")
    
    print(f"\n📝 Download log saved to {log_file}")


# ============================================================================
# MAIN CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Find and download tiles missing all_merged/ folders",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find missing merged folders
  %(prog)s find
  
  # Download missing folders (interactive)
  %(prog)s download
  
  # Find and then download (with auto-confirm)
  %(prog)s both --auto-confirm
  
  # Custom paths
  %(prog)s find --base-path gs://my-bucket/path/
  %(prog)s download --local-path /my/local/path
        """
    )
    
    parser.add_argument(
        'action',
        choices=['find', 'download', 'both'],
        help='Action to perform: find missing folders, download them, or both'
    )
    
    parser.add_argument(
        '--base-path',
        default='gs://circumpolar_model_output/recent2/',
        help='Base GCS path to scan (default: gs://circumpolar_model_output/recent2/)'
    )
    
    parser.add_argument(
        '--local-path',
        default='/mnt/exacloud/dteber_woodwellclimate_org/missing_merge',
        help='Local path for downloads (default: /mnt/exacloud/dteber_woodwellclimate_org/missing_merge)'
    )
    
    parser.add_argument(
        '--output-file',
        default='missing_merged_folders.txt',
        help='Output file for missing folders list (default: missing_merged_folders.txt)'
    )
    
    parser.add_argument(
        '--estimate-sizes',
        action='store_true',
        help='Estimate download sizes before downloading'
    )
    
    parser.add_argument(
        '--auto-confirm',
        action='store_true',
        help='Skip confirmation prompts (use with caution)'
    )
    
    args = parser.parse_args()
    
    # Execute the requested action
    if args.action == 'find':
        find_missing_merged(args.base_path, args.output_file)
    
    elif args.action == 'download':
        download_missing_merged(
            args.base_path,
            args.local_path,
            args.output_file,
            args.estimate_sizes,
            args.auto_confirm
        )
    
    elif args.action == 'both':
        print("🔍 Step 1: Finding missing merged folders")
        print("=" * 60)
        tiles_with_issues = find_missing_merged(args.base_path, args.output_file)
        
        if tiles_with_issues:
            print("\n" + "=" * 60)
            print("📦 Step 2: Downloading missing folders")
            print("=" * 60)
            download_missing_merged(
                args.base_path,
                args.local_path,
                args.output_file,
                args.estimate_sizes,
                args.auto_confirm
            )
        else:
            print("\n✅ No missing folders to download!")


if __name__ == "__main__":
    main()

