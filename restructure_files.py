#!/usr/bin/env python3
"""
File restructuring script for TEM processing results.

This script reorganizes the tile processing results from the current structure:
  {tile_name}_sc/
    scenario1_split/
    scenario2_split/
    ...

To the desired structure:
  merged_tiles/
    scenario1/
      {tile_name}/
        all_merged/
        summary_plots.pdf
    scenario2/
      {tile_name}/
        all_merged/
        summary_plots.pdf
  split_tiles/
    scenario1/
      {tile_name}/
        batch_0/
        batch_1/
        ...
        logs/
        elapsed_time.txt
    scenario2/
      {tile_name}/
        batch_0/
        batch_1/
        ...
        logs/
        elapsed_time.txt

Usage:
  python restructure_files.py TILE_NAME [--base-path PATH] [--dry-run]
  
  --dry-run: Preview mode - shows what operations would be performed without
             making any actual changes. Use this to verify the expected
             directory structure and file movements before execution.
"""

import os
import shutil
import argparse

def restructure_tile_results(tile_name, base_path=None, dry_run=False):
    """
    Restructure tile processing results into organized directory structure.
    
    Args:
        tile_name (str): Name of the tile (e.g., H8_V14)
        base_path (str, optional): Base directory path. Defaults to current directory.
        dry_run (bool): If True, only show what would be done without executing.
    """
    if base_path is None:
        base_path = os.getcwd()
    
    mode_prefix = "[DRY-RUN] " if dry_run else ""
    print(f"{mode_prefix}[RESTRUCTURE] Processing tile {tile_name} in {base_path}")
    
    if dry_run:
        print(f"[DRY-RUN] Preview Mode - No changes will be made")
        print(f"Expected structure: merged_tiles/<scenario>/{tile_name}/ & split_tiles/<scenario>/{tile_name}/")
    
    # Source directory containing scenario results
    source_dir = os.path.join(base_path, f"{tile_name}_sc")
    
    if not os.path.exists(source_dir):
        print(f"[ERROR] Source directory {source_dir} does not exist")
        return False
    
    # Create target directories
    merged_tiles_dir = os.path.join(base_path, "merged_tiles")
    split_tiles_dir = os.path.join(base_path, "split_tiles")
    
    if dry_run:
        print(f"Would create: merged_tiles/ & split_tiles/ directories")
    else:
        os.makedirs(merged_tiles_dir, exist_ok=True)
        os.makedirs(split_tiles_dir, exist_ok=True)
        print(f"[RESTRUCTURE] Created target directories: {merged_tiles_dir}, {split_tiles_dir}")
    
    # Process each scenario directory
    success_count = 0
    total_scenarios = 0
    error_count = 0
    
    for item in os.listdir(source_dir):
        item_path = os.path.join(source_dir, item)
        
        if os.path.isdir(item_path) and item.endswith("_split"):
            total_scenarios += 1
            
            # Extract scenario name (remove "_split" suffix)
            scenario_name = item[:-6]  # Remove "_split"
            
            # Check if required files exist before processing
            all_merged_path = os.path.join(item_path, "all_merged")
            summary_plots_path = os.path.join(item_path, "all_merged", "summary_plots.pdf")
            
            if not os.path.exists(all_merged_path) or not os.path.exists(summary_plots_path):
                print(f"{mode_prefix}[SKIP] Scenario {scenario_name} missing required files (all_merged or summary_plots.pdf)")
                print(f"{mode_prefix}[SKIP] all_merged exists: {os.path.exists(all_merged_path)}")
                print(f"{mode_prefix}[SKIP] all_merged/summary_plots.pdf exists: {os.path.exists(summary_plots_path)}")
                error_count += 1
                continue
            
            print(f"{mode_prefix}[RESTRUCTURE] Processing scenario: {scenario_name}")
            
            # Create scenario directories in target locations
            merged_scenario_dir = os.path.join(merged_tiles_dir, scenario_name)
            split_scenario_dir = os.path.join(split_tiles_dir, scenario_name)
            
            # Create tile-specific directories
            merged_tile_dir = os.path.join(merged_scenario_dir, tile_name)
            split_tile_dir = os.path.join(split_scenario_dir, tile_name)
            
            if not dry_run:
                os.makedirs(merged_scenario_dir, exist_ok=True)
                os.makedirs(split_scenario_dir, exist_ok=True)
                os.makedirs(merged_tile_dir, exist_ok=True)
                os.makedirs(split_tile_dir, exist_ok=True)
            
            # Move files based on type
            moved_merged = False
            moved_split = False
            scenario_error = False
            
            # Collect files to move
            merged_files = []
            split_files = []
            unknown_files = []
            
            try:
                for file_item in os.listdir(item_path):
                    file_path = os.path.join(item_path, file_item)
                    
                    if file_item == "all_merged":
                        merged_files.append(file_item)
                        summary_plots_source = os.path.join(file_path, "summary_plots.pdf")
                        if os.path.exists(summary_plots_source):
                            merged_files.append("summary_plots.pdf (extracted)")
                        
                        if not dry_run:
                            target_path = os.path.join(merged_tile_dir, file_item)
                            summary_plots_target = os.path.join(merged_tile_dir, "summary_plots.pdf")
                            
                            if os.path.exists(target_path):
                                if os.path.isdir(target_path):
                                    shutil.rmtree(target_path)
                                else:
                                    os.remove(target_path)
                            
                            # Move summary_plots.pdf out first if it exists
                            if os.path.exists(summary_plots_source):
                                if os.path.exists(summary_plots_target):
                                    os.remove(summary_plots_target)
                                shutil.move(summary_plots_source, summary_plots_target)
                            
                            # Then move the all_merged directory
                            shutil.move(file_path, target_path)
                        
                        moved_merged = True
                    
                    elif file_item.startswith("batch_") or file_item in ["logs", "elapsed_time.txt"]:
                        split_files.append(file_item)
                        
                        if not dry_run:
                            target_path = os.path.join(split_tile_dir, file_item)
                            if os.path.exists(target_path):
                                if os.path.isdir(target_path):
                                    shutil.rmtree(target_path)
                                else:
                                    os.remove(target_path)
                            shutil.move(file_path, target_path)
                        
                        moved_split = True
                    
                    else:
                        unknown_files.append(file_item)
                
                # Print concise summary
                if merged_files:
                    files_str = ", ".join(merged_files)
                    action = "Would move" if dry_run else "Moving"
                    print(f"{action} to merged_tiles/{scenario_name}/{tile_name}/: {files_str}")
                
                if split_files:
                    files_str = ", ".join(split_files)
                    action = "Would move" if dry_run else "Moving"
                    print(f"{action} to split_tiles/{scenario_name}/{tile_name}/: {files_str}")
                
                if unknown_files:
                    files_str = ", ".join(unknown_files)
                    print(f"{mode_prefix}[WARNING] Unknown files (leaving in place): {files_str}")
                
            except Exception as e:
                if not dry_run:
                    print(f"[ERROR] Failed to move files for scenario {scenario_name}: {e}")
                    scenario_error = True
                    error_count += 1
                else:
                    print(f"[DRY-RUN] Would encounter error moving files for scenario {scenario_name}: {e}")
            
            if moved_merged or moved_split:
                if not scenario_error:
                    success_count += 1
                    action = "Would process" if dry_run else "Successfully processed"
                    print(f"✓ {action} scenario: {scenario_name}")
                else:
                    print(f"{mode_prefix}[ERROR] Scenario {scenario_name} had errors during processing")
            else:
                print(f"{mode_prefix}[WARNING] No files moved for scenario: {scenario_name}")
                if not dry_run:
                    error_count += 1
    
    # Summary
    print(f"\nSummary: {success_count}/{total_scenarios} scenarios processed" + (f", {error_count} errors" if error_count > 0 else ""))
    
    if error_count == 0 and total_scenarios > 0 and success_count == total_scenarios:
        if dry_run:
            print(f"Would remove source directory: {tile_name}_sc/")
        else:
            try:
                if os.path.exists(source_dir):
                    shutil.rmtree(source_dir)
                    print(f"✓ Removed source directory: {tile_name}_sc/")
            except Exception as e:
                print(f"[ERROR] Could not remove source directory {source_dir}: {e}")
                return False
    elif error_count > 0:
        retain_reason = f"({error_count} error(s))"
        print(f"Source directory {tile_name}_sc/ retained {retain_reason}")
    elif total_scenarios == 0:
        print(f"No scenarios found in {tile_name}_sc/")
    
    return error_count == 0 and total_scenarios > 0

def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(
        description="Restructure tile processing results into organized directory structure."
    )
    parser.add_argument("tile_name", help="Tile name, e.g., H8_V14")
    parser.add_argument(
        "--base-path", 
        help="Base directory path (default: current directory)", 
        default=None
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Preview mode - show what would be done without making any changes"
    )
    
    args = parser.parse_args()
    
    success = restructure_tile_results(args.tile_name, args.base_path, args.dry_run)
    
    if args.dry_run:
        print("\n[DRY-RUN] Preview completed")
    else:
        if success:
            print("\n[SUCCESS] Restructuring completed")
        else:
            print("\n[FAILED] Restructuring failed")
            exit(1)

if __name__ == "__main__":
    main()
