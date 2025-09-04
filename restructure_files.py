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
"""

import os
import shutil
import argparse

def restructure_tile_results(tile_name, base_path=None):
    """
    Restructure tile processing results into organized directory structure.
    
    Args:
        tile_name (str): Name of the tile (e.g., H8_V14)
        base_path (str, optional): Base directory path. Defaults to current directory.
    """
    if base_path is None:
        base_path = os.getcwd()
    
    print(f"[RESTRUCTURE] Processing tile {tile_name} in {base_path}")
    
    # Source directory containing scenario results
    source_dir = os.path.join(base_path, f"{tile_name}_sc")
    
    if not os.path.exists(source_dir):
        print(f"[ERROR] Source directory {source_dir} does not exist")
        return False
    
    # Create target directories
    merged_tiles_dir = os.path.join(base_path, "merged_tiles")
    split_tiles_dir = os.path.join(base_path, "split_tiles")
    
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
                print(f"[SKIP] Scenario {scenario_name} missing required files (all_merged or summary_plots.pdf)")
                print(f"[SKIP] all_merged exists: {os.path.exists(all_merged_path)}")
                print(f"[SKIP] all_merged/summary_plots.pdf exists: {os.path.exists(summary_plots_path)}")
                error_count += 1
                continue
            
            print(f"[RESTRUCTURE] Processing scenario: {scenario_name}")
            
            # Create scenario directories in target locations
            merged_scenario_dir = os.path.join(merged_tiles_dir, scenario_name)
            split_scenario_dir = os.path.join(split_tiles_dir, scenario_name)
            
            os.makedirs(merged_scenario_dir, exist_ok=True)
            os.makedirs(split_scenario_dir, exist_ok=True)
            
            # Create tile-specific directories
            merged_tile_dir = os.path.join(merged_scenario_dir, tile_name)
            split_tile_dir = os.path.join(split_scenario_dir, tile_name)
            
            os.makedirs(merged_tile_dir, exist_ok=True)
            os.makedirs(split_tile_dir, exist_ok=True)
            
            # Move files based on type
            moved_merged = False
            moved_split = False
            scenario_error = False
            
            try:
                for file_item in os.listdir(item_path):
                    file_path = os.path.join(item_path, file_item)
                    
                    if file_item == "all_merged":
                        # Move all_merged directory and extract summary_plots.pdf
                        target_path = os.path.join(merged_tile_dir, file_item)
                        print(f"[RESTRUCTURE] Moving {file_item} to merged_tiles/{scenario_name}/{tile_name}/")
                        
                        # Check if summary_plots.pdf exists inside all_merged
                        summary_plots_source = os.path.join(file_path, "summary_plots.pdf")
                        summary_plots_target = os.path.join(merged_tile_dir, "summary_plots.pdf")
                        
                        if os.path.exists(target_path):
                            if os.path.isdir(target_path):
                                shutil.rmtree(target_path)
                            else:
                                os.remove(target_path)
                        
                        # Move summary_plots.pdf out first if it exists
                        if os.path.exists(summary_plots_source):
                            print(f"[RESTRUCTURE] Extracting summary_plots.pdf to merged_tiles/{scenario_name}/{tile_name}/")
                            if os.path.exists(summary_plots_target):
                                os.remove(summary_plots_target)
                            shutil.move(summary_plots_source, summary_plots_target)
                        
                        # Then move the all_merged directory
                        shutil.move(file_path, target_path)
                        moved_merged = True
                    
                    elif file_item.startswith("batch_") or file_item in ["logs", "elapsed_time.txt"]:
                        # Move to split_tiles
                        target_path = os.path.join(split_tile_dir, file_item)
                        print(f"[RESTRUCTURE] Moving {file_item} to split_tiles/{scenario_name}/{tile_name}/")
                        if os.path.exists(target_path):
                            if os.path.isdir(target_path):
                                shutil.rmtree(target_path)
                            else:
                                os.remove(target_path)
                        shutil.move(file_path, target_path)
                        moved_split = True
                    
                    else:
                        print(f"[WARNING] Unknown file type: {file_item}, leaving in place")
                
            except Exception as e:
                print(f"[ERROR] Failed to move files for scenario {scenario_name}: {e}")
                scenario_error = True
                error_count += 1
            
            if moved_merged or moved_split:
                if not scenario_error:
                    success_count += 1
                    print(f"[RESTRUCTURE] Successfully processed scenario: {scenario_name}")
                else:
                    print(f"[ERROR] Scenario {scenario_name} had errors during processing")
            else:
                print(f"[WARNING] No files moved for scenario: {scenario_name}")
                error_count += 1
    
    # Delete source directory only if restructuring completed successfully without any errors
    print(f"[RESTRUCTURE] Summary - Total scenarios: {total_scenarios}, Successful: {success_count}, Errors: {error_count}")
    
    if error_count == 0 and total_scenarios > 0 and success_count == total_scenarios:
        try:
            if os.path.exists(source_dir):
                print(f"[CLEANUP] All scenarios processed successfully. Removing source directory: {source_dir}")
                shutil.rmtree(source_dir)
                print(f"[CLEANUP] Successfully removed source directory: {source_dir}")
        except Exception as e:
            print(f"[ERROR] Could not remove source directory {source_dir}: {e}")
            return False
    else:
        if error_count > 0:
            print(f"[INFO] Source directory {source_dir} retained due to {error_count} error(s)")
        elif total_scenarios == 0:
            print(f"[INFO] No scenarios found to process in {source_dir}")
        else:
            print(f"[INFO] Source directory {source_dir} retained (not all scenarios processed)")
    
    print(f"[RESTRUCTURE] Completed restructuring for {tile_name}. Processed {success_count}/{total_scenarios} scenarios.")
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
    
    args = parser.parse_args()
    
    success = restructure_tile_results(args.tile_name, args.base_path)
    
    if success:
        print("[SUCCESS] File restructuring completed successfully")
    else:
        print("[FAILED] File restructuring failed")
        exit(1)

if __name__ == "__main__":
    main()
