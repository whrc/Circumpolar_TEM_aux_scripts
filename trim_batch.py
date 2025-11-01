#!/usr/bin/env python3
"""
Script to trim batch output folders to match a reference set of files.

This script identifies batch folders with different file counts and removes
extra files to match a reference batch (typically the batch with fewer files).
It ensures all batches have the same set of filenames.

Usage:
    python trim_batch_files.py <base_directory> [options]
    
    Example:
        python trim_batch_files.py /path/to/ssp5_8_5_mri_esm2_0_split
        python trim_batch_files.py /path/to/ssp5_8_5_mri_esm2_0_split --dry-run
        python trim_batch_files.py /path/to/ssp5_8_5_mri_esm2_0_split --reference-batch batch_2
"""

import os
import sys
import argparse
from pathlib import Path
from collections import defaultdict


def get_batch_folders(base_directory):
    """
    Get all batch folders from the base directory.
    
    Args:
        base_directory (str or Path): Path to the directory containing batch folders
        
    Returns:
        list: List of (batch_name, batch_path) tuples, sorted by batch name
    """
    base_path = Path(base_directory)
    
    if not base_path.exists():
        print(f"Error: Directory '{base_directory}' does not exist")
        sys.exit(1)
    
    if not base_path.is_dir():
        print(f"Error: '{base_directory}' is not a directory")
        sys.exit(1)
    
    batches = []
    
    # Get all subdirectories that start with 'batch_'
    try:
        for item in base_path.iterdir():
            if item.is_dir() and item.name.startswith('batch_'):
                output_dir = item / 'output'
                if output_dir.exists() and output_dir.is_dir():
                    batches.append((item.name, output_dir))
                else:
                    print(f"Warning: {item.name} found but has no output folder")
    except PermissionError:
        print(f"Error: Permission denied for {base_directory}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: Could not read directory {base_directory}: {e}")
        sys.exit(1)
    
    # Sort batches by name
    batches.sort(key=lambda x: x[0])
    
    return batches


def get_files_in_directory(directory_path):
    """
    Get all files in a directory (non-recursive).
    
    Args:
        directory_path (Path): Path to the directory
        
    Returns:
        set: Set of filenames in the directory
    """
    if not directory_path.exists():
        return set()
    
    try:
        files = {item.name for item in directory_path.iterdir() if item.is_file()}
        return files
    except PermissionError:
        print(f"Warning: Permission denied for {directory_path}", file=sys.stderr)
        return set()
    except Exception as e:
        print(f"Warning: Error accessing {directory_path}: {e}", file=sys.stderr)
        return set()


def find_reference_batch(batches_info):
    """
    Find the batch with the smallest file count to use as reference.
    
    Args:
        batches_info (dict): Dictionary mapping batch names to file sets
        
    Returns:
        tuple: (batch_name, file_set) of the reference batch
    """
    min_batch = min(batches_info.items(), key=lambda x: len(x[1]))
    return min_batch


def trim_files(batch_path, files_to_keep, files_to_remove, dry_run=False):
    """
    Remove files from a batch that are not in the reference set.
    
    Args:
        batch_path (Path): Path to the batch output directory
        files_to_keep (set): Set of filenames to keep
        files_to_remove (set): Set of filenames to remove
        dry_run (bool): If True, only report what would be done
        
    Returns:
        tuple: (success_count, error_count)
    """
    success_count = 0
    error_count = 0
    
    for filename in files_to_remove:
        file_path = batch_path / filename
        
        if not file_path.exists():
            continue
        
        if dry_run:
            print(f"  [DRY RUN] Would remove: {filename}")
            success_count += 1
        else:
            try:
                file_path.unlink()
                print(f"  ✓ Removed: {filename}")
                success_count += 1
            except Exception as e:
                print(f"  ✗ Error removing {filename}: {e}", file=sys.stderr)
                error_count += 1
    
    return success_count, error_count


def main():
    """Main function to trim batch files."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Trim batch output folders to match a reference set of files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan and trim batches automatically (using smallest batch as reference)
  python trim_batch_files.py /path/to/ssp5_8_5_mri_esm2_0_split

  # Dry run to see what would be removed
  python trim_batch_files.py /path/to/ssp5_8_5_mri_esm2_0_split --dry-run

  # Specify a reference batch explicitly
  python trim_batch_files.py /path/to/ssp5_8_5_mri_esm2_0_split --reference-batch batch_2

  # Only scan without making changes
  python trim_batch_files.py /path/to/ssp5_8_5_mri_esm2_0_split --scan-only
        """
    )
    parser.add_argument(
        "base_directory",
        help="Path to directory containing batch_* folders"
    )
    parser.add_argument(
        "--reference-batch",
        help="Name of the batch to use as reference (e.g., 'batch_2'). If not specified, uses the batch with fewest files."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be removed without actually removing files"
    )
    parser.add_argument(
        "--scan-only",
        action="store_true",
        help="Only scan and report file counts without trimming"
    )
    
    args = parser.parse_args()
    base_directory = args.base_directory
    reference_batch_name = args.reference_batch
    dry_run = args.dry_run
    scan_only = args.scan_only
    
    # Get all batch folders
    batches = get_batch_folders(base_directory)
    
    if not batches:
        print(f"No batch folders found in {base_directory}")
        print("Expected folders named 'batch_1', 'batch_2', etc. with 'output' subdirectories")
        sys.exit(0)
    
    print(f"Found {len(batches)} batch folder(s) in {os.path.abspath(base_directory)}")
    print()
    
    # Scan all batches and collect file information
    batches_info = {}
    print("Scanning batch folders...")
    print()
    
    for batch_name, batch_path in batches:
        files = get_files_in_directory(batch_path)
        batches_info[batch_name] = files
        print(f"  {batch_name}: {len(files)} files")
    
    print()
    
    # Check if all batches have the same files
    file_counts = [len(files) for files in batches_info.values()]
    if len(set(file_counts)) == 1:
        print("✓ All batches already have the same number of files!")
        
        # Check if they have the same filenames
        all_files = list(batches_info.values())
        if all(files == all_files[0] for files in all_files):
            print("✓ All batches have identical file sets!")
            return
        else:
            print("⚠ Warning: Batches have same file count but different filenames")
    
    # Determine reference batch
    if reference_batch_name:
        if reference_batch_name not in batches_info:
            print(f"Error: Reference batch '{reference_batch_name}' not found")
            print(f"Available batches: {', '.join(batches_info.keys())}")
            sys.exit(1)
        reference_files = batches_info[reference_batch_name]
        print(f"Using specified reference batch: {reference_batch_name} ({len(reference_files)} files)")
    else:
        reference_batch_name, reference_files = find_reference_batch(batches_info)
        print(f"Using batch with fewest files as reference: {reference_batch_name} ({len(reference_files)} files)")
    
    print()
    print("Reference files:")
    for filename in sorted(reference_files):
        print(f"  - {filename}")
    print()
    
    if scan_only:
        print("=" * 60)
        print("SCAN ONLY MODE - No files will be modified")
        print("=" * 60)
        print()
        
        for batch_name, files in sorted(batches_info.items()):
            if batch_name == reference_batch_name:
                continue
            
            extra_files = files - reference_files
            missing_files = reference_files - files
            
            print(f"Batch: {batch_name}")
            print(f"  Current files: {len(files)}")
            print(f"  Reference files: {len(reference_files)}")
            
            if extra_files:
                print(f"  Extra files ({len(extra_files)}):")
                for filename in sorted(extra_files):
                    print(f"    - {filename}")
            
            if missing_files:
                print(f"  Missing files ({len(missing_files)}):")
                for filename in sorted(missing_files):
                    print(f"    - {filename}")
            
            if not extra_files and not missing_files:
                print(f"  ✓ Files match reference")
            
            print()
        
        return
    
    # Trim batches
    print("=" * 60)
    if dry_run:
        print("DRY RUN MODE - No files will actually be removed")
    else:
        print("TRIMMING BATCHES")
    print("=" * 60)
    print()
    
    total_removed = 0
    total_errors = 0
    
    for batch_name, batch_path in batches:
        if batch_name == reference_batch_name:
            print(f"Skipping reference batch: {batch_name}")
            print()
            continue
        
        files = batches_info[batch_name]
        files_to_remove = files - reference_files
        missing_files = reference_files - files
        
        if not files_to_remove and not missing_files:
            print(f"Batch: {batch_name}")
            print(f"  ✓ Already matches reference (no changes needed)")
            print()
            continue
        
        print(f"Batch: {batch_name}")
        print(f"  Current files: {len(files)}")
        print(f"  Files to remove: {len(files_to_remove)}")
        
        if missing_files:
            print(f"  ⚠ Warning: {len(missing_files)} files present in reference but missing here:")
            for filename in sorted(missing_files):
                print(f"    - {filename}")
        
        if files_to_remove:
            print(f"  Removing files:")
            success, errors = trim_files(batch_path, reference_files, files_to_remove, dry_run)
            total_removed += success
            total_errors += errors
        
        print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if dry_run:
        print(f"Files that would be removed: {total_removed}")
    else:
        print(f"Files removed: {total_removed}")
        print(f"Errors: {total_errors}")
    print()
    
    if not dry_run and total_removed > 0:
        print("✓ Trimming complete!")
    elif dry_run:
        print("ℹ This was a dry run. Use without --dry-run to actually remove files.")


if __name__ == "__main__":
    main()


