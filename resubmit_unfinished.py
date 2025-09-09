#!/usr/bin/env python3
"""
resubmit_unfinished.py

Usage:
  python resubmit_unfinished.py H7_V15_sc/ssp1_2_6_access_cm2_split/
  # optional:
  python resubmit_unfinished.py <SPLIT_DIR> --dry-run
"""

import argparse
import pathlib
import re
import subprocess
import sys
import numpy as np
from netCDF4 import Dataset


def run(*cmd, cwd=None):
    return subprocess.run(cmd, cwd=cwd, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout

def count_run_ones(file_path):
    """Count the number of 1s in the 'run' variable of a NetCDF file."""
    try:
        with Dataset(file_path, "r") as ds:
            run_data = ds.variables['run'][:]
            run_flat = run_data.flatten()
            
            # Remove fill values if they exist
            if hasattr(ds.variables['run'], '_FillValue'):
                fill_value = ds.variables['run']._FillValue
                run_flat = run_flat[run_flat != fill_value]
            
            # Remove NaNs
            run_flat = run_flat[~np.isnan(run_flat)]
            
            # Count ones
            return int(np.sum(run_flat == 1))
    except Exception as e:
        print(f"[ERROR] Failed to read run mask {file_path}: {e}")
        return 0

def check_batch_status(batch_dir):
    """Check if a batch is finished by comparing completed vs expected runs.
    
    Returns:
        bool: True if batch is finished, False otherwise
    """
    run_status_nc = batch_dir / "output" / "run_status.nc"
    run_mask_nc = batch_dir / "input" / "run-mask.nc"
    
    if not run_status_nc.exists():
        print(f"[WARN] Missing run_status.nc: {run_status_nc}")
        return False
        
    if not run_mask_nc.exists():
        print(f"[WARN] Missing run-mask.nc: {run_mask_nc}")
        return False
    
    try:
        # Count expected runs from mask file
        n_expected = count_run_ones(run_mask_nc)
        
        # Count completed runs from status file
        with Dataset(run_status_nc, "r") as ds:
            run_status = ds.variables['run_status'][:]
            run_status_array = np.array(run_status)
            n_completed = int(np.sum(run_status_array == 100))
        
        is_finished = (n_completed == n_expected)
        print(f"[INFO] {batch_dir.name}: {n_completed}/{n_expected} completed ({'finished' if is_finished else 'unfinished'})")
        return is_finished
        
    except Exception as e:
        print(f"[ERROR] Failed to check batch status for {batch_dir.name}: {e}")
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("split_dir", help="Path like H7_V15_sc/ssp1_2_6_access_cm2_split/")
    ap.add_argument("--dry-run", action="store_true", help="Print actions without submitting")
    args = ap.parse_args()

    split_path = pathlib.Path(args.split_dir).resolve()

    if not split_path.exists():
        print(f"[ERROR] split_dir not found: {split_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Checking runs in: {split_path}")

    # Find all batch directories
    batch_dirs = sorted([d for d in split_path.iterdir() if d.is_dir() and re.match(r"batch_\d+$", d.name)],
                        key=lambda d: int(d.name.split("_")[1]))
    
    if not batch_dirs:
        print(f"[ERROR] No batch directories found in {split_path}")
        sys.exit(1)
    
    print(f"[INFO] Found {len(batch_dirs)} batch directories")
    
    # Check each batch for completion status
    unfinished = []
    for batch_dir in batch_dirs:
        idx = int(batch_dir.name.split("_")[1])
        if not check_batch_status(batch_dir):
            unfinished.append(idx)

    if not unfinished:
        print("[OK] All batches finished. Nothing to resubmit.")
        sys.exit(0)

    print(f"[INFO] Unfinished batches: {', '.join(map(str, sorted(unfinished)))}")

    resubmitted = 0
    for idx in sorted(unfinished):
        batch_dir = split_path / f"batch_{idx}"
        slurm = batch_dir / "slurm_runner.sh"
        if not slurm.exists():
            print(f"[WARN] Missing slurm_runner.sh: {slurm} — skipping")
            continue

        if args.dry_run:
            print(f"[DRY-RUN] sbatch {slurm}")
            resubmitted += 1
            continue

        try:
            sb_out = run("sbatch", str(slurm), cwd=batch_dir)
            print(f"[SUBMIT] batch_{idx}: {sb_out.strip()}")
            resubmitted += 1
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] batch_{idx} submission failed:\n{e.stdout}", file=sys.stderr)

    print(f"[DONE] Resubmitted {resubmitted} batch(es).")

if __name__ == "__main__":
    main()

