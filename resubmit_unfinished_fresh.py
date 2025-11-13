#!/usr/bin/env python3
"""
resubmit_unfinished_fresh.py

This script identifies unfinished jobs and updates their slurm_runner.sh files
by commenting out the restart-run command and adding a fresh run command.

Usage:
  python resubmit_unfinished_fresh.py H7_V15_sc/ssp1_2_6_access_cm2_split/
  # optional:
  # Preview what would be done (no changes, no submissions)
  python resubmit_unfinished_fresh.py <SPLIT_DIR> --dry-run
  # Update files but DON'T submit jobs yet
  python resubmit_unfinished_fresh.py <SPLIT_DIR> --no-submit
  # Set partition to spot
  python resubmit_unfinished_fresh.py <SPLIT_DIR> --p spot
  # Remove walltime line
  python resubmit_unfinished_fresh.py <SPLIT_DIR> --nowalltime
  # Combine options
  python resubmit_unfinished_fresh.py <SPLIT_DIR> --p spot --nowalltime --dry-run


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
        #print(f"[INFO] {batch_dir.name}: {n_completed}/{n_expected} completed ({'finished' if is_finished else 'unfinished'})")
        return is_finished
        
    except Exception as e:
        print(f"[ERROR] Failed to check batch status for {batch_dir.name}: {e}")
        return False


from pathlib import Path

def update_config_paths(path_to_scenario, batch_idx):
    """
    Updates input/output paths in config.js to reflect the new path_to_scenario and batch index.

    Args:
        path_to_scenario (str or Path): Base scenario path (e.g., /lustre/H10_V14_sc/ssp2_4_5_access_cm2_split)
        batch_idx (int): Batch number (e.g., 0, 1, 2)
    """
    config_path = Path(path_to_scenario) / f"batch_{batch_idx}/config/config.js"
    if not config_path.exists():
        print(f"[ERROR] config.js not found: {config_path}")
        return False

    batch_path = f"{path_to_scenario}/batch_{batch_idx}"
    input_path = f"{batch_path}/input"
    config_dir = f"{batch_path}/config"
    output_path = f"{batch_path}/output"

    replacements = {
        "parameter_dir": f"{batch_path}/parameters/",
        "hist_climate_file": f"{input_path}/historic-climate.nc",
        "proj_climate_file": f"{input_path}/projected-climate.nc",
        "veg_class_file": f"{input_path}/vegetation.nc",
        "drainage_file": f"{input_path}/drainage.nc",
        "soil_texture_file": f"{input_path}/soil-texture.nc",
        "co2_file": f"{input_path}/co2.nc",
        "proj_co2_file": f"{input_path}/projected-co2.nc",
        "topo_file": f"{input_path}/topo.nc",
        "fri_fire_file": f"{input_path}/fri-fire.nc",
        "hist_exp_fire_file": f"{input_path}/historic-explicit-fire.nc",
        "proj_exp_fire_file": f"{input_path}/projected-explicit-fire.nc",
        "runmask_file": f"{input_path}/run-mask.nc",
        "output_dir": f"{output_path}/",
        "output_spec_file": f"{config_dir}/output_spec.csv",
    }

    try:
        content = config_path.read_text()

        for key, new_path in replacements.items():
            # Match JSON-style: "key": "old_path",
            pattern = rf'"{key}":\s*".*?"'
            replacement = f'"{key}": "{new_path}"'
            content = re.sub(pattern, replacement, content)

        # Update cell_timelimit from 3600 to 7200
        content = re.sub(r'"cell_timelimit":\s*3600,', '"cell_timelimit": 7200,', content)

        config_path.write_text(content)
        print(f"[UPDATE] Updated paths in: {config_path}")
        print(f"[UPDATE] Updated cell_timelimit to 7200")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to update config.js: {e}")
        return False



def update_slurm_runner(path_to_scenario, slurm_file,  batch_idx, dry_run=False, partition=None, nowalltime=False):
    """
    Update slurm_runner.sh by trimming everything after 'module load openmpi'
    and appending a fresh mpirun run command.
    
    Args:
        path_to_scenario (str or Path): Base path to scenario (e.g., /.../H11_V16_sc/ssp1_2_6_gfdl_esm4_split)
        slurm_file (Path): Path to slurm_runner.sh
        tile_path (str): Full tile path (unused in this version)
        scenario (str): Scenario name (e.g., ssp5_8_5_mri_esm2_0_split)
        batch_idx (int): Batch index (e.g., 0, 1, 2)
        dry_run (bool): If True, only simulate changes without writing
        partition (str): If provided, rewrites #SBATCH -p line with this partition
        nowalltime (bool): If True, removes #SBATCH --time= line
    Returns:
        bool: True if update successful or would succeed
    """

    if not slurm_file.exists():
        print(f"[ERROR] slurm_runner.sh not found: {slurm_file}")
        return False

    try:
        lines = slurm_file.read_text().splitlines()
        new_lines = []
        openmpi_found = False

        # Construct batch path
        batch_path = f"{path_to_scenario}/batch_{batch_idx}"
        fresh_run_cmd = f"mpirun --use-hwthread-cpus /opt/apps/dvm-dos-tem/dvmdostem -f {batch_path}/config/config.js -l disabled --max-output-volume=-1 -p 100 -e 2000 -s 200 -t 124 -n 76"

        for line in lines:
            # Update SBATCH -o line
            if line.strip().startswith("#SBATCH -o"):
                new_lines.append(f"#SBATCH -o {path_to_scenario}/logs/batch-{batch_idx}")
                continue
            
            # Handle partition rewrite
            if partition and line.strip().startswith("#SBATCH -p"):
                new_lines.append(f"#SBATCH -p {partition}")
                continue
            
            # Handle walltime removal
            if nowalltime and line.strip().startswith("#SBATCH --time="):
                continue  # Skip this line (remove it)
            
            new_lines.append(line)
            if "module load openmpi" in line:
                openmpi_found = True
                break  # Stop after this line

        if not openmpi_found:
            print(f"[ERROR] 'module load openmpi' not found in {slurm_file}")
            return False

        # Append fresh run command
        new_lines.append(fresh_run_cmd)

        # Dry-run check
        if dry_run:
            print(f"[DRY-RUN] Would update {slurm_file}")
            if partition:
                print(f"[DRY-RUN] Would set partition to: {partition}")
            if nowalltime:
                print(f"[DRY-RUN] Would remove walltime line")
            #print(f"[DRY-RUN] New content:")
            #print("\n".join(new_lines))
            return True

        # Write the modified content
        slurm_file.write_text("\n".join(new_lines) + "\n")
        print(f"[UPDATE] Rewrote {slurm_file} after 'module load openmpi'")
        print(f"[UPDATE] Added fresh run command: {fresh_run_cmd}")
        if partition:
            print(f"[UPDATE] Set partition to: {partition}")
        if nowalltime:
            print(f"[UPDATE] Removed walltime line")
        update_config_paths(path_to_scenario, batch_idx)

        return True

    except Exception as e:
        print(f"[ERROR] Failed to update {slurm_file}: {e}")
        return False


def extract_paths_from_slurm(slurm_file):
    """Extract tile path and scenario from existing slurm_runner.sh.
    
    Returns:
        tuple: (tile_path, scenario) or (None, None) if not found
    """
    try:
        content = slurm_file.read_text()
        
        # Pattern to extract path from mpirun command
        # Example: /mnt/exacloud/ejafarov_woodwellclimate_org/Alaska/H8_V16_sc/ssp5_8_5_mri_esm2_0_split/batch_i/config/config.js
        path_pattern = r'/mnt/exacloud/[^/]+/Alaska/([^/]+)/([^/]+)/batch_\d+'
        match = re.search(path_pattern, content)
        
        if match:
            tile_name = match.group(1)  # e.g., H8_V16_sc
            scenario = match.group(2)   # e.g., ssp5_8_5_mri_esm2_0_split
            tile_path = f"/mnt/exacloud/ejafarov_woodwellclimate_org/Alaska/{tile_name}"
            return tile_path, scenario
        
        return None, None
        
    except Exception as e:
        print(f"[ERROR] Failed to extract paths from {slurm_file}: {e}")
        return None, None

def main():
    ap = argparse.ArgumentParser(
        description="Identify unfinished jobs and update their slurm_runner.sh files for fresh runs"
    )
    ap.add_argument("split_dir", help="Path like H7_V15_sc/ssp1_2_6_access_cm2_split/")
    ap.add_argument("--dry-run", action="store_true", help="Print actions without modifying files or submitting jobs")
    ap.add_argument("--no-submit", action="store_true", help="Update files but don't submit jobs")
    ap.add_argument("--p", dest="partition", help="Set SLURM partition (e.g., 'spot')")
    ap.add_argument("--nowalltime", action="store_true", help="Remove #SBATCH --time= line from slurm scripts")
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
        print("[OK] All batches finished. Nothing to update.")
        sys.exit(0)

    print(f"[INFO] Unfinished batches: {', '.join(map(str, sorted(unfinished)))}")

    # Loop over unfinished batches and update their slurm_runner.sh files
    updated = 0
    updated_batches = []
    for idx in sorted(unfinished):
        batch_dir = split_path / f"batch_{idx}"
        slurm = batch_dir / "slurm_runner.sh"
        
        if not slurm.exists():
            print(f"[WARN] Missing slurm_runner.sh: {slurm} — skipping")
            continue
        
        # Update the slurm_runner.sh file
        #update_slurm_runner(slurm, tile_path, scenario, idx, args.dry_run)
        if update_slurm_runner(split_path, slurm,  idx, args.dry_run, args.partition, args.nowalltime):
            updated += 1
            updated_batches.append((idx, batch_dir, slurm))
        #print('updated batches:',updated_batches)

    print(f"[DONE] Updated {updated} slurm_runner.sh file(s).")
    
    # Submit the updated jobs
    if not args.dry_run and not args.no_submit and updated_batches:
        print(f"\n[INFO] Submitting {len(updated_batches)} job(s)...")
        submitted = 0
        for idx, batch_dir, slurm in updated_batches:
            try:
                sb_out = run("sbatch", str(slurm), cwd=batch_dir)
                print(f"[SUBMIT] batch_{idx}: {sb_out.strip()}")
                submitted += 1
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] batch_{idx} submission failed:\n{e.stdout}", file=sys.stderr)
        
        print(f"[DONE] Successfully submitted {submitted} job(s).")
    elif args.no_submit:
        print(f"[INFO] --no-submit flag set. You can manually submit jobs using sbatch.")

if __name__ == "__main__":
    main()



