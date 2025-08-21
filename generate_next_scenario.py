#!/usr/bin/env python3
"""
Usage:
  python generate_next_scenario.py /path/to/base_folder /path/to/scenario_folder

Does two things for each subfolder under folder1 (e.g., batch_0, batch_1, ...):
1) Copies restart_tr.nc from folder1/<batch>/output/ -> folder2/<batch>/output/
2) In folder2/<batch>/slurm_runner.sh, inserts '--no-output-cleanup --restart-run'
   immediately after '-l disabled' (idempotent: won't duplicate if already present)
"""

import argparse
import shutil
import re
from pathlib import Path
from typing import Tuple

def insert_flags_after_disabled(line: str) -> str:
    """
    Insert '--no-output-cleanup --restart-run' immediately after '-l disabled'
    without duplicating flags if they already exist.
    """
    if "-l disabled" not in line:
        return line

    # If flags already exist, don't insert them again
    if "--no-output-cleanup" in line or "--restart-run" in line:
        return line

    # Insert flags after '-l disabled'
    key = "-l disabled"
    idx = line.find(key)
    before = line[: idx + len(key)]
    after = line[idx + len(key):]
    insertion = " --no-output-cleanup --restart-run"

    return before + insertion + after


def copy_restart_sp_file(src_output: Path, dst_output: Path):
    """
    Copies only restart-sp.nc from src_output to dst_output.
    """
    if not src_output.exists():
        print(f"[SKIP] Source output folder not found: {src_output}")
        return

    dst_output.mkdir(parents=True, exist_ok=True)
    src_file = src_output / "restart-*.nc"

    restart_files=['restart-tr.nc']#,'restart-sp.nc','restart-pr.nc']
    copied = False
    for name in restart_files:
        src_file = src_output / name
        print(src_file)
        if src_file.is_file():  # ← FIXED: check the Path, not the string
            shutil.copy2(src_file, dst_output / src_file.name)
            print(f"[COPY] {src_file} -> {dst_output / src_file.name}")
            copied_any = True
        else:
            print(f"[INFO] Not found: {src_file}")

    if not copied:
        print(f"[INFO] No restart-*.nc files found in {src_output}")

def modify_slurm_command(line: str) -> Tuple[str, bool]:
    """
    Modify a mpirun dvmdostem command line to:
    1. Insert '--no-output-cleanup --restart-run' after '-l disabled' (without duplicating)
    2. Replace -p, -e, -s parameters with 0 values while preserving -t and -n
    
    Returns: (modified_line, was_changed)
    """
    if not ("mpirun" in line and "dvmdostem" in line and "-l disabled" in line):
        return line, False
    
    original_line = line
    
    # Step 1: Insert flags after '-l disabled' without duplicating
    line = insert_flags_after_disabled(line)
    
    # Step 2: Use regex to robustly replace parameters
    # Pattern matches: -p <number> -e <number> -s <number> -t <number> -n <number>
    # We want to replace the first three with 0 while keeping -t and -n values
    
    def replace_params(match):
        p_val = match.group(1)  # -p value (replace with 0)
        e_val = match.group(2)  # -e value (replace with 0) 
        s_val = match.group(3)  # -s value (replace with 0)
        t_val = match.group(4)  # -t value (replace with 0)
        n_val = match.group(5)  # -n value (keep)
        return f"-p 0 -e 0 -s 0 -t 0 -n {n_val}"
    
    # Pattern to match -p <num> -e <num> -s <num> -t <num> -n <num>
    param_pattern = r'-p\s+(\d+)\s+-e\s+(\d+)\s+-s\s+(\d+)\s+-t\s+(\d+)\s+-n\s+(\d+)'
    line = re.sub(param_pattern, replace_params, line)
    
    return line, line != original_line


def modify_slurm(slurm_path: Path):
    """Update slurm_runner.sh with required flags and args."""
    if not slurm_path.exists():
        print(f"[SKIP] File not found: {slurm_path}")
        return

    with open(slurm_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    changed = False

    for line in lines:
        new_line, line_changed = modify_slurm_command(line)
        if line_changed:
            changed = True
        new_lines.append(new_line)

    if changed:
        with open(slurm_path, "w") as f:
            f.writelines(new_lines)
        #print(f"[EDIT] Updated {slurm_path}")
    else:
        print(f"[OK] No changes needed in {slurm_path}")

def modify_slurm_walltime(slurm_path: Path):
    """Update slurm_runner.sh with required flags and args."""
    if not slurm_path.exists():
        print(f"[SKIP] File not found: {slurm_path}")
        return

    with open(slurm_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    changed = False
    time_line = "#SBATCH --time=00:60:00   # <-- set max wall time to 25 minutes\n"
    time_inserted = any("--time=" in line for line in lines)

    for i, line in enumerate(lines):
        # Insert wall time right after "#SBATCH -N 1"
        if "#SBATCH -N 1" in line and not time_inserted:
            new_lines.append(line)
            new_lines.append(time_line)
            time_inserted = True
            changed = True
            continue

        # Replace args after --max-output-volume=-1
        if "mpirun" in line and "--max-output-volume=-1" in line:
            line = line.split("--max-output-volume=-1")[0] + "--max-output-volume=-1 -p 0 -e 0 -s 0 -t 0 -n 76\n"
            changed = True

        # Ensure restart flags are in place
        if "mpirun" in line and "-l" in line and "disabled" in line:
            updated_line = insert_flags_after_disabled(line)
            if updated_line != line:
                line = updated_line
                changed = True

        new_lines.append(line)

    if changed:
        with open(slurm_path, "w") as f:
            f.writelines(new_lines)
        #print(f"[EDIT] Updated {slurm_path}")
    else:
        print(f"[OK] No changes needed in {slurm_path}")

def main():
    parser = argparse.ArgumentParser(description="Copy restart files and update slurm scripts.")
    parser.add_argument("base_folder", type=Path, help="Source with batch_* folders")
    parser.add_argument("scenario_folder", type=Path, help="Destination with batch_* folders")
    args = parser.parse_args()

    folder1 = args.folder1
    folder2 = args.folder2

    if not folder1.exists():
        raise SystemExit(f"[ERROR] folder1 does not exist: {folder1}")
    if not folder2.exists():
        raise SystemExit(f"[ERROR] folder2 does not exist: {folder2}")

    # Iterate over all batch_N folders in folder1 (e.g., batch_0, batch_1, ...)
    batch_dirs = sorted([p for p in folder1.iterdir() if p.is_dir() and p.name.startswith('batch_')])
    if not batch_dirs:
        print(f"[WARN] No subfolders found in {folder1}")

    for batch_src in batches:
        batch_name = batch_src.name
        batch_dst = args.scenario_folder / batch_name
        #print(batch_src)
        #print(batch_dst)

        # 1. Copy restart-tr.nc
        copy_restart_file(batch_src / "output", batch_dst / "output")
        #print(f"[OK] Restart files are copied.")

        # 2. Modify slurm_runner.sh
        #modify_slurm(batch_dst / "slurm_runner.sh")
        modify_slurm_walltime(batch_dst / "slurm_runner.sh")
        #print(f"[EDIT] Updated slurm job.")

if __name__ == "__main__":
    main()
