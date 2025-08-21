#!/usr/bin/env python3
"""
Usage:
  python generate_next_scenario.py /path/to/base_folder /path/to/scenario_folder

Steps for each batch_* in base_folder:
1. Copies restart-tr.nc from base/output/ to scenario/output/
2. Edits slurm_runner.sh in scenario/ to:
   - Ensure flags '--no-output-cleanup --restart-run' are present after '-l disabled'
   - Replace args after '--max-output-volume=-1' with '-p 0 -e 0 -s 0 -t 0 -n 76'
"""

import argparse
import shutil
from pathlib import Path

def insert_flags_after_disabled(line: str) -> str:
    """Ensure '--no-output-cleanup' and '--restart-run' follow '-l disabled'."""
    parts = line.strip().split()
    has_nl = line.endswith("\n")

    try:
        idx = parts.index("-l")
        if parts[idx + 1] != "disabled":
            return line
    except (ValueError, IndexError):
        return line

    if "--no-output-cleanup" in parts and "--restart-run" in parts:
        return line

    insert_pos = idx + 2
    if "--no-output-cleanup" not in parts:
        parts.insert(insert_pos, "--no-output-cleanup")
        insert_pos += 1
    if "--restart-run" not in parts:
        parts.insert(insert_pos, "--restart-run")

    return " ".join(parts) + ("\n" if has_nl else "")

def copy_restart_file(src_output: Path, dst_output: Path):
    """Copy restart-tr.nc from src to dst, if it exists."""
    restart_file = src_output / "restart-tr.nc"
    if not restart_file.exists():
        print(f"[INFO] Not found: {restart_file}")
        return

    dst_output.mkdir(parents=True, exist_ok=True)
    shutil.copy2(restart_file, dst_output / restart_file.name)
    #print(f"[COPY] {restart_file} -> {dst_output / restart_file.name}")

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

    if not args.base_folder.exists():
        raise SystemExit(f"[ERROR] Base folder not found: {args.base_folder}")
    if not args.scenario_folder.exists():
        raise SystemExit(f"[ERROR] Scenario folder not found: {args.scenario_folder}")

    batches = sorted([p for p in args.base_folder.iterdir() if p.is_dir() and p.name.startswith("batch_")])
    if not batches:
        print(f"[WARN] No batch_* subfolders found in {args.base_folder}")
        return

    #print(batches)

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

