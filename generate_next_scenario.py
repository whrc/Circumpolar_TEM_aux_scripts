#!/usr/bin/env python3
"""
Usage:
  python generate_next_scenario.py /path/to/base_folder /path/to/scenario_folder

Does two things for each subfolder under base_folder (e.g., batch_0, batch_1, ...):
1) Copies restart_tr.nc from base_folder/<batch>/output/ -> scenario_folder/<batch>/output/
2) In scenario_folder/<batch>/slurm_runner.sh, inserts '--no-output-cleanup --restart-run'
   immediately after '-l disabled' (idempotent: won’t duplicate if already present)
"""

import argparse
import shutil
from pathlib import Path

def insert_flags_after_disabled(line: str) -> str:
    """
    Insert '--no-output-cleanup --restart-run' immediately after '-l disabled'
    without duplicating flags if they already exist.
    """
    if "-l disabled" not in line:
        return line

    to_add = []
    if "--no-output-cleanup" not in line:
        to_add.append("--no-output-cleanup")
    if "--restart-run" not in line:
        to_add.append("--restart-run")

    if not to_add:
        return line  # nothing missing

    key = "-l disabled"
    idx = line.find(key)
    before = line[: idx + len(key)]
    after = line[idx + len(key):]
    insertion = " " + " ".join(to_add)

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


def insert_flags_after_disabled(line: str) -> str:
    """
    After '-l disabled', ensure the flags '--no-output-cleanup' and '--restart-run' are present.
    If both flags already exist anywhere in the line, do nothing.
    """
    # Preserve trailing newline if present
    has_nl = line.endswith("\n")
    parts = line.strip().split()

    # Require the exact pattern "-l disabled"
    try:
        l_idx = parts.index("-l")
        if parts[l_idx + 1] != "disabled":
            return line  # do nothing if "-l disabled" isn't there exactly
    except (ValueError, IndexError):
        return line  # "-l" not found or malformed

    # If both flags already exist anywhere, do nothing
    if "--no-output-cleanup" in parts and "--restart-run" in parts:
        return line

    # Insert missing flags immediately after "disabled" in a stable order
    insert_pos = l_idx + 2
    if "--no-output-cleanup" not in parts:
        parts.insert(insert_pos, "--no-output-cleanup")
        insert_pos += 1
    if "--restart-run" not in parts:
        parts.insert(insert_pos, "--restart-run")

    new_line = " ".join(parts)
    return new_line + ("\n" if has_nl else "")

def modify_slurm(slurm_path: Path):
    if not slurm_path.exists():
        print(f"[SKIP] File not found: {slurm_path}")
        return

    with open(slurm_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    changed = False
    for line in lines:
        if "mpirun" in line and "--max-output-volume=-1" in line:
            # Replace everything after --max-output-volume=-1 with the new flags
            parts = line.split("--max-output-volume=-1")
            line = parts[0] + "--max-output-volume=-1 -p 0 -e 0 -s 0 -t 0 -n 76\n"
            changed = True
        new_lines.append(line)

    if changed:
        with open(slurm_path, "w") as f:
            f.writelines(new_lines)
        print(f"[EDIT] Updated {slurm_path}")
    else:
        print(f"[OK] No changes needed in {slurm_path}")


def main():
    parser = argparse.ArgumentParser(description="Sync *_sp.nc and edit slurm_runner.sh lines.")
    parser.add_argument("folder1", type=Path, help="Source parent folder with batch_* subfolders")
    parser.add_argument("folder2", type=Path, help="Destination parent folder with batch_* subfolders")
    args = parser.parse_args()

    folder1 = args.folder1
    folder2 = args.folder2

    if not folder1.exists():
        raise SystemExit(f"[ERROR] folder1 does not exist: {folder1}")
    if not folder2.exists():
        raise SystemExit(f"[ERROR] folder2 does not exist: {folder2}")

    # Iterate over all subfolders in folder1 (e.g., batch_0, batch_1, ...)
    batch_dirs = sorted([p for p in folder1.iterdir() if p.is_dir()])
    if not batch_dirs:
        print(f"[WARN] No subfolders found in {folder1}")

    for batch_src in batch_dirs:
        batch_name = batch_src.name
        batch_dst = folder2 / batch_name

        # Step 1: copy *_sp.nc
        copy_restart_sp_file(batch_src / "output", batch_dst / "output")

        # Step 2: modify slurm_runner.sh
        modify_slurm(batch_dst / "slurm_runner.sh")

if __name__ == "__main__":
    main()

