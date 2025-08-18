#!/usr/bin/env python3

"""
Batch split + next-scenario runner.

Usage:
  python run_splits_and_next.py --path-to-folder /absolute/output/base \
                                --tile-dir H10_V14_sc \
                                --new-scenario-script /path/to/new_scenario.py
                                [--python /usr/bin/python3]
                                [--dry-run]

What it does:
1) For each scenario in `folder_list`, constructs `folder_list_split` by appending `_split`.
2) Runs:
      cd <tile-dir>
      bp batch split -i <folder_i> -b <path_to_folder>/<folder_i>_split --p 100 --e 2000 --s 200 --t 123 --n 76
3) Then loops over the scenarios and runs:
      python new_scenario.py ssp1_2_6_access_cm2__ssp1_2_6_split <path_to_folder>/<folder_i>_split
"""
import argparse
import subprocess
from pathlib import Path
import sys

DEFAULT_FOLDER_LIST = [
    "ssp2_4_5_access_cm2__ssp2_4_5",
    "ssp3_7_0_mri_esm2_0__ssp3_7_0",
    "ssp2_4_5_mri_esm2_0__ssp2_4_5",
    "ssp5_8_5_access_cm2__ssp5_8_5",
    "ssp1_2_6_mri_esm2_0__ssp1_2_6",
    "ssp3_7_0_access_cm2__ssp3_7_0",
    "ssp5_8_5_mri_esm2_0__ssp5_8_5",
]

SOURCE_SPLIT = "ssp1_2_6_access_cm2__ssp1_2_6_split"

def run_cmd(cmd, cwd=None, dry_run=False):
    printable = " ".join(cmd)
    if cwd:
        printable += f"  (cwd={cwd})"
    print(f"[RUN] {printable}")
    if dry_run:
        return 0
    try:
        subprocess.run(cmd, cwd=cwd, check=True)
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with exit code {e.returncode}: {' '.join(cmd)}")
        return e.returncode

def main():
    parser = argparse.ArgumentParser(description="Split scenarios and apply next-scenario script across folders.")
    parser.add_argument("--path-to-folder", type=Path, required=True,
                        help="Absolute path to base folder where *_split outputs will be created (destination base).")
    parser.add_argument("--tile-dir", type=Path, required=True,
                        help="Working directory that contains the scenario folders (e.g., H10_V14_sc).")
    parser.add_argument("--new-scenario-script", type=Path, required=True,
                        help="Path to new_scenario.py (or generate_next_scenario.py) to apply to each destination split.")
    parser.add_argument("--python", default=sys.executable,
                        help="Python interpreter to use for running the next-scenario script (default: current).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing.")
    parser.add_argument("--folders", nargs="*", default=DEFAULT_FOLDER_LIST,
                        help="Optional override for the scenario folder list.")
    args = parser.parse_args()

    path_to_folder: Path = args.path_to_folder.resolve()
    tile_dir: Path = args.tile_dir.resolve()
    next_script: Path = args.new_scenario_script.resolve()
    python_exec = args.python
    dry_run = args.dry_run

    if not path_to_folder.exists():
        print(f"[ERROR] path-to-folder does not exist: {path_to_folder}")
        sys.exit(1)
    if not tile_dir.exists():
        print(f"[ERROR] tile-dir does not exist: {tile_dir}")
        sys.exit(1)
    if not next_script.exists():
        print(f"[ERROR] new-scenario-script does not exist: {next_script}")
        sys.exit(1)

    folder_list = args.folders
    folder_list_split = [f"{name}_split" for name in folder_list]

    print("[INFO] folder_list:")
    for name in folder_list:
        print(f"  - {name}")
    print("[INFO] folder_list_split:")
    for name in folder_list_split:
        print(f"  - {name}")

    # Step 1: Split each folder
    for folder_i in folder_list:
        src_path = path_to_folder / tile_dir / folder_i
        split_target = path_to_folder / tile_dir / f"{folder_i}_split"
        cmd = [
            "bp", "batch", "split",
            "-i", str(src_path),
            "-b", str(split_target),
            "--p", "100", "--e", "2000", "--s", "200", "--t", "123", "--n", "76",
        ]
        rc = run_cmd(cmd, cwd=str(tile_dir), dry_run=dry_run)
        if rc != 0:
            print(f"[WARN] Split failed for {folder_i} (continuing)")

    # Step 2: Apply next-scenario script for each folder
    for folder_i in folder_list:
        split_target = path_to_folder / tile_dir / f"{folder_i}_split"
        cmd = [python_exec, str(next_script), SOURCE_SPLIT, str(split_target)]
        print(cmd)
        rc = run_cmd(cmd, cwd=str(tile_dir), dry_run=dry_run)
        if rc != 0:
            print(f"[WARN] Next-scenario script failed for {folder_i} (continuing)")

    print("[DONE] All commands issued.")

if __name__ == "__main__":
    main()

