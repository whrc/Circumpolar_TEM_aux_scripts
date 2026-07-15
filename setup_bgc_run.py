#!/usr/bin/env python3
"""
Setup script for the BGC run starting from restart-sp.nc.

Steps performed:
  1. Download co2_dyn.nc from GCS to a temp file.
  2. Run `bp batch split` with BGC parameters, then replace co2.nc in every
     batch input directory with the downloaded co2_dyn.nc.
  3. Copy restart-sp.nc from Special_spin_WetlandOn_split_4/batch_x/output/
     to Special_spin_WetlandOn_split_bgc/batch_x/output/.
  4. Patch each batch's slurm_runner.sh to:
       a. Add --no-output-cleanup and --restart-run after '-l disabled'.
       b. Insert a 'cp "$NFS_OUT/restart-sp.nc" "$LOCAL_OUT/restart-sp.nc"'
          line before mpirun so the restart file is available on node-local
          scratch (where the model's output_dir points at runtime).

Usage:
  python setup_bgc_run.py
  python setup_bgc_run.py --skip-split   # re-run steps 3 & 4 only
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration defaults (overridable via CLI arguments)
# ---------------------------------------------------------------------------
_DEFAULT_SOURCE_INPUT   = Path("/mnt/exacloud/ext_ejafarov_woodwellclimate_org/Special_spin_WetlandOn")
_DEFAULT_SOURCE_RESTART = Path("/mnt/exacloud/ext_ejafarov_woodwellclimate_org/Special_spin_WetlandOn_split_4")
_DEFAULT_BGC_SPLIT      = Path("/mnt/exacloud/ext_ejafarov_woodwellclimate_org/Special_spin_WetlandOn_split_bgc")

CO2_GCS = "gs://wiemip/teminputs/co2_dyn.nc"
CO2_TMP = Path("/tmp/co2_dyn.nc")

# These are set to the CLI values at runtime inside main()
SOURCE_INPUT   = _DEFAULT_SOURCE_INPUT
SOURCE_RESTART = _DEFAULT_SOURCE_RESTART
BGC_SPLIT      = _DEFAULT_BGC_SPLIT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def run_cmd(command: str) -> None:
    print(f"[RUN] {command}")
    subprocess.run(command, shell=True, check=True)


def insert_restart_flags(line: str) -> str:
    """
    Add --no-output-cleanup and --restart-run after '-l disabled' in the
    mpirun line.  Idempotent: flags already present are not duplicated.
    """
    parts = line.rstrip("\n").split()
    try:
        idx = parts.index("-l")
        if parts[idx + 1] != "disabled":
            return line
    except (ValueError, IndexError):
        return line

    if "--no-output-cleanup" in parts and "--restart-run" in parts:
        return line  # already patched

    insert_pos = idx + 2
    if "--no-output-cleanup" not in parts:
        parts.insert(insert_pos, "--no-output-cleanup")
        insert_pos += 1
    if "--restart-run" not in parts:
        parts.insert(insert_pos, "--restart-run")

    return " ".join(parts) + ("\n" if line.endswith("\n") else "")


# ---------------------------------------------------------------------------
# Step 1 – Download co2_dyn.nc
# ---------------------------------------------------------------------------
def download_co2() -> None:
    print("\n=== Step 1: Download co2_dyn.nc from GCS ===")
    run_cmd(f"gsutil cp {CO2_GCS} {CO2_TMP}")
    print(f"[OK] co2_dyn.nc saved to {CO2_TMP}")


# ---------------------------------------------------------------------------
# Step 2.1 – Split + replace co2.nc
# ---------------------------------------------------------------------------
def split_and_replace_co2() -> None:
    print("\n=== Step 2.1: Run bp batch split ===")
    run_cmd(
        f"bp batch split"
        f" -i {SOURCE_INPUT}"
        f" -b {BGC_SPLIT}"
        f" --p 0 --e 0 --s 0 --t 150"
        f" --wiemip --cells-per-batch 24"
        f" -sp compute --mpi-ranks 15"
    )

    print("\n[CO2] Replacing co2.nc in all batch input directories ...")
    if not CO2_TMP.exists():
        print(f"[ERROR] {CO2_TMP} not found – run step 1 first.")
        sys.exit(1)

    replaced = 0
    for batch_dir in sorted(BGC_SPLIT.glob("batch_*")):
        co2_path = batch_dir / "input" / "co2.nc"
        if co2_path.parent.is_dir():
            shutil.copy2(CO2_TMP, co2_path)
            print(f"  [CO2] {batch_dir.name}/input/co2.nc replaced")
            replaced += 1
        else:
            print(f"  [WARN] {batch_dir.name}/input/ not found – skipping")

    print(f"[CO2] Replaced co2.nc in {replaced} batches.")


# ---------------------------------------------------------------------------
# Step 2.2 – Copy restart-sp.nc
# ---------------------------------------------------------------------------
def copy_restart_files() -> None:
    print("\n=== Step 2.2: Copy restart-sp.nc from split_4 ===")
    if not SOURCE_RESTART.exists():
        print(f"[ERROR] Restart source not found: {SOURCE_RESTART}")
        sys.exit(1)

    batches = sorted(SOURCE_RESTART.glob("batch_*"))
    if not batches:
        print(f"[WARN] No batch_* directories found in {SOURCE_RESTART}")
        return

    copied = 0
    missing = 0
    for batch_src in batches:
        batch_name = batch_src.name
        src_restart = batch_src / "output" / "restart-sp.nc"
        dst_output = BGC_SPLIT / batch_name / "output"
        dst_output.mkdir(parents=True, exist_ok=True)

        if src_restart.exists():
            shutil.copy2(src_restart, dst_output / "restart-sp.nc")
            print(f"  [RESTART] Copied restart-sp.nc -> {batch_name}/output/")
            copied += 1
        else:
            print(f"  [WARN] restart-sp.nc not found in {batch_src}/output/ – skipping")
            missing += 1

    print(f"[RESTART] Copied {copied} files; {missing} missing.")


# Sentinel inserted before mpirun to stage restart-sp.nc to local scratch.
_RESTART_STAGE_COMMENT = "# Stage restart file from NFS to local scratch for --restart-run"
_RESTART_STAGE_CMD     = 'cp "$NFS_OUT/restart-sp.nc" "$LOCAL_OUT/restart-sp.nc"'
# Anchor comment that appears immediately before the mpirun line.
_MPIRUN_COMMENT = "# OpenMPI 4.1.x: use ROMIO instead of buggy OMPIO for NetCDF/HDF5 parallel I/O"


# ---------------------------------------------------------------------------
# Step 2.3 – Patch slurm_runner.sh
# ---------------------------------------------------------------------------
def modify_slurm_scripts() -> None:
    print("\n=== Step 2.3: Patch slurm_runner.sh with restart flags and local-scratch staging ===")
    slurm_files = sorted(BGC_SPLIT.glob("batch_*/slurm_runner.sh"))
    if not slurm_files:
        print(f"[WARN] No slurm_runner.sh files found under {BGC_SPLIT}")
        return

    patched = 0
    already_ok = 0
    for slurm_sh in slurm_files:
        text = slurm_sh.read_text()
        changed = False

        # --- 4a: add --no-output-cleanup --restart-run to the mpirun line ---
        lines = text.splitlines(keepends=True)
        new_lines = []
        for line in lines:
            if "mpirun" in line and "-l" in line and "disabled" in line:
                updated = insert_restart_flags(line)
                if updated != line:
                    line = updated
                    changed = True
            new_lines.append(line)
        text = "".join(new_lines)

        # --- 4b: insert cp restart-sp.nc before the mpirun comment block ---
        if _RESTART_STAGE_COMMENT not in text:
            old_anchor = f"{_MPIRUN_COMMENT}\nmpirun"
            new_anchor = (
                f"{_RESTART_STAGE_COMMENT}\n"
                f"{_RESTART_STAGE_CMD}\n\n"
                f"{_MPIRUN_COMMENT}\nmpirun"
            )
            if old_anchor in text:
                text = text.replace(old_anchor, new_anchor, 1)
                changed = True
            else:
                print(f"  [WARN] mpirun anchor not found in {slurm_sh.parent.name}/slurm_runner.sh – skipping stage-fix")

        if changed:
            slurm_sh.write_text(text)
            print(f"  [SLURM] Patched: {slurm_sh.parent.name}/slurm_runner.sh")
            patched += 1
        else:
            print(f"  [SLURM] Already up-to-date: {slurm_sh.parent.name}/slurm_runner.sh")
            already_ok += 1

    print(f"[SLURM] Patched {patched} files; {already_ok} already up-to-date.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--skip-split",
        action="store_true",
        help="Skip steps 1 & 2.1 (split + co2 substitution) and only run steps 2.2 & 2.3.",
    )
    parser.add_argument(
        "--source-input",
        type=Path,
        default=_DEFAULT_SOURCE_INPUT,
        metavar="DIR",
        help=f"Base input directory passed to 'bp batch split' (default: {_DEFAULT_SOURCE_INPUT})",
    )
    parser.add_argument(
        "--source-restart",
        type=Path,
        default=_DEFAULT_SOURCE_RESTART,
        metavar="DIR",
        help=f"Directory containing batch_*/output/restart-sp.nc to copy from (default: {_DEFAULT_SOURCE_RESTART})",
    )
    parser.add_argument(
        "--bgc-split",
        type=Path,
        default=_DEFAULT_BGC_SPLIT,
        metavar="DIR",
        help=f"Output directory for the new BGC split (default: {_DEFAULT_BGC_SPLIT})",
    )
    args = parser.parse_args()

    global SOURCE_INPUT, SOURCE_RESTART, BGC_SPLIT
    SOURCE_INPUT   = args.source_input
    SOURCE_RESTART = args.source_restart
    BGC_SPLIT      = args.bgc_split

    print(f"[CONFIG] source-input   : {SOURCE_INPUT}")
    print(f"[CONFIG] source-restart : {SOURCE_RESTART}")
    print(f"[CONFIG] bgc-split      : {BGC_SPLIT}")

    if not args.skip_split:
        download_co2()
        split_and_replace_co2()
    else:
        print("[INFO] --skip-split set: skipping download and bp batch split.")

    copy_restart_files()
    modify_slurm_scripts()

    print("\n=== BGC setup complete ===")


if __name__ == "__main__":
    main()
