#!/usr/bin/env python3
"""
Seed a WIEMIP split experiment with spinup restart files.

For each batch in {CASE}_split:
  1. Copy restart-sp.nc from the spin split output into the target batch output/.
  2. Set IO.restart_from in config/config.js to that restart file (absolute path).
  3. Report whether slurm_runner.sh already has the correct stage flags for a
     spin -> transient restart run (-p 0 -e 0 -s 0).

Example:
  python3 restart_setup_1ptcCO2.py
  python3 restart_setup_1ptcCO2.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

# --- defaults for Exp_bgc_noWetland 1pct CO2 transient from spin ----------------
DEFAULT_CASE = "Exp_bgc_noWetland"
DEFAULT_BASE = Path("/mnt/exacloud/ext_ejafarov_woodwellclimate_org")
DEFAULT_RESTART_SPLIT = DEFAULT_BASE / "Exp_spin_noWetland_NF1_Split1"

RESTART_FILENAME = "restart-sp.nc"
CONFIG_REL = Path("config/config.js")
SLURM_RUNNER = "slurm_runner.sh"

# dvmdostem flags for restarting after spinup (skip pr/eq/sp, run transient+scenario)
EXPECTED_SLURM_FLAGS = "-p 0 -e 0 -s 0"
MPIRUN_FLAG_RE = re.compile(
    r'"?\$BINARY"?\s+-f\s+"?\$LOCAL_CONFIG"?\s+-l\s+\S+\s+'
    r"(?:--max-output-volume=\S+\s+)?(-p\s+\d+\s+-e\s+\d+\s+-s\s+\d+\s+-t\s+\d+\s+-n\s+\d+)"
)


def batch_dirs(split_dir: Path) -> list[Path]:
    batches = [p for p in split_dir.iterdir() if p.is_dir() and p.name.startswith("batch_")]
    return sorted(batches, key=lambda p: int(p.name.split("_", 1)[1]))


def update_config(config_path: Path, restart_path: Path, dry_run: bool) -> bool:
    with config_path.open() as fh:
        cfg = json.load(fh)

    new_value = str(restart_path)
    old_value = cfg.get("IO", {}).get("restart_from", "")
    if old_value == new_value:
        return False

    cfg.setdefault("IO", {})["restart_from"] = new_value
    if not dry_run:
        with config_path.open("w") as fh:
            json.dump(cfg, fh, indent=4)
            fh.write("\n")
    return True


def copy_restart(src: Path, dst: Path, dry_run: bool) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        return
    shutil.copy2(src, dst)


def check_slurm_runner(slurm_path: Path) -> tuple[bool, str]:
    if not slurm_path.is_file():
        return False, "missing slurm_runner.sh"

    text = slurm_path.read_text()
    if EXPECTED_SLURM_FLAGS not in text:
        match = MPIRUN_FLAG_RE.search(text)
        found = match.group(1) if match else "no mpirun stage flags found"
        return False, f"expected '{EXPECTED_SLURM_FLAGS}', found '{found}'"
    return True, "ok (-p 0 -e 0 -s 0 present)"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", default=DEFAULT_CASE, help="Experiment case name")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=DEFAULT_BASE,
        help="Parent directory containing the split folders",
    )
    parser.add_argument(
        "--restart-split",
        type=Path,
        default=DEFAULT_RESTART_SPLIT,
        help="Spin split directory with restart-sp.nc outputs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without copying or editing files",
    )
    args = parser.parse_args()

    target_split = args.base_dir / f"{args.case}_split"
    if not target_split.is_dir():
        print(f"ERROR: target split not found: {target_split}", file=sys.stderr)
        return 1
    if not args.restart_split.is_dir():
        print(f"ERROR: restart split not found: {args.restart_split}", file=sys.stderr)
        return 1

    target_batches = batch_dirs(target_split)
    source_batches = {p.name: p for p in batch_dirs(args.restart_split)}
    if not target_batches:
        print(f"ERROR: no batch_* directories under {target_split}", file=sys.stderr)
        return 1

    print(f"Case:          {args.case}")
    print(f"Target split:  {target_split}")
    print(f"Restart split: {args.restart_split}")
    print(f"Batches:       {len(target_batches)} ({target_batches[0].name} .. {target_batches[-1].name})")
    if args.dry_run:
        print("Mode:          dry-run")
    print()

    copied = 0
    configs_updated = 0
    slurm_ok = 0
    slurm_bad: list[str] = []
    errors: list[str] = []

    for batch in target_batches:
        name = batch.name
        src_batch = source_batches.get(name)
        if src_batch is None:
            errors.append(f"{name}: no matching batch in restart split")
            continue

        src_restart = src_batch / "output" / RESTART_FILENAME
        dst_restart = batch / "output" / RESTART_FILENAME
        config_path = batch / CONFIG_REL
        slurm_path = batch / SLURM_RUNNER

        if not src_restart.is_file():
            errors.append(f"{name}: missing source {src_restart}")
            continue
        if not config_path.is_file():
            errors.append(f"{name}: missing {config_path}")
            continue

        print(f"{name}: copy {src_restart.name} -> {dst_restart}")
        copy_restart(src_restart, dst_restart, args.dry_run)
        copied += 1

        restart_for_config = dst_restart.resolve()
        changed = update_config(config_path, restart_for_config, args.dry_run)
        action = "update" if changed else "unchanged"
        print(f"{name}: config restart_from {action} -> {restart_for_config}")
        if changed:
            configs_updated += 1

        ok, msg = check_slurm_runner(slurm_path)
        print(f"{name}: slurm_runner.sh {msg}")
        if ok:
            slurm_ok += 1
        else:
            slurm_bad.append(f"{name}: {msg}")

    print()
    print("Summary")
    print(f"  restart copies:     {copied}")
    print(f"  configs updated:    {configs_updated}")
    print(f"  slurm_runner ok:    {slurm_ok}/{len(target_batches)}")
    if slurm_bad:
        print("  slurm_runner issues:")
        for line in slurm_bad:
            print(f"    - {line}")
        print(
            "\n  Note: for a spin -> transient restart, mpirun should include "
            f"'{EXPECTED_SLURM_FLAGS}' (skip pr/eq/sp)."
        )
    if errors:
        print("  errors:")
        for line in errors:
            print(f"    - {line}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
