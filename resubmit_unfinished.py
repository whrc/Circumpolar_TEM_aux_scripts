#!/usr/bin/env python3
"""
resubmit_unfinished.py

Usage:
  python resubmit_unfinished.py H7_V15_sc/ssp1_2_6_access_cm2_split/
  # optional:
  python resubmit_unfinished.py <SPLIT_DIR> --check-script ~/Circumpolar_TEM_aux_scripts/check_runs.py --dry-run
"""

import argparse
import pathlib
import re
import subprocess
import sys

LINE_RE = re.compile(
    r"""^(?P<prefix>.*?/batch_(?P<idx>\d+)/output/run_status\.nc):\s*
        (?:(?P<finished>finished)|
           m\s*=\s*(?P<m>\d+),\s*n\s*=\s*(?P<n>\d+))$""",
    re.VERBOSE
)

def run(*cmd, cwd=None):
    return subprocess.run(cmd, cwd=cwd, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("split_dir", help="Path like H7_V15_sc/ssp1_2_6_access_cm2_split/")
    ap.add_argument("--check-script", default="~/Circumpolar_TEM_aux_scripts/check_runs.py",
                    help="Path to check_runs.py (default: %(default)s)")
    ap.add_argument("--dry-run", action="store_true", help="Print actions without submitting")
    args = ap.parse_args()

    split_path = pathlib.Path(args.split_dir).resolve()
    check_script = pathlib.Path(args.check_script).expanduser().resolve()

    if not split_path.exists():
        print(f"[ERROR] split_dir not found: {split_path}", file=sys.stderr)
        sys.exit(1)
    if not check_script.exists():
        print(f"[ERROR] check_runs.py not found: {check_script}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Checking runs in: {split_path}")
    out = run(sys.executable, str(check_script), str(split_path))

    unfinished = []
    for line in out.splitlines():
        m = LINE_RE.match(line.strip())
        if not m:
            continue
        idx = int(m.group("idx"))
        if m.group("finished"):
            continue
        # If we have m,n, anything not 'finished' is treated as unfinished
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

