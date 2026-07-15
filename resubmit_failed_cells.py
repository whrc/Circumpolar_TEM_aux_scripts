#!/usr/bin/env python3
"""
Scan a dvmdostem batch split directory for incomplete batches and resubmit only
the failed grid cells.

Algorithm per incomplete batch:
  1. Build a retry run-mask with only the failed (non-100) cells enabled.
  2. Copy the batch dir to batch_N/retry/, rewrite run-mask, config.js, slurm_runner.sh.
  3. Submit the retry Slurm job (job name: <split>-batch-N-retry).
  4. Wait until all retry jobs disappear from squeue.
  5. Merge retry/output/*.nc back into output/*.nc.

Usage:
    ~/Circumpolar_TEM_aux_scripts/resubmit_failed_cells.py /path/to/split_dir
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

try:
    import xarray as xr
    _HAS_XARRAY = True
except ImportError:
    _HAS_XARRAY = False

try:
    from netCDF4 import Dataset as NC4Dataset
    _HAS_NETCDF4 = True
except ImportError:
    _HAS_NETCDF4 = False

PARTITION = "compute"
POLL_SECONDS = 300
INITIAL_GRACE_SECONDS = 120
STABLE_EMPTY_POLLS = 2
RETRY_DIR_NAME = "retry"
RUN_MASK_VAR = "run"
RUN_STATUS_VAR = "run_status"
RUN_ENABLED_VALUE = 1
RUN_SUCCESS_VALUE = 100


# ---------------------------------------------------------------------------
# NetCDF helpers
# ---------------------------------------------------------------------------

def _open_nc(path: Path):
    """Open a NetCDF file with xarray, falling back to netCDF4."""
    if _HAS_XARRAY:
        try:
            return xr.open_dataset(path.as_posix(), engine="h5netcdf", decode_times=False)
        except Exception:
            return xr.open_dataset(path.as_posix(), engine="netcdf4", decode_times=False)
    raise RuntimeError("xarray is required but not available.")


def _read_2d_var(path: Path, var: str) -> np.ndarray:
    """Read a 2-D (Y, X) variable from a NetCDF file, squeezing singleton dims."""
    with _open_nc(path) as ds:
        if var not in ds:
            raise KeyError(f"{path}: variable '{var}' not found.")
        arr = ds[var].values
    while arr.ndim > 2:
        arr = arr[0]
    return arr.astype(float)


def count_where(arr: np.ndarray, value) -> int:
    return int(np.sum(np.isfinite(arr) & np.isclose(arr, value)))


# ---------------------------------------------------------------------------
# Batch discovery
# ---------------------------------------------------------------------------

def get_batch_dirs(split_path: Path) -> List[Path]:
    pattern = re.compile(r"^batch_(\d+)$")
    dirs = [p for p in split_path.iterdir() if p.is_dir() and pattern.match(p.name)]
    return sorted(dirs, key=lambda p: int(p.name.split("_")[1]))


def collect_incomplete_batches(split_path: Path) -> List[Dict]:
    """Return list of dicts for batches where completed < active cells."""
    incomplete = []
    for batch_dir in get_batch_dirs(split_path):
        batch_id = int(batch_dir.name.split("_")[1])
        run_mask_path = batch_dir / "input" / "run-mask.nc"
        run_status_path = batch_dir / "output" / "run_status.nc"

        if not run_status_path.exists():
            continue  # never started

        if not run_mask_path.exists():
            print(f"[WARN] batch_{batch_id}: run-mask.nc missing, skipping.")
            continue

        try:
            mask_arr = _read_2d_var(run_mask_path, RUN_MASK_VAR)
            n_active = count_where(mask_arr, RUN_ENABLED_VALUE)
        except Exception as exc:
            print(f"[WARN] batch_{batch_id}: cannot read run-mask ({exc}), skipping.")
            continue

        try:
            status_arr = _read_2d_var(run_status_path, RUN_STATUS_VAR)
            m_completed = count_where(status_arr, RUN_SUCCESS_VALUE)
        except Exception as exc:
            print(f"[WARN] batch_{batch_id}: cannot read run_status ({exc}), skipping.")
            continue

        if m_completed < n_active:
            incomplete.append({
                "id": batch_id,
                "path": batch_dir,
                "completed": m_completed,
                "active": n_active,
                "mask_arr": mask_arr,
                "status_arr": status_arr,
            })

    return incomplete


# ---------------------------------------------------------------------------
# Retry setup
# ---------------------------------------------------------------------------

def _build_retry_run_mask(mask_arr: np.ndarray, status_arr: np.ndarray) -> np.ndarray:
    """Return a new run-mask array with only the failed (non-100) cells enabled."""
    enabled = np.isfinite(mask_arr) & np.isclose(mask_arr, RUN_ENABLED_VALUE)
    completed = np.isfinite(status_arr) & np.isclose(status_arr, RUN_SUCCESS_VALUE)
    failed = enabled & ~completed
    return np.where(failed, RUN_ENABLED_VALUE, 0).astype(mask_arr.dtype)


def _write_retry_run_mask(retry_run_mask: np.ndarray, retry_mask_path: Path) -> None:
    with _open_nc(retry_mask_path) as ds_in:
        ds = ds_in.load()

    original_da = ds[RUN_MASK_VAR]
    # Detect spatial dims
    dims = original_da.dims
    spatial = [d for d in dims if d.upper() in ("Y", "X")]
    if len(spatial) < 2:
        spatial = list(dims[-2:])
    row_dim, col_dim = spatial[0], spatial[1]

    new_da = xr.DataArray(
        retry_run_mask,
        dims=(row_dim, col_dim),
        coords={row_dim: ds[row_dim].values, col_dim: ds[col_dim].values},
    )
    for d in original_da.dims:
        if d not in (row_dim, col_dim):
            new_da = new_da.expand_dims({d: ds[d].values}, axis=0)
    new_da = new_da.transpose(*original_da.dims).astype(original_da.dtype)
    new_da.attrs = original_da.attrs.copy()
    ds[RUN_MASK_VAR] = new_da

    tmp = retry_mask_path.with_suffix(".tmp.nc")
    try:
        ds.to_netcdf(tmp.as_posix(), engine="netcdf4")
        tmp.replace(retry_mask_path)
    finally:
        ds.close()
        if tmp.exists():
            tmp.unlink()


def _rewrite_retry_config(batch_path: Path, retry_path: Path) -> None:
    config_path = retry_path / "config" / "config.js"
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    src = batch_path.resolve().as_posix()
    dst = retry_path.resolve().as_posix()

    def replace(obj):
        if isinstance(obj, dict):
            return {k: replace(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [replace(v) for v in obj]
        if isinstance(obj, str):
            return obj.replace(src, dst)
        return obj

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(replace(data), f, indent=4)


def _rewrite_retry_slurm(batch_path: Path, retry_path: Path) -> None:
    slurm_path = retry_path / "slurm_runner.sh"
    content = slurm_path.read_text()

    # Replace all batch_path occurrences with retry_path
    content = content.replace(batch_path.resolve().as_posix(), retry_path.resolve().as_posix())
    content = content.replace(batch_path.as_posix(), retry_path.resolve().as_posix())

    # Append -retry to job name
    content = re.sub(
        r'(#SBATCH\s+--job-name="?)([^"\n]+?)(-retry)?("?)(\n)',
        lambda m: f'{m.group(1)}{m.group(2)}-retry{m.group(4)}{m.group(5)}',
        content,
    )

    # Update log path with -retry suffix
    def add_retry_suffix(m):
        flag, path = m.group(1), m.group(2)
        if "-retry" in path:
            return m.group(0)
        return f"#SBATCH {flag} {path}-retry"

    content = re.sub(
        r"^(#SBATCH\s+(?:-o|-e))\s+(.+)$",
        add_retry_suffix,
        content,
        flags=re.MULTILINE,
    )

    slurm_path.write_text(content)


def prepare_retry(batch: Dict) -> Path:
    """Create batch_N/retry/ with rewritten run-mask, config, and slurm script."""
    batch_path: Path = batch["path"]
    retry_path = batch_path / RETRY_DIR_NAME

    if retry_path.exists():
        print(f"  Removing existing retry/ dir...")
        shutil.rmtree(retry_path)

    print(f"  Copying batch to retry/...")
    shutil.copytree(batch_path, retry_path, ignore=shutil.ignore_patterns(RETRY_DIR_NAME))

    retry_run_mask = _build_retry_run_mask(batch["mask_arr"], batch["status_arr"])
    failed_cells = count_where(retry_run_mask, RUN_ENABLED_VALUE)
    print(f"  Failed cells to retry: {failed_cells}")

    _write_retry_run_mask(retry_run_mask, retry_path / "input" / "run-mask.nc")
    _rewrite_retry_config(batch_path, retry_path)
    _rewrite_retry_slurm(batch_path, retry_path)

    return retry_path


def submit_retry(retry_path: Path) -> None:
    slurm_script = retry_path / "slurm_runner.sh"
    result = subprocess.run(["sbatch", slurm_script.as_posix()], text=True, capture_output=True)
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed: {output}")
    print(f"  Submitted: {output}")


# ---------------------------------------------------------------------------
# Wait for Slurm
# ---------------------------------------------------------------------------

def wait_for_retry_jobs(split_path: Path, batch_ids: List[int]) -> None:
    if not batch_ids:
        return

    split_name = split_path.name
    expected = {f"{split_name}-batch-{bid}-retry" for bid in batch_ids}
    print(f"\n[WAIT] Monitoring {len(expected)} retry job(s). Poll every {POLL_SECONDS}s.")

    user = os.getenv("USER")
    if not user:
        raise EnvironmentError("USER environment variable is not set.")

    start = time.time()
    empty_streak = 0
    saw_jobs = False

    while True:
        try:
            result = subprocess.run(
                ["squeue", "-h", "-u", user, "-o", "%j"],
                check=True, text=True, capture_output=True,
            )
            active = {line.strip() for line in result.stdout.splitlines()} & expected
        except subprocess.CalledProcessError as exc:
            print(f"[WARN] squeue failed ({exc}), retrying in {POLL_SECONDS}s.")
            time.sleep(POLL_SECONDS)
            continue

        if active:
            saw_jobs = True
            empty_streak = 0
            print(f"[WAIT] {len(active)} job(s) still running. Next check in {POLL_SECONDS}s.")
            time.sleep(POLL_SECONDS)
            continue

        elapsed = time.time() - start
        if not saw_jobs and elapsed < INITIAL_GRACE_SECONDS:
            print(f"[WAIT] No jobs seen yet ({elapsed:.0f}s grace). Waiting {POLL_SECONDS}s.")
            time.sleep(POLL_SECONDS)
            continue

        empty_streak += 1
        if empty_streak >= STABLE_EMPTY_POLLS:
            msg = "Queue clear — all retry jobs finished." if saw_jobs else \
                  "No retry jobs appeared in queue during grace period."
            print(f"[WAIT] {msg}")
            return
        print(f"[WAIT] Queue empty ({empty_streak}/{STABLE_EMPTY_POLLS}). Waiting {POLL_SECONDS}s.")
        time.sleep(POLL_SECONDS)


# ---------------------------------------------------------------------------
# Merge retry outputs back
# ---------------------------------------------------------------------------

def _is_valid(arr: np.ndarray, da) -> np.ndarray:
    fill = da.attrs.get("_FillValue", da.encoding.get("_FillValue"))
    if np.issubdtype(arr.dtype, np.floating):
        valid = ~np.isnan(arr)
        if fill is not None:
            valid &= (arr != fill)
        return valid
    if fill is not None:
        return arr != fill
    return np.ones(arr.shape, dtype=bool)


def _atomic_write(ds: xr.Dataset, target: Path) -> None:
    tmp = target.with_suffix(".tmp.nc")
    try:
        ds.to_netcdf(tmp.as_posix(), engine="netcdf4")
        tmp.replace(target)
    finally:
        if tmp.exists():
            tmp.unlink()


def merge_retry(batch_path: Path) -> None:
    retry_out = batch_path / RETRY_DIR_NAME / "output"
    orig_out = batch_path / "output"
    orig_status = orig_out / "run_status.nc"
    retry_status = retry_out / "run_status.nc"

    if not retry_out.exists():
        print(f"  [WARN] retry/output/ not found, skipping merge.")
        return

    # --- merge run_status.nc ---
    with _open_nc(orig_status) as ds_in:
        orig_ds = ds_in.load()
    with _open_nc(retry_status) as ds_in:
        retry_ds = ds_in.load()

    orig_vals = orig_ds[RUN_STATUS_VAR].values.copy()
    retry_vals = retry_ds[RUN_STATUS_VAR].values
    newly_ok = (retry_vals == RUN_SUCCESS_VALUE) & (orig_vals != RUN_SUCCESS_VALUE)
    orig_vals[newly_ok] = RUN_SUCCESS_VALUE
    orig_ds[RUN_STATUS_VAR].values[:] = orig_vals

    for var in retry_ds.data_vars:
        if var == RUN_STATUS_VAR or var not in orig_ds.data_vars:
            continue
        r_arr = retry_ds[var].values
        o_arr = orig_ds[var].values.copy()
        if r_arr.shape != o_arr.shape:
            continue
        valid = _is_valid(r_arr, retry_ds[var])
        o_arr[valid] = r_arr[valid]
        orig_ds[var].values[:] = o_arr

    _atomic_write(orig_ds, orig_status)
    orig_ds.close()
    retry_ds.close()

    # --- merge other *.nc files ---
    for retry_file in sorted(retry_out.glob("*.nc")):
        if retry_file.name == "run_status.nc":
            continue
        orig_file = orig_out / retry_file.name
        if not orig_file.exists():
            shutil.copy2(retry_file, orig_file)
            print(f"  Copied: {retry_file.name}")
            continue
        try:
            with _open_nc(orig_file) as ds_in:
                o_ds = ds_in.load()
            with _open_nc(retry_file) as ds_in:
                r_ds = ds_in.load()
            for var in r_ds.data_vars:
                if var not in o_ds.data_vars:
                    continue
                r_arr = r_ds[var].values
                o_arr = o_ds[var].values.copy()
                if r_arr.shape != o_arr.shape:
                    continue
                valid = _is_valid(r_arr, r_ds[var])
                o_arr[valid] = r_arr[valid]
                o_ds[var].values[:] = o_arr
            _atomic_write(o_ds, orig_file)
            o_ds.close()
            r_ds.close()
            print(f"  Merged: {retry_file.name}")
        except Exception as exc:
            print(f"  [WARN] Merge failed for {retry_file.name} ({exc}), copying instead.")
            shutil.copy2(retry_file, orig_file)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <split_dir>")
        sys.exit(1)

    split_path = Path(sys.argv[1]).expanduser().resolve()
    if not split_path.exists() or not split_path.is_dir():
        print(f"[ERROR] Directory not found: {split_path}")
        sys.exit(1)

    print(f"[INFO] Scanning {split_path} for incomplete batches...")
    incomplete = collect_incomplete_batches(split_path)

    if not incomplete:
        print("[INFO] No incomplete batches found. Nothing to do.")
        return

    print(f"\n[INFO] Found {len(incomplete)} incomplete batch(es):")
    for b in incomplete:
        print(f"  batch_{b['id']}: {b['completed']}/{b['active']} cells completed")

    # Step 1: Prepare and submit all retry jobs
    print("\n[STEP 1] Preparing and submitting retry jobs...")
    submitted_ids = []
    for b in incomplete:
        print(f"\nbatch_{b['id']}:")
        try:
            retry_path = prepare_retry(b)
            submit_retry(retry_path)
            submitted_ids.append(b["id"])
        except Exception as exc:
            print(f"  [ERROR] batch_{b['id']}: {exc}")

    if not submitted_ids:
        print("[ERROR] No jobs were submitted.")
        sys.exit(1)

    # Step 2: Wait
    print("\n[STEP 2] Waiting for retry jobs to complete...")
    wait_for_retry_jobs(split_path, submitted_ids)

    # Step 3: Merge
    print("\n[STEP 3] Merging retry outputs back into batch outputs...")
    for b in incomplete:
        if b["id"] not in submitted_ids:
            continue
        print(f"\nbatch_{b['id']}:")
        try:
            merge_retry(b["path"])
        except Exception as exc:
            print(f"  [ERROR] merge failed: {exc}")

    # Final check
    print("\n[INFO] Re-checking completion...")
    still_incomplete = collect_incomplete_batches(split_path)
    if still_incomplete:
        print(f"[WARN] {len(still_incomplete)} batch(es) still incomplete:")
        for b in still_incomplete:
            print(f"  batch_{b['id']}: {b['completed']}/{b['active']} cells completed")
    else:
        print("[INFO] All batches are now complete.")


if __name__ == "__main__":
    main()
