#!/usr/bin/env python3
import os
import sys
import numpy as np
from netCDF4 import Dataset

STATUS_SUCCESS = 100
STATUS_FAIL = -100
STATUS_TIMEOUT = -5


def count_run_ones(file_path):
    with Dataset(file_path, "r") as nc:
        run_data = np.array(nc.variables["run"][:])
        fill_value = getattr(nc.variables["run"], "_FillValue", None)
    run_flat = run_data.flatten()
    if fill_value is not None:
        run_flat = run_flat[run_flat != fill_value]
    run_flat = run_flat[~np.isnan(run_flat)]
    return int(np.sum(run_flat == 1))


def get_runtime_stats(nc_file):
    """Mean and max total_runtime for cells with run_status == 100."""
    try:
        with Dataset(nc_file, "r") as nc:
            run_status = np.array(nc.variables["run_status"][:])
            total_runtime = np.array(nc.variables["total_runtime"][:])
            valid_runtimes = total_runtime[run_status == STATUS_SUCCESS]
            if valid_runtimes.size > 0:
                return float(np.mean(valid_runtimes)), float(np.max(valid_runtimes))
            return None, None
    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        return None, None


def check_run_status(mask_file_path, nc_file):
    """
    Print per-batch status for a started run and return counts.
    Returns (success, active, fail, timeout) or None on error.
    """
    try:
        if not os.path.exists(mask_file_path):
            print(f"{mask_file_path}: File does not exist")
            return None

        n = count_run_ones(mask_file_path)

        with Dataset(nc_file, "r") as nc:
            run_status = np.array(nc.variables["run_status"][:])

        m = int(np.sum(run_status == STATUS_SUCCESS))
        fail = int(np.sum(run_status == STATUS_FAIL))
        timeout = int(np.sum(run_status == STATUS_TIMEOUT))

        if m == n:
            print(f"{nc_file}: finished")
        else:
            extra = []
            if fail:
                extra.append(f"fail={fail}")
            if timeout:
                extra.append(f"timeout={timeout}")
            suffix = f" ({', '.join(extra)})" if extra else ""
            print(f"{nc_file}: m = {m}, n = {n}{suffix}")

        return m, n, fail, timeout

    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        return None


def batch_sort_key(name):
    return int(name.split("_", 1)[1])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_runs.py <base_folder>")
        sys.exit(1)

    base_folder = sys.argv[1]
    batch_folders = sorted(
        [
            d
            for d in os.listdir(base_folder)
            if os.path.isdir(os.path.join(base_folder, d)) and d.startswith("batch_")
        ],
        key=batch_sort_key,
    )

    total_m = 0
    total_n = 0
    total_time = 0.0
    max_runtime = None
    count_n = 0
    started_batches = 0
    failed_batches = []
    batches_with_fail = 0
    batches_with_timeout = 0

    for batch_name in batch_folders:
        batch_folder = os.path.join(base_folder, batch_name, "output")
        nc_file_path = os.path.join(batch_folder, "run_status.nc")
        mask_file_path = os.path.join(base_folder, batch_name, "input", "run-mask.nc")

        if not os.path.exists(mask_file_path):
            print(f"{mask_file_path}: File does not exist")
            continue

        n = count_run_ones(mask_file_path)
        total_n += n

        if not os.path.exists(nc_file_path):
            continue

        started_batches += 1
        result = check_run_status(mask_file_path, nc_file_path)
        if result is None:
            failed_batches.append(
                {
                    "batch": batch_name,
                    "success": 0,
                    "active": n,
                    "fail": 0,
                    "timeout": 0,
                    "reason": "error reading run_status.nc",
                }
            )
            continue

        m, n_active, fail, timeout = result
        total_m += m

        if fail > 0:
            batches_with_fail += 1
        if timeout > 0:
            batches_with_timeout += 1

        mean_runtime, batch_max_runtime = get_runtime_stats(nc_file_path)
        if mean_runtime is not None:
            total_time += mean_runtime
            count_n += 1
        if batch_max_runtime is not None:
            max_runtime = (
                batch_max_runtime
                if max_runtime is None
                else max(max_runtime, batch_max_runtime)
            )

        if fail > 0 or timeout > 0 or m < n_active:
            reasons = []
            if fail > 0:
                reasons.append(f"{fail} failed cell(s)")
            if timeout > 0:
                reasons.append(f"{timeout} timeout cell(s)")
            if m < n_active and fail == 0 and timeout == 0:
                reasons.append(f"incomplete ({m}/{n_active} success)")
            failed_batches.append(
                {
                    "batch": batch_name,
                    "success": m,
                    "active": n_active,
                    "fail": fail,
                    "timeout": timeout,
                    "reason": "; ".join(reasons),
                }
            )

    if total_n > 0:
        completion_percentage = (total_m / total_n) * 100
        print(f"\nOverall Completion: {completion_percentage:.2f}%")
        print(f"Started batches: {started_batches}/{len(batch_folders)}")
        if count_n > 0:
            average_run_time = total_time / count_n
            average_run_time_min = average_run_time / 60.0
            print(
                f"Mean total runtime: {average_run_time:.2f} seconds"
                f" ({average_run_time_min:.2f} min)"
            )
            if max_runtime is not None:
                max_runtime_min = max_runtime / 60.0
                print(
                    f"Max total runtime:  {max_runtime:.2f} seconds"
                    f" ({max_runtime_min:.2f} min)"
                )
        if batches_with_fail > 0 or batches_with_timeout > 0:
            print(
                f"Batches with run_status -100 (fail): {batches_with_fail};"
                f" -5 (timeout): {batches_with_timeout}"
            )
        elif started_batches > 0:
            print("No timeout (-5) or failed (-100) cells found.")
    else:
        print("\nNo valid data found for processing.")

    if failed_batches:
        print(f"\nFailed / incomplete batches ({len(failed_batches)}):")
        for entry in failed_batches:
            print(
                f"  {entry['batch']}: {entry['success']}/{entry['active']} success"
                f" — {entry['reason']}"
            )
