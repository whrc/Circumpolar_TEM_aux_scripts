#!/usr/bin/env python3
"""
Memory-efficient merge of dvmdostem batch output NetCDF files.

Why this exists
---------------
`bp batch merge` builds the entire merged canvas in RAM before writing,
which OOM-kills the process on a 14 GB login node when processing large
variables like TLAYER_monthly_tr (28 GB uncompressed at global scale).

Strategy
--------
For each output variable file (e.g. LAI_monthly_tr.nc):
  1. Create the output NetCDF on disk with global canvas dimensions
     (time, [pft|layer], global_Y, global_X) pre-filled with FillValue.
  2. Loop over every batch in order.  For each batch, open its file,
     read the data (≤882 MB for the largest variable), and write it
     directly into the correct spatial slice of the output file.
  3. Close and move on to the next variable.

Peak RAM = max(one batch file size) ≈ 882 MB (TLAYER, 22 layers, 1800 t).

Usage
-----
  python merge_batches.py -b /mnt/exacloud/.../Special_spin_WetlandOn_split_4
  python merge_batches.py -b /path/to/split_dir -o /path/to/output_dir
  python merge_batches.py -b /path/to/split_dir --pattern "*_tr.nc"
  python merge_batches.py -b /path/to/split_dir --time-chunk 200   # extra-safe
"""

import argparse
import json
import sys
from pathlib import Path

import netCDF4 as nc
import numpy as np


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_batch_info(batch_dir: Path) -> dict:
    info_path = batch_dir / "batch_info.json"
    if not info_path.exists():
        raise FileNotFoundError(f"batch_info.json not found in {batch_dir}")
    return json.loads(info_path.read_text())


def collect_batches(split_dir: Path) -> list[Path]:
    batches = sorted(
        [d for d in split_dir.iterdir() if d.is_dir() and d.name.startswith("batch_")],
        key=lambda p: int(p.name.split("_")[1]),
    )
    if not batches:
        raise RuntimeError(f"No batch_* directories found in {split_dir}")
    return batches


def get_variable_files(batches: list[Path], pattern: str) -> list[str]:
    """Return sorted list of output file names present in the first batch."""
    ref_output = batches[0] / "output"
    files = sorted(
        f.name for f in ref_output.glob(pattern)
        if "restart" not in f.name and "run_status" not in f.name
    )
    return files


def copy_attributes(src_var, dst_var) -> None:
    for attr in src_var.ncattrs():
        if attr == "_FillValue":
            continue
        setattr(dst_var, attr, getattr(src_var, attr))


def get_extra_dim(src_ds: nc.Dataset) -> tuple[str | None, int | None]:
    """Return (dim_name, size) for pft or layer dimensions, or (None, None)."""
    for dim_name in ("pft", "layer"):
        if dim_name in src_ds.dimensions:
            return dim_name, src_ds.dimensions[dim_name].size
    return None, None


# ---------------------------------------------------------------------------
# Core merge
# ---------------------------------------------------------------------------

def merge_variable(
    var_name: str,
    batches: list[Path],
    batch_infos: list[dict],
    global_Y: int,
    global_X: int,
    output_dir: Path,
    time_chunk: int | None,
) -> None:
    print(f"  Creating canvas for {var_name}")

    # --- read metadata from the first batch that has the file ---
    src_path = None
    for batch in batches:
        candidate = batch / "output" / var_name
        if candidate.exists():
            src_path = candidate
            break
    if src_path is None:
        print(f"  [SKIP] {var_name} not found in any batch")
        return

    with nc.Dataset(src_path) as src_ds:
        n_time = src_ds.dimensions["time"].size
        time_units = src_ds.variables["time"].units if "time" in src_ds.variables else None
        time_calendar = getattr(src_ds.variables.get("time"), "calendar", "standard")
        time_data = src_ds.variables["time"][:].copy()

        extra_dim, extra_size = get_extra_dim(src_ds)

        data_var_name = [v for v in src_ds.variables if v not in src_ds.dimensions][0]
        fill_value = getattr(src_ds.variables[data_var_name], "_FillValue", -9999.0)
        dtype = src_ds.variables[data_var_name].dtype

        # Collect all variable attributes for later
        var_attrs = {
            a: getattr(src_ds.variables[data_var_name], a)
            for a in src_ds.variables[data_var_name].ncattrs()
            if a != "_FillValue"
        }

    # --- create output file ---
    out_path = output_dir / var_name
    with nc.Dataset(out_path, "w", format="NETCDF4") as dst_ds:
        # Dimensions
        dst_ds.createDimension("time", n_time)
        dst_ds.createDimension("y", global_Y)
        dst_ds.createDimension("x", global_X)
        if extra_dim:
            dst_ds.createDimension(extra_dim, extra_size)

        # Time variable
        time_var = dst_ds.createVariable("time", "f8", ("time",))
        time_var[:] = time_data
        if time_units:
            time_var.units = time_units
        time_var.calendar = time_calendar

        # Data variable – chunk layout optimised for time-slice reads (plotting).
        # Keep time chunks small (≤120 steps) so reading a single year from the
        # merged file only decompresses ~120 timesteps, not the full axis.
        T_CHUNK = min(n_time, 120)
        if extra_dim:
            dim_order = ("time", extra_dim, "y", "x")
            chunk = (T_CHUNK, extra_size, 1, global_X)
        else:
            dim_order = ("time", "y", "x")
            chunk = (T_CHUNK, 1, global_X)

        data_var = dst_ds.createVariable(
            data_var_name,
            dtype,
            dim_order,
            fill_value=fill_value,
            chunksizes=chunk,
            zlib=True,
            complevel=4,
        )
        for attr, val in var_attrs.items():
            setattr(data_var, attr, val)

        # Pre-fill with FillValue (netCDF4 does this automatically with fill_value kwarg,
        # but explicit init ensures correct values on partial grids)
        # (skip – fill_value= in createVariable handles it)

        # --- fill canvas batch by batch ---
        print(f"  Filling  canvas for {var_name}")
        for batch, info in zip(batches, batch_infos):
            src_file = batch / "output" / var_name
            if not src_file.exists():
                print(f"    [WARN] Missing {batch.name}/output/{var_name} – leaving FillValue")
                continue

            with nc.Dataset(src_file) as src_ds:
                # batch_info uses inclusive [min, max] — convert to Python exclusive slice
                min_y, max_y = info["min_y"], info["max_y"] + 1
                min_x, max_x = info["min_x"], info["max_x"] + 1

                if time_chunk:
                    # Read and write in time slices to cap RAM further
                    for t_start in range(0, n_time, time_chunk):
                        t_end = min(t_start + time_chunk, n_time)
                        if extra_dim:
                            chunk_data = src_ds.variables[data_var_name][t_start:t_end, :, :, :]
                            data_var[t_start:t_end, :, min_y:max_y, min_x:max_x] = chunk_data
                        else:
                            chunk_data = src_ds.variables[data_var_name][t_start:t_end, :, :]
                            data_var[t_start:t_end, min_y:max_y, min_x:max_x] = chunk_data
                else:
                    # Read the whole batch at once (fastest; fits in RAM)
                    if extra_dim:
                        data_var[:, :, min_y:max_y, min_x:max_x] = \
                            src_ds.variables[data_var_name][:]
                    else:
                        data_var[:, min_y:max_y, min_x:max_x] = \
                            src_ds.variables[data_var_name][:]

    print(f"  Saved    {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-b", "--batches",
        required=True,
        help="Path to the split directory containing batch_* subdirectories.",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=None,
        help="Directory to write merged files into. "
             "Defaults to <split_dir>/all_merged/.",
    )
    parser.add_argument(
        "--pattern",
        default="*.nc",
        help="Glob pattern for output files to merge (default: '*.nc'). "
             "Example: '*_tr.nc' to merge only transient outputs.",
    )
    parser.add_argument(
        "--time-chunk",
        type=int,
        default=None,
        metavar="N",
        help="Read/write N timesteps at a time per batch. "
             "Use when even a single batch variable is too large for RAM. "
             "Omitting this reads each batch file in one shot (fastest).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip variables whose merged output file already exists.",
    )
    args = parser.parse_args()

    split_dir = Path(args.batches).resolve()
    if not split_dir.exists():
        print(f"[ERROR] Split directory not found: {split_dir}")
        sys.exit(1)

    output_dir = Path(args.output_dir).resolve() if args.output_dir else split_dir / "all_merged"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect batches and their spatial info
    batches = collect_batches(split_dir)
    print(f"Found {len(batches)} batches in {split_dir}")

    batch_infos = [load_batch_info(b) for b in batches]
    global_Y = batch_infos[0]["global_Y"]
    global_X = batch_infos[0]["global_X"]
    print(f"Global canvas: Y={global_Y}, X={global_X}")

    var_files = get_variable_files(batches, args.pattern)
    print(f"Variables to merge ({len(var_files)}): {', '.join(var_files)}\n")

    for var_name in var_files:
        out_path = output_dir / var_name
        if args.skip_existing and out_path.exists():
            print(f"  [SKIP] {var_name} already exists at {out_path}")
            continue
        try:
            merge_variable(
                var_name=var_name,
                batches=batches,
                batch_infos=batch_infos,
                global_Y=global_Y,
                global_X=global_X,
                output_dir=output_dir,
                time_chunk=args.time_chunk,
            )
        except Exception as exc:
            print(f"  [ERROR] Failed to merge {var_name}: {exc}")
            import traceback
            traceback.print_exc()

    print(f"\n=== Merge complete. Output: {output_dir} ===")


if __name__ == "__main__":
    main()
