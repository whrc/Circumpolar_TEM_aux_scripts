#!/usr/bin/env python3
"""
Memory-efficient merge of dvmdostem batch output NetCDF files.

Why this exists
---------------
``bp batch merge`` / ``bp batch wiemip_merge`` can build large canvases in RAM,
which OOM-kills the process on a memory-limited login node for big variables
(e.g. monthly PFT/layer fields).

Strategy
--------
For each output variable file (e.g. LAI_monthly_tr.nc):
  1. Create the filtered/staging canvas on disk with FillValue.
  2. Loop over every batch and write each tile into its spatial slice
     (peak RAM ≈ one batch, or one ``--time-chunk`` slice).
  3. For WIEMIP splits (default): restore that canvas onto the full
     original grid (same product as ``wiemip_merged/merged_restored``)
     using ``wiemip_split_metadata.json`` + the original run-mask.

Supported layouts
-----------------
Auto-detected from the split directory (first match wins):

1. **WIEMIP / ``bp batch wiemip_split``**
   - Prefer ``batch_layout.json`` at the split root
     ``{"blocks": [[y0, y1, x0, x1], ...], "Y": ..., "X": ...}``
     with half-open block ranges ``[y0:y1, x0:x1)``.
   - Else ``wiemip_split_metadata.json`` (same ``blocks``; canvas from
     filtered extent of those blocks).
   - Full-grid restore (default) needs ``wiemip_split_metadata.json``
     (active bbox, full_rows/full_cols, run-mask path).

2. **Legacy wetlands / tile splits**
   - Per-batch ``batch_*/batch_info.json`` with inclusive extents::
       min_y, max_y, min_x, max_x, global_Y, global_X
   - Already full-grid; restore is skipped.

Usage
-----
  python merge_batches.py -b /path/to/split_dir
  python merge_batches.py -b /path/to/split_dir -o /path/to/output_dir
  python merge_batches.py -b /path/to/split_dir --pattern "*_tr.nc"
  python merge_batches.py -b /path/to/split_dir --time-chunk 200
  python merge_batches.py -b /path/to/split_dir --skip-existing
  python merge_batches.py -b /path/to/split_dir --no-restore
  python merge_batches.py -b /path/to/split_dir --restore-from /path/to/filtered
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from pathlib import Path
from typing import Any

import netCDF4 as nc
import numpy as np

BATCH_DIR_RE = re.compile(r"^batch_(\d+)$")
BATCH_LAYOUT_FILENAME = "batch_layout.json"
WIEMIP_METADATA_FILENAME = "wiemip_split_metadata.json"
SKIP_NAME_TOKENS = ("restart", "run_status")
RUN_MASK_VARIABLE = "run"
DEFAULT_ACTIVE_VALUE = 1


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------

def collect_batches(split_dir: Path) -> list[Path]:
    batches = sorted(
        [
            path
            for path in split_dir.iterdir()
            if path.is_dir() and BATCH_DIR_RE.match(path.name)
        ],
        key=lambda path: int(BATCH_DIR_RE.match(path.name).group(1)),
    )
    if not batches:
        raise RuntimeError(f"No batch_* directories found in {split_dir}")
    return batches


def batch_id_from_path(batch_dir: Path) -> int:
    match = BATCH_DIR_RE.match(batch_dir.name)
    if not match:
        raise ValueError(f"Invalid batch directory name: {batch_dir.name}")
    return int(match.group(1))


def _slice_from_inclusive(info: dict[str, Any]) -> dict[str, int]:
    """Convert inclusive batch_info.json bounds to half-open slices."""
    return {
        "y0": int(info["min_y"]),
        "y1": int(info["max_y"]) + 1,
        "x0": int(info["min_x"]),
        "x1": int(info["max_x"]) + 1,
    }


def _slice_from_block(block: list[int] | tuple[int, ...]) -> dict[str, int]:
    """Convert WIEMIP [y0, y1, x0, x1) block to half-open slices."""
    if len(block) != 4:
        raise ValueError(f"Expected block [y0, y1, x0, x1], got {block!r}")
    y0, y1, x0, x1 = (int(v) for v in block)
    if y1 <= y0 or x1 <= x0:
        raise ValueError(f"Invalid half-open block {block!r}")
    return {"y0": y0, "y1": y1, "x0": x0, "x1": x1}


def load_layout(split_dir: Path, batches: list[Path]) -> tuple[list[dict[str, int]], int, int, str]:
    """
    Return (batch_slices, canvas_Y, canvas_X, layout_source_label).

    ``batch_slices`` is aligned with ``batches`` (same order / length).
    Each slice dict uses half-open keys y0,y1,x0,x1.
    """
    layout_path = split_dir / BATCH_LAYOUT_FILENAME
    metadata_path = split_dir / WIEMIP_METADATA_FILENAME

    # 1) WIEMIP batch_layout.json
    if layout_path.is_file():
        layout = json.loads(layout_path.read_text())
        blocks = layout.get("blocks")
        if not isinstance(blocks, list) or not blocks:
            raise ValueError(f"{layout_path} missing non-empty 'blocks' list")
        canvas_y = int(layout["Y"])
        canvas_x = int(layout["X"])
        slices = _slices_for_batches_from_blocks(batches, blocks)
        return slices, canvas_y, canvas_x, layout_path.name

    # 2) WIEMIP wiemip_split_metadata.json
    if metadata_path.is_file():
        metadata = json.loads(metadata_path.read_text())
        blocks = metadata.get("blocks")
        if not isinstance(blocks, list) or not blocks:
            raise ValueError(f"{metadata_path} missing non-empty 'blocks' list")
        # Filtered staging canvas from the block extents (not full original grid).
        canvas_y = max(int(block[1]) for block in blocks)
        canvas_x = max(int(block[3]) for block in blocks)
        slices = _slices_for_batches_from_blocks(batches, blocks)
        return slices, canvas_y, canvas_x, metadata_path.name

    # 3) Legacy per-batch batch_info.json
    infos: list[dict[str, int]] = []
    for batch_dir in batches:
        info_path = batch_dir / "batch_info.json"
        if not info_path.is_file():
            raise FileNotFoundError(
                "No WIEMIP layout found "
                f"({BATCH_LAYOUT_FILENAME} / {WIEMIP_METADATA_FILENAME}) and "
                f"batch_info.json missing in {batch_dir}. "
                "Use splits from `bp batch wiemip_split` (writes batch_layout.json) "
                "or legacy splits that write batch_*/batch_info.json."
            )
        info = json.loads(info_path.read_text())
        infos.append(_slice_from_inclusive(info))

    canvas_y = int(json.loads((batches[0] / "batch_info.json").read_text())["global_Y"])
    canvas_x = int(json.loads((batches[0] / "batch_info.json").read_text())["global_X"])
    return infos, canvas_y, canvas_x, "batch_*/batch_info.json"


def _slices_for_batches_from_blocks(
    batches: list[Path], blocks: list[list[int]]
) -> list[dict[str, int]]:
    slices: list[dict[str, int]] = []
    for batch_dir in batches:
        batch_id = batch_id_from_path(batch_dir)
        if batch_id >= len(blocks):
            raise IndexError(
                f"{batch_dir.name} index {batch_id} is outside layout blocks "
                f"(len={len(blocks)})"
            )
        slices.append(_slice_from_block(blocks[batch_id]))
    if len(batches) != len(blocks):
        print(
            f"[WARN] Found {len(batches)} batch_* dirs but layout has "
            f"{len(blocks)} blocks; merging only listed batches by batch index."
        )
    return slices


def get_variable_files(batches: list[Path], pattern: str) -> list[str]:
    """Sorted output file names from the first batch that has matches."""
    for batch_dir in batches:
        ref_output = batch_dir / "output"
        if not ref_output.is_dir():
            continue
        files = sorted(
            path.name
            for path in ref_output.glob(pattern)
            if path.is_file()
            and not any(token in path.name for token in SKIP_NAME_TOKENS)
        )
        if files:
            return files
    return []


def get_variable_files_from_dir(directory: Path, pattern: str) -> list[str]:
    files = sorted(
        path.name
        for path in directory.glob(pattern)
        if path.is_file()
        and not any(token in path.name for token in SKIP_NAME_TOKENS)
    )
    return files


def get_extra_dim(src_ds: nc.Dataset) -> tuple[str | None, int | None]:
    """Return (dim_name, size) for pft or layer dimensions, or (None, None)."""
    for dim_name in ("pft", "layer"):
        if dim_name in src_ds.dimensions:
            return dim_name, int(src_ds.dimensions[dim_name].size)
    return None, None


def _pick_data_var_name(src_ds: nc.Dataset) -> str:
    dim_names = set(src_ds.dimensions)
    candidates = [name for name in src_ds.variables if name not in dim_names]
    if not candidates:
        raise ValueError("No data variable found in NetCDF file")
    # Prefer the unique non-coordinate field; if several, take the first.
    return candidates[0]


def _spatial_dim_names(src_ds: nc.Dataset) -> tuple[str, str]:
    dims = set(src_ds.dimensions)
    row = next((name for name in ("y", "Y", "latitude", "lat") if name in dims), None)
    col = next((name for name in ("x", "X", "longitude", "lon") if name in dims), None)
    if row is None or col is None:
        raise ValueError(
            f"Could not detect spatial dims in {tuple(src_ds.dimensions.keys())}"
        )
    return row, col


def load_restore_info(split_dir: Path) -> dict[str, Any] | None:
    """
    Load WIEMIP restore parameters from wiemip_split_metadata.json.

    Returns None when metadata is absent (legacy / non-WIEMIP layouts).
    """
    metadata_path = split_dir / WIEMIP_METADATA_FILENAME
    if not metadata_path.is_file():
        return None

    metadata = json.loads(metadata_path.read_text())
    bbox = metadata.get("active_bbox")
    if not isinstance(bbox, dict):
        raise ValueError(f"{metadata_path} missing 'active_bbox'")

    required = ("row_start", "row_end", "col_start", "col_end")
    missing = [key for key in required if key not in bbox]
    if missing:
        raise ValueError(f"{metadata_path} active_bbox missing keys: {missing}")

    full_rows = int(metadata["full_rows"])
    full_cols = int(metadata["full_cols"])
    row_start = int(bbox["row_start"])
    row_end = int(bbox["row_end"])  # inclusive
    col_start = int(bbox["col_start"])
    col_end = int(bbox["col_end"])  # inclusive
    filt_y = row_end - row_start + 1
    filt_x = col_end - col_start + 1

    original_input = Path(metadata["original_input_path"]).expanduser()
    run_mask_name = str(metadata.get("run_mask_filename", "run-mask.nc"))
    run_mask_path = original_input / run_mask_name
    if not run_mask_path.is_file():
        raise FileNotFoundError(
            f"Run-mask for restore not found: {run_mask_path}. "
            "Check original_input_path / run_mask_filename in "
            f"{metadata_path}."
        )

    active_value = int(metadata.get("active_value", DEFAULT_ACTIVE_VALUE))
    active_mask = _load_active_mask(run_mask_path, active_value)
    if active_mask.shape != (full_rows, full_cols):
        raise ValueError(
            f"Run-mask shape {active_mask.shape} does not match "
            f"full grid ({full_rows}, {full_cols}) from metadata."
        )

    return {
        "full_y": full_rows,
        "full_x": full_cols,
        "y0": row_start,
        "y1": row_end + 1,  # half-open
        "x0": col_start,
        "x1": col_end + 1,
        "filt_y": filt_y,
        "filt_x": filt_x,
        "active_mask": active_mask,
        "run_mask_path": run_mask_path,
        "metadata_path": metadata_path,
    }


def _load_active_mask(run_mask_path: Path, active_value: int) -> np.ndarray:
    with nc.Dataset(run_mask_path) as ds:
        if RUN_MASK_VARIABLE not in ds.variables:
            raise ValueError(
                f"{run_mask_path} has no '{RUN_MASK_VARIABLE}' variable "
                f"(found: {list(ds.variables)})"
            )
        run = np.asarray(ds.variables[RUN_MASK_VARIABLE][:])
    # Collapse any leading singleton dims (e.g. time=1).
    while run.ndim > 2 and run.shape[0] == 1:
        run = run[0]
    if run.ndim != 2:
        raise ValueError(
            f"Expected 2-D run-mask, got shape {run.shape} in {run_mask_path}"
        )
    return np.isfinite(run) & np.isclose(run, active_value)


# ---------------------------------------------------------------------------
# Core merge + restore
# ---------------------------------------------------------------------------

def merge_variable(
    var_name: str,
    batches: list[Path],
    batch_slices: list[dict[str, int]],
    canvas_y: int,
    canvas_x: int,
    output_dir: Path,
    time_chunk: int | None,
) -> Path | None:
    print(f"  Creating canvas for {var_name}")

    src_path = None
    for batch in batches:
        candidate = batch / "output" / var_name
        if candidate.exists():
            src_path = candidate
            break
    if src_path is None:
        print(f"  [SKIP] {var_name} not found in any batch")
        return None

    with nc.Dataset(src_path) as src_ds:
        if "time" not in src_ds.dimensions:
            print(f"  [SKIP] {var_name}: no 'time' dimension")
            return None

        n_time = int(src_ds.dimensions["time"].size)
        time_units = src_ds.variables["time"].units if "time" in src_ds.variables else None
        time_calendar = getattr(src_ds.variables.get("time"), "calendar", "standard")
        time_data = (
            src_ds.variables["time"][:].copy()
            if "time" in src_ds.variables
            else np.arange(n_time)
        )

        extra_dim, extra_size = get_extra_dim(src_ds)
        data_var_name = _pick_data_var_name(src_ds)
        src_var = src_ds.variables[data_var_name]
        fill_value = getattr(src_var, "_FillValue", -9999.0)
        dtype = src_var.dtype
        var_attrs = {
            attr: getattr(src_var, attr)
            for attr in src_var.ncattrs()
            if attr != "_FillValue"
        }

    out_path = output_dir / var_name
    with nc.Dataset(out_path, "w", format="NETCDF4") as dst_ds:
        dst_ds.createDimension("time", n_time)
        dst_ds.createDimension("y", canvas_y)
        dst_ds.createDimension("x", canvas_x)
        if extra_dim:
            dst_ds.createDimension(extra_dim, extra_size)

        time_var = dst_ds.createVariable("time", "f8", ("time",))
        time_var[:] = time_data
        if time_units:
            time_var.units = time_units
        time_var.calendar = time_calendar

        # Chunk for time-slice reads (plotting); keep time chunks modest.
        t_chunk = min(n_time, 120)
        if extra_dim:
            dim_order = ("time", extra_dim, "y", "x")
            chunk = (t_chunk, extra_size, 1, canvas_x)
        else:
            dim_order = ("time", "y", "x")
            chunk = (t_chunk, 1, canvas_x)

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

        print(f"  Filling  canvas for {var_name}")
        for batch, slc in zip(batches, batch_slices):
            src_file = batch / "output" / var_name
            if not src_file.exists():
                print(
                    f"    [WARN] Missing {batch.name}/output/{var_name} – leaving FillValue"
                )
                continue

            y0, y1, x0, x1 = slc["y0"], slc["y1"], slc["x0"], slc["x1"]
            with nc.Dataset(src_file) as src_ds:
                data_var_name_local = _pick_data_var_name(src_ds)
                src_var = src_ds.variables[data_var_name_local]
                row_dim, col_dim = _spatial_dim_names(src_ds)
                expected_y = y1 - y0
                expected_x = x1 - x0
                got_y = int(src_ds.dimensions[row_dim].size)
                got_x = int(src_ds.dimensions[col_dim].size)
                if got_y != expected_y or got_x != expected_x:
                    raise ValueError(
                        f"{batch.name}/{var_name}: spatial shape "
                        f"({got_y}, {got_x}) does not match layout "
                        f"({expected_y}, {expected_x}) for slice "
                        f"y=[{y0}:{y1}), x=[{x0}:{x1})"
                    )

                if time_chunk:
                    for t_start in range(0, n_time, time_chunk):
                        t_end = min(t_start + time_chunk, n_time)
                        if extra_dim:
                            chunk_data = src_var[t_start:t_end, :, :, :]
                            data_var[t_start:t_end, :, y0:y1, x0:x1] = chunk_data
                        else:
                            chunk_data = src_var[t_start:t_end, :, :]
                            data_var[t_start:t_end, y0:y1, x0:x1] = chunk_data
                else:
                    if extra_dim:
                        data_var[:, :, y0:y1, x0:x1] = src_var[:]
                    else:
                        data_var[:, y0:y1, x0:x1] = src_var[:]

    print(f"  Saved    {out_path}")
    return out_path


def restore_variable(
    var_name: str,
    filtered_path: Path,
    restored_dir: Path,
    restore_info: dict[str, Any],
    time_chunk: int | None,
) -> Path | None:
    """
    Place a filtered/staging merge onto the full original grid.

    Equivalent in spirit to ``wiemip_merged/merged_restored`` from
    ``bp batch wiemip_merge`` (without template back-fill).
    """
    if not filtered_path.is_file():
        print(f"  [SKIP] restore {var_name}: filtered file missing ({filtered_path})")
        return None

    full_y = restore_info["full_y"]
    full_x = restore_info["full_x"]
    y0 = restore_info["y0"]
    y1 = restore_info["y1"]
    x0 = restore_info["x0"]
    x1 = restore_info["x1"]
    filt_y = restore_info["filt_y"]
    filt_x = restore_info["filt_x"]
    active_mask = restore_info["active_mask"]  # (full_y, full_x) bool

    print(
        f"  Restoring {var_name} -> full grid "
        f"Y={full_y}, X={full_x} (bbox y=[{y0}:{y1}), x=[{x0}:{x1}))"
    )

    with nc.Dataset(filtered_path) as src_ds:
        if "time" not in src_ds.dimensions:
            print(f"  [SKIP] restore {var_name}: no 'time' dimension")
            return None

        row_dim, col_dim = _spatial_dim_names(src_ds)
        got_y = int(src_ds.dimensions[row_dim].size)
        got_x = int(src_ds.dimensions[col_dim].size)
        if got_y == full_y and got_x == full_x:
            print(
                f"  [SKIP] {var_name} already full-grid "
                f"({got_y}x{got_x}); copying to output"
            )
            out_path = restored_dir / var_name
            if filtered_path.resolve() != out_path.resolve():
                out_path.write_bytes(filtered_path.read_bytes())
            return out_path
        if got_y != filt_y or got_x != filt_x:
            raise ValueError(
                f"{var_name}: filtered shape ({got_y}, {got_x}) does not match "
                f"active bbox ({filt_y}, {filt_x}) from {WIEMIP_METADATA_FILENAME}"
            )

        n_time = int(src_ds.dimensions["time"].size)
        time_units = src_ds.variables["time"].units if "time" in src_ds.variables else None
        time_calendar = getattr(src_ds.variables.get("time"), "calendar", "standard")
        time_data = (
            src_ds.variables["time"][:].copy()
            if "time" in src_ds.variables
            else np.arange(n_time)
        )
        extra_dim, extra_size = get_extra_dim(src_ds)
        data_var_name = _pick_data_var_name(src_ds)
        src_var = src_ds.variables[data_var_name]
        fill_value = getattr(src_var, "_FillValue", -9999.0)
        dtype = src_var.dtype
        var_attrs = {
            attr: getattr(src_var, attr)
            for attr in src_var.ncattrs()
            if attr != "_FillValue"
        }

        out_path = restored_dir / var_name
        with nc.Dataset(out_path, "w", format="NETCDF4") as dst_ds:
            dst_ds.createDimension("time", n_time)
            dst_ds.createDimension("y", full_y)
            dst_ds.createDimension("x", full_x)
            if extra_dim:
                dst_ds.createDimension(extra_dim, extra_size)

            time_var = dst_ds.createVariable("time", "f8", ("time",))
            time_var[:] = time_data
            if time_units:
                time_var.units = time_units
            time_var.calendar = time_calendar

            t_chunk = min(n_time, 120)
            if extra_dim:
                dim_order = ("time", extra_dim, "y", "x")
                chunksizes = (t_chunk, extra_size, 1, full_x)
            else:
                dim_order = ("time", "y", "x")
                chunksizes = (t_chunk, 1, full_x)

            data_var = dst_ds.createVariable(
                data_var_name,
                dtype,
                dim_order,
                fill_value=fill_value,
                chunksizes=chunksizes,
                zlib=True,
                complevel=4,
            )
            for attr, val in var_attrs.items():
                setattr(data_var, attr, val)
            data_var.restored_from_filtered = "true"

            bbox_mask = active_mask[y0:y1, x0:x1]
            step = time_chunk if time_chunk else n_time
            for t_start in range(0, n_time, step):
                t_end = min(t_start + step, n_time)
                if extra_dim:
                    slab = np.asarray(src_var[t_start:t_end, :, :, :])
                    # Broadcast mask over (time, extra, y, x)
                    masked = np.where(bbox_mask[None, None, :, :], slab, fill_value)
                    data_var[t_start:t_end, :, y0:y1, x0:x1] = masked
                else:
                    slab = np.asarray(src_var[t_start:t_end, :, :])
                    masked = np.where(bbox_mask[None, :, :], slab, fill_value)
                    data_var[t_start:t_end, y0:y1, x0:x1] = masked

    print(f"  Saved    {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-b",
        "--batches",
        required=True,
        help="Path to the split directory containing batch_* subdirectories.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Directory for final merged files. "
        "Defaults to <split_dir>/all_merged/ "
        "(full-grid restored for WIEMIP; filtered for --no-restore / legacy).",
    )
    parser.add_argument(
        "--filtered-dir",
        default=None,
        help="Directory for WIEMIP filtered/staging merge "
        "(default: <split_dir>/all_merged_filtered/). "
        "Ignored with --no-restore or --restore-from.",
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
        help="Read/write N timesteps at a time. "
        "Use when even a single batch/variable is too large for RAM. "
        "Omitting this reads each batch file in one shot (fastest).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip variables whose final output file already exists.",
    )
    parser.add_argument(
        "--no-restore",
        action="store_true",
        help="Write only the filtered/staging canvas (like "
        "wiemip_merged/merged_filtered). Default for WIEMIP is to restore "
        "to the full original grid (like merged_restored).",
    )
    parser.add_argument(
        "--restore-from",
        default=None,
        metavar="DIR",
        help="Skip batch merge; restore existing filtered NetCDF files in DIR "
        "to the full original grid (needs wiemip_split_metadata.json).",
    )
    args = parser.parse_args()

    split_dir = Path(args.batches).expanduser().resolve()
    if not split_dir.exists():
        print(f"[ERROR] Split directory not found: {split_dir}")
        sys.exit(1)

    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else split_dir / "all_merged"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- restore-only path -------------------------------------------------
    if args.restore_from is not None:
        filtered_dir = Path(args.restore_from).expanduser().resolve()
        if not filtered_dir.is_dir():
            print(f"[ERROR] --restore-from directory not found: {filtered_dir}")
            sys.exit(1)
        try:
            restore_info = load_restore_info(split_dir)
        except (FileNotFoundError, ValueError, KeyError, json.JSONDecodeError) as exc:
            print(f"[ERROR] Cannot restore: {exc}")
            sys.exit(1)
        if restore_info is None:
            print(
                f"[ERROR] {WIEMIP_METADATA_FILENAME} not found in {split_dir}; "
                "cannot restore to full grid."
            )
            sys.exit(1)

        var_files = get_variable_files_from_dir(filtered_dir, args.pattern)
        if not var_files:
            print(f"[ERROR] No files matching {args.pattern!r} in {filtered_dir}")
            sys.exit(1)

        print(f"Restore-only from: {filtered_dir}")
        print(
            f"Full grid: Y={restore_info['full_y']}, X={restore_info['full_x']} "
            f"(from {restore_info['metadata_path'].name})"
        )
        print(f"Run-mask: {restore_info['run_mask_path']}")
        print(f"Variables ({len(var_files)}): {', '.join(var_files)}\n")

        for var_name in var_files:
            out_path = output_dir / var_name
            if args.skip_existing and out_path.exists():
                print(f"  [SKIP] {var_name} already exists at {out_path}")
                continue
            try:
                restore_variable(
                    var_name=var_name,
                    filtered_path=filtered_dir / var_name,
                    restored_dir=output_dir,
                    restore_info=restore_info,
                    time_chunk=args.time_chunk,
                )
            except Exception as exc:
                print(f"  [ERROR] Failed to restore {var_name}: {exc}")
                traceback.print_exc()

        print(f"\n=== Restore complete. Full-grid output: {output_dir} ===")
        return

    # --- merge (+ optional restore) ----------------------------------------
    batches = collect_batches(split_dir)
    print(f"Found {len(batches)} batches in {split_dir}")

    try:
        batch_slices, canvas_y, canvas_x, layout_label = load_layout(split_dir, batches)
    except (FileNotFoundError, ValueError, IndexError, KeyError, json.JSONDecodeError) as exc:
        print(f"[ERROR] Failed to load batch layout: {exc}")
        sys.exit(1)

    print(f"Layout source: {layout_label}")
    print(f"Filtered canvas: Y={canvas_y}, X={canvas_x}")

    restore_info: dict[str, Any] | None = None
    do_restore = not args.no_restore
    if do_restore:
        try:
            restore_info = load_restore_info(split_dir)
        except (FileNotFoundError, ValueError, KeyError, json.JSONDecodeError) as exc:
            print(f"[ERROR] Restore requested but failed to load metadata: {exc}")
            sys.exit(1)
        if restore_info is None:
            if layout_label == "batch_*/batch_info.json":
                print(
                    "[INFO] Legacy batch_info layout is already full-grid; "
                    "skipping restore."
                )
                do_restore = False
            else:
                print(
                    f"[ERROR] WIEMIP restore needs {WIEMIP_METADATA_FILENAME} in "
                    f"{split_dir}. Use --no-restore for filtered-only output, or "
                    "re-run split with a bp version that writes this file."
                )
                sys.exit(1)
        else:
            print(
                f"Restore: ON -> full grid Y={restore_info['full_y']}, "
                f"X={restore_info['full_x']} "
                f"(bbox y=[{restore_info['y0']}:{restore_info['y1']}), "
                f"x=[{restore_info['x0']}:{restore_info['x1']}))"
            )
            print(f"Run-mask: {restore_info['run_mask_path']}")
    else:
        print("Restore: OFF (--no-restore); writing filtered/staging canvas only")

    if do_restore:
        filtered_dir = (
            Path(args.filtered_dir).expanduser().resolve()
            if args.filtered_dir
            else split_dir / "all_merged_filtered"
        )
        filtered_dir.mkdir(parents=True, exist_ok=True)
        print(f"Filtered staging dir: {filtered_dir}")
        print(f"Full-grid output dir: {output_dir}")
    else:
        filtered_dir = output_dir
        print(f"Output dir: {output_dir}")

    var_files = get_variable_files(batches, args.pattern)
    if not var_files:
        print(f"[ERROR] No output files matching pattern {args.pattern!r}")
        sys.exit(1)
    print(f"Variables to merge ({len(var_files)}): {', '.join(var_files)}\n")

    for var_name in var_files:
        final_path = output_dir / var_name
        if args.skip_existing and final_path.exists():
            print(f"  [SKIP] {var_name} already exists at {final_path}")
            continue
        try:
            filtered_path = merge_variable(
                var_name=var_name,
                batches=batches,
                batch_slices=batch_slices,
                canvas_y=canvas_y,
                canvas_x=canvas_x,
                output_dir=filtered_dir,
                time_chunk=args.time_chunk,
            )
            if filtered_path is None:
                continue
            if do_restore and restore_info is not None:
                restore_variable(
                    var_name=var_name,
                    filtered_path=filtered_path,
                    restored_dir=output_dir,
                    restore_info=restore_info,
                    time_chunk=args.time_chunk,
                )
        except Exception as exc:
            print(f"  [ERROR] Failed on {var_name}: {exc}")
            traceback.print_exc()

    print(f"\n=== Merge complete. Output: {output_dir} ===")
    if do_restore:
        print(
            "WIEMIP full-grid restore applied "
            f"(like wiemip_merged/merged_restored). "
            f"Filtered staging kept in {filtered_dir}."
        )
    else:
        print(
            "Filtered/staging canvas only "
            "(like wiemip_merged/merged_filtered). "
            "Omit --no-restore for full-grid restore when metadata is present."
        )


if __name__ == "__main__":
    main()
