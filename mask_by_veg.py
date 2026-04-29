#!/usr/bin/env python3
"""
mask_by_veg.py

Zero out values of the 'run' variable in a run-mask NetCDF file wherever
the 'veg_class' variable in a vegetation NetCDF file is NOT in a specified
list of allowed vegetation class values.

Usage
-----
    python mask_by_veg.py \\
        --run-mask   run-mask.nc \\
        --vegetation vegetation.nc \\
        --output     run-mask-filtered.nc \\
        --veg-values 1 2 3 5 7

Arguments
---------
  --run-mask    Path to the run-mask NetCDF file  (NetCDF4/HDF5 format).
  --vegetation  Path to the vegetation NetCDF file (NetCDF3 or NetCDF4).
  --output      Path for the output run-mask file (same format as input).
  --veg-values  One or more integer vegetation class values to KEEP active.
                Pixels whose veg_class is NOT in this list will have
                their 'run' value set to 0.
  --run-var     (optional) Name of the run variable. Default: 'run'
  --veg-var     (optional) Name of the vegetation class variable.
                Default: 'veg_class'
  --dry-run     (optional) Print stats without writing output.

Dependencies
------------
  netCDF4   (preferred, handles both NetCDF3 and NetCDF4/HDF5):
      pip install netCDF4

  Fallback: scipy + h5py (if netCDF4 is unavailable):
      pip install scipy h5py

Examples
--------
  # Keep only veg classes 1, 2, and 7
  python mask_by_veg.py \\
      --run-mask run-mask.nc \\
      --vegetation vegetation.nc \\
      --output run-mask-filtered.nc \\
      --veg-values 1 2 7

  # Preview without writing
  python mask_by_veg.py \\
      --run-mask run-mask.nc \\
      --vegetation vegetation.nc \\
      --output run-mask-filtered.nc \\
      --veg-values 1 2 7 \\
      --dry-run
"""

import argparse
import sys
import shutil
import numpy as np


# ---------------------------------------------------------------------------
# Backend helpers
# ---------------------------------------------------------------------------

def _try_import_netcdf4():
    try:
        import netCDF4  # noqa: F401
        return True
    except ImportError:
        return False


def read_variable_netcdf4(path, varname):
    """Read a variable array using the netCDF4 library."""
    import netCDF4 as nc
    with nc.Dataset(path, "r") as ds:
        if varname not in ds.variables:
            raise KeyError(
                f"Variable '{varname}' not found in '{path}'.\n"
                f"Available variables: {list(ds.variables.keys())}"
            )
        data = ds.variables[varname][:]  # returns a masked array
    if not hasattr(data, "filled"):
        return data
    # Use the array's own fill value so we stay within the dtype's range.
    # For integer arrays np.nan is invalid; the existing fill_value is safe.
    return np.ma.filled(data, fill_value=data.fill_value)


def copy_and_modify_netcdf4(src_path, dst_path, run_varname, mask):
    """
    Copy src_path → dst_path, then zero out run_varname wherever mask is True.
    Uses netCDF4 library.
    """
    import netCDF4 as nc
    import os

    shutil.copy2(src_path, dst_path)

    with nc.Dataset(dst_path, "r+") as ds:
        if run_varname not in ds.variables:
            raise KeyError(
                f"Variable '{run_varname}' not found in '{src_path}'.\n"
                f"Available variables: {list(ds.variables.keys())}"
            )
        var = ds.variables[run_varname]
        data = var[:]
        data[mask] = 0
        var[:] = data


# -- Fallback: scipy (NetCDF3) + h5py (HDF5/NetCDF4) -----------------------

def read_variable_scipy(path, varname):
    """Read a variable from a NetCDF3 file using scipy."""
    from scipy.io import netcdf_file
    with netcdf_file(path, "r", mmap=False) as ds:
        if varname not in ds.variables:
            raise KeyError(
                f"Variable '{varname}' not found in '{path}'.\n"
                f"Available variables: {list(ds.variables.keys())}"
            )
        return ds.variables[varname][:].copy()


def read_variable_h5py(path, varname):
    """Read a variable from a NetCDF4/HDF5 file using h5py."""
    import h5py
    with h5py.File(path, "r") as f:
        if varname not in f:
            raise KeyError(
                f"Variable '{varname}' not found in '{path}'.\n"
                f"Available keys: {list(f.keys())}"
            )
        return f[varname][:]


def copy_and_modify_h5py(src_path, dst_path, run_varname, mask):
    """Copy src → dst and zero out run_varname where mask is True using h5py."""
    import h5py
    shutil.copy2(src_path, dst_path)
    with h5py.File(dst_path, "r+") as f:
        if run_varname not in f:
            raise KeyError(
                f"Variable '{run_varname}' not found in '{src_path}'.\n"
                f"Available keys: {list(f.keys())}"
            )
        data = f[run_varname][:]
        data[mask] = 0
        f[run_varname][...] = data


def _is_hdf5(path):
    """Return True if the file has the HDF5 magic bytes."""
    with open(path, "rb") as fh:
        return fh.read(4) == b"\x89HDF"


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Zero out 'run' values in a run-mask NetCDF file wherever "
            "'veg_class' in a vegetation NetCDF file is not in the "
            "specified set of allowed values."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--run-mask", required=True, metavar="FILE",
        help="Path to the run-mask NetCDF file.",
    )
    parser.add_argument(
        "--vegetation", required=True, metavar="FILE",
        help="Path to the vegetation NetCDF file.",
    )
    parser.add_argument(
        "--output", required=True, metavar="FILE",
        help="Path for the output (modified run-mask) NetCDF file.",
    )
    parser.add_argument(
        "--veg-values", required=True, nargs="+", type=int, metavar="INT",
        help=(
            "Allowed vegetation class values. Pixels whose veg_class is "
            "NOT in this list will have their run value set to 0."
        ),
    )
    parser.add_argument(
        "--run-var", default="run", metavar="NAME",
        help="Name of the run variable in the run-mask file. Default: 'run'",
    )
    parser.add_argument(
        "--veg-var", default="veg_class", metavar="NAME",
        help=(
            "Name of the vegetation class variable in the vegetation file. "
            "Default: 'veg_class'"
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print statistics without writing an output file.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    have_netcdf4 = _try_import_netcdf4()

    print(f"Backend : {'netCDF4' if have_netcdf4 else 'scipy + h5py (fallback)'}")
    print(f"Run mask: {args.run_mask}")
    print(f"Veg file: {args.vegetation}")
    print(f"Output  : {args.output}")
    print(f"Allowed veg classes: {sorted(args.veg_values)}")
    print()

    # --- Read vegetation class array ---
    print(f"Reading '{args.veg_var}' from vegetation file …")
    try:
        if have_netcdf4:
            veg = read_variable_netcdf4(args.vegetation, args.veg_var)
        elif _is_hdf5(args.vegetation):
            veg = read_variable_h5py(args.vegetation, args.veg_var)
        else:
            veg = read_variable_scipy(args.vegetation, args.veg_var)
    except Exception as exc:
        sys.exit(f"ERROR reading vegetation file: {exc}")

    print(f"  shape : {veg.shape}")
    print(f"  unique veg classes found: {sorted(np.unique(veg).tolist())}")

    # --- Read run array (for stats only) ---
    print(f"\nReading '{args.run_var}' from run-mask file …")
    try:
        if have_netcdf4:
            run = read_variable_netcdf4(args.run_mask, args.run_var)
        elif _is_hdf5(args.run_mask):
            run = read_variable_h5py(args.run_mask, args.run_var)
        else:
            run = read_variable_scipy(args.run_mask, args.run_var)
    except Exception as exc:
        sys.exit(f"ERROR reading run-mask file: {exc}")

    print(f"  shape : {run.shape}")

    # --- Shape check ---
    if veg.shape != run.shape:
        # Try to broadcast / squeeze trivial dimensions
        veg_s = veg.squeeze()
        run_s = run.squeeze()
        if veg_s.shape != run_s.shape:
            sys.exit(
                f"ERROR: Shape mismatch after squeezing:\n"
                f"  veg_class shape : {veg.shape}  →  {veg_s.shape}\n"
                f"  run shape       : {run.shape}  →  {run_s.shape}\n"
                "The two grids must have the same spatial dimensions."
            )
        print(
            f"  Note: shapes differ ({veg.shape} vs {run.shape}) "
            "but match after squeezing singleton dimensions — proceeding."
        )
        veg = veg_s
        run = run_s

    # --- Build mask: True where veg_class is NOT in allowed set ---
    allowed = np.array(args.veg_values, dtype=veg.dtype)
    exclude_mask = ~np.isin(veg, allowed)

    n_total   = exclude_mask.size
    n_zeroed  = int(exclude_mask.sum())
    n_kept    = n_total - n_zeroed
    pct       = 100.0 * n_zeroed / n_total if n_total else 0.0

    print(f"\nMask statistics:")
    print(f"  Total pixels   : {n_total:,}")
    print(f"  Pixels kept    : {n_kept:,}  (veg_class in allowed set)")
    print(f"  Pixels zeroed  : {n_zeroed:,}  ({pct:.1f}%)")

    if args.dry_run:
        print("\n[Dry run] No output written.")
        return

    # --- Write output ---
    print(f"\nWriting output to '{args.output}' …")
    try:
        if have_netcdf4:
            copy_and_modify_netcdf4(args.run_mask, args.output, args.run_var, exclude_mask)
        elif _is_hdf5(args.run_mask):
            copy_and_modify_h5py(args.run_mask, args.output, args.run_var, exclude_mask)
        else:
            # NetCDF3 via scipy — scipy doesn't support in-place editing,
            # so we read everything and rewrite.
            _copy_and_modify_scipy(args.run_mask, args.output, args.run_var, exclude_mask)
    except Exception as exc:
        sys.exit(f"ERROR writing output: {exc}")

    print("Done.")


def _copy_and_modify_scipy(src_path, dst_path, run_varname, mask):
    """
    Full copy + modify for NetCDF3 using scipy (which has no in-place edit).
    All variables, dimensions, and attributes are preserved.
    """
    from scipy.io import netcdf_file

    with netcdf_file(src_path, "r", mmap=False) as src:
        with netcdf_file(dst_path, "w") as dst:
            # Copy global attributes
            for attr in src._attributes:
                setattr(dst, attr, getattr(src, attr))

            # Copy dimensions
            for dim, size in src.dimensions.items():
                dst.createDimension(dim, size)

            # Copy variables
            for vname, vobj in src.variables.items():
                data = vobj[:].copy()
                if vname == run_varname:
                    data[mask] = 0
                new_var = dst.createVariable(vname, vobj.typecode(), vobj.dimensions)
                new_var[:] = data
                for attr in vobj._attributes:
                    setattr(new_var, attr, getattr(vobj, attr))


if __name__ == "__main__":
    main()
