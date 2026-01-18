#!/usr/bin/env python3
"""
Extract the last N time steps from a NetCDF file.
Example:
python visualization/extract_last_times.py ../merge/Circumpolar/NEE_ssp5_8_5_mri_esm2_0_sc_yearly.nc -n 21 -o ../merge/Circumpolar/NEE_ssp5_8_5_mri_esm2_0_sc_yearly_last21.nc
"""

from __future__ import annotations

import argparse
from pathlib import Path

import xarray as xr


def build_default_output_path(input_path: Path, n: int) -> Path:
    if input_path.suffix:
        return input_path.with_name(f"{input_path.stem}_last{n}{input_path.suffix}")
    return input_path.with_name(f"{input_path.name}_last{n}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract the last N time steps from a NetCDF file."
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to the input NetCDF file.",
    )
    parser.add_argument(
        "-n",
        "--num-times",
        type=int,
        default=21,
        help="Number of last time steps to keep (default: 21).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output NetCDF path (default: input name with _lastN suffix).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = args.input
    output_path = args.output or build_default_output_path(input_path, args.num_times)

    if args.num_times <= 0:
        raise SystemExit("num-times must be a positive integer.")

    ds = xr.open_dataset(input_path)
    if "time" not in ds.dims:
        raise SystemExit("Input dataset does not have a 'time' dimension.")

    time_len = ds.sizes.get("time", 0)
    if time_len == 0:
        raise SystemExit("Input dataset has an empty 'time' dimension.")

    keep = min(args.num_times, time_len)
    ds_last = ds.isel(time=slice(-keep, None))
    ds_last.to_netcdf(output_path)

    print(f"Wrote {keep} time steps to {output_path}")


if __name__ == "__main__":
    main()
