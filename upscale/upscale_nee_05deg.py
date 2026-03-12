#!/usr/bin/env python3
"""
Upscale yearly monthly NEE files from ~4x4 km to 0.5 degree.

This is a Python equivalent of `merge/upscale_05deg.sh`, preserving the same
workflow and command-line tools (NCO + GDAL) while adding robust logging.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Sequence


NUM_RE = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eEdD][-+]?\d+)?"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def run_cmd(
    cmd: Sequence[str],
    *,
    capture: bool = False,
    check: bool = True,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    log("$ " + " ".join(cmd))
    proc = subprocess.run(
        list(cmd),
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=capture,
        check=False,
    )
    if check and proc.returncode != 0:
        if capture and proc.stdout:
            log("stdout:")
            for line in proc.stdout.splitlines():
                log(f"  {line}")
        if proc.stderr:
            log("stderr:")
            for line in proc.stderr.splitlines():
                log(f"  {line}")
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc


def detect_coord_var(grid_file: Path, candidates: Sequence[str]) -> str | None:
    for name in candidates:
        proc = run_cmd(["ncks", "-m", "-v", name, str(grid_file)], check=False, capture=True)
        if proc.returncode == 0:
            return name
    return None


def parse_coord_stats_from_ncdump(grid_file: Path, var_name: str) -> tuple[float, float, float, int]:
    proc = run_cmd(["ncdump", "-v", var_name, str(grid_file)], capture=True)
    txt = proc.stdout

    data_part = txt.split("data:", 1)[1] if "data:" in txt else txt
    m = re.search(rf"^\s*{re.escape(var_name)}\s*=\s*(.*?);", data_part, flags=re.M | re.S)
    if not m:
        raise RuntimeError(f"Could not find data section for coordinate variable '{var_name}'")

    values_txt = m.group(1).replace("D", "E").replace("d", "e")
    nums = re.findall(NUM_RE, values_txt)
    if len(nums) < 2:
        raise RuntimeError(f"Not enough coordinate values for '{var_name}'")

    vals = [float(v) for v in nums]
    return vals[0], vals[1], vals[-1], len(vals)


def _is_geographic_extent(extent: list[float]) -> bool:
    """Return True when extent values are in degree range (geographic CRS like EPSG:4326)."""
    return all(abs(v) <= 360 for v in extent)


def parse_grid_metadata(grid_file: Path) -> tuple[list[float], list[float], str]:
    """Return (grid_extent, grid_res, te_srs).

    te_srs is the EPSG code string to use for the gdalwarp -te_srs argument.
    It is derived by examining whether the extent values are in geographic
    (degree-range) or projected (meter-range) coordinates.
    """
    log("Reading grid metadata with gdalinfo")
    gdalinfo_proc = run_cmd(["gdalinfo", str(grid_file)], capture=True, check=False)
    info = gdalinfo_proc.stdout if gdalinfo_proc.returncode == 0 else ""
    if gdalinfo_proc.returncode != 0:
        # Some installations miss HDF5 plugin support for gdalinfo; fallback works via ncdump.
        log("gdalinfo could not read grid file; falling back to NetCDF coordinate metadata.")
        if gdalinfo_proc.stderr:
            for line in gdalinfo_proc.stderr.splitlines():
                log(f"  {line}")

    origin_line = re.search(r"^Origin\s*=\s*\(([^,]+),\s*([^)]+)\)", info, flags=re.M)
    size_line = re.search(r"^Size is\s+([0-9]+),\s*([0-9]+)", info, flags=re.M)
    pixel_line = re.search(r"^Pixel Size\s*=\s*\(([^,]+),\s*([^)]+)\)", info, flags=re.M)

    if origin_line and size_line and pixel_line:
        ox = float(origin_line.group(1))
        oy = float(origin_line.group(2))
        sx = int(size_line.group(1))
        sy = int(size_line.group(2))
        rx = float(pixel_line.group(1))
        ry = float(pixel_line.group(2))

        left = ox
        top = oy
        right = ox + rx * sx
        bottom = oy + ry * sy
        grid_res = [abs(rx), abs(ry)]
        grid_extent = [left, bottom, right, top]

        te_srs = "EPSG:4326" if _is_geographic_extent(grid_extent) else "EPSG:6931"
        log("Grid metadata source: gdalinfo geotransform")
        log(f"Grid extent: {grid_extent}")
        log(f"Grid resolution: {grid_res}")
        log(f"Detected te_srs: {te_srs}")
        return grid_extent, grid_res, te_srs

    log("Origin/Pixel Size missing in gdalinfo. Falling back to NetCDF coordinate vars.")
    # Try projected coordinate names first, then geographic.
    x_var = detect_coord_var(grid_file, ["x", "X", "longitude", "lon"])
    y_var = detect_coord_var(grid_file, ["y", "Y", "latitude", "lat"])
    if not x_var or not y_var:
        raise RuntimeError("Could not detect coordinate variables in grid file.")

    log(f"Using coordinate variables: x='{x_var}', y='{y_var}'")
    x_first, x_second, x_last, x_count = parse_coord_stats_from_ncdump(grid_file, x_var)
    y_first, y_second, y_last, y_count = parse_coord_stats_from_ncdump(grid_file, y_var)

    res_x = abs(x_second - x_first)
    res_y = abs(y_second - y_first)
    x_min, x_max = min(x_first, x_last), max(x_first, x_last)
    y_min, y_max = min(y_first, y_last), max(y_first, y_last)

    left = x_min - res_x / 2.0
    right = x_max + res_x / 2.0
    bottom = y_min - res_y / 2.0
    top = y_max + res_y / 2.0

    grid_extent = [left, bottom, right, top]
    grid_res = [res_x, res_y]
    te_srs = "EPSG:4326" if _is_geographic_extent(grid_extent) else "EPSG:6931"

    log(f"x stats (first,second,last,count): {x_first}, {x_second}, {x_last}, {x_count}")
    log(f"y stats (first,second,last,count): {y_first}, {y_second}, {y_last}, {y_count}")
    log(f"Grid extent: {grid_extent}")
    log(f"Grid resolution: {grid_res}")
    log(f"Detected te_srs: {te_srs}")
    return grid_extent, grid_res, te_srs


def parse_time_length(netcdf_path: Path) -> int:
    out = run_cmd(["ncks", "-m", "-v", "time", str(netcdf_path)], capture=True).stdout
    m = re.search(r"\btime\s*=\s*([0-9]+)\s*;", out)
    if not m:
        raise RuntimeError(f"Could not parse time dimension from {netcdf_path}")
    return int(m.group(1))


def parse_time_value(netcdf_path: Path, month_idx0: int) -> str:
    out = run_cmd(
        ["ncks", "-H", "-C", "-v", "time", "-d", f"time,{month_idx0}", str(netcdf_path)],
        capture=True,
    ).stdout
    m = re.search(r"=\s*(" + NUM_RE + r")\s*;?", out.replace("D", "E").replace("d", "e"))
    if m:
        return m.group(1)
    nums = re.findall(NUM_RE, out)
    if nums:
        return nums[-1]
    raise RuntimeError(f"Could not parse time value for month index {month_idx0}")


def require_tools(tools: Sequence[str]) -> None:
    for tool in tools:
        if shutil.which(tool) is None:
            raise RuntimeError(f"Missing required command: {tool}")
    # Catch broken dynamic-linker setups early (common on local macOS Homebrew upgrades).
    health = run_cmd(["ncrename", "--version"], check=False, capture=True)
    if health.returncode != 0:
        details = (health.stderr or health.stdout or "").strip()
        msg = "ncrename is installed but not runnable."
        if details:
            msg += f"\nCommand output:\n{details}"
        msg += (
            "\nHint: this is usually a broken NCO/NetCDF library linkage. "
            "Use a consistent environment (e.g., conda-forge nco/gdal/netcdf) "
            "or reinstall NCO with matching NetCDF libraries."
        )
        raise RuntimeError(msg)


def build_parser() -> argparse.ArgumentParser:
    default_in_dir = Path.home() / "Circumpolar_TEM_aux_scripts" / "merge" / "Circumpolar" / "merged"
    parser = argparse.ArgumentParser(description="Upscale yearly NEE monthly files to 0.5 degree")
    parser.add_argument("--in-dir", type=Path, default=default_in_dir, help="Input directory with NEE files")
    parser.add_argument(
        "--grid",
        type=Path,
        default=None,
        help="Reference grid NetCDF. Default: <in-dir>/RECO_ssp1_2_6_mri_esm2_0_tr_monthly_1901.nc",
    )
    parser.add_argument(
        "--mask",
        type=Path,
        default=Path.home() / "Circumpolar_TEM_aux_scripts" / "merge" / "aoi_5k_buff_6931.tiff",
        help="Mask file in EPSG:6931",
    )
    parser.add_argument("--var", default="NEE", help="Variable name to upscale")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output directory (default: in-dir)")
    parser.add_argument("--pattern", default="NEE_*_monthly_[0-9][0-9][0-9][0-9].nc", help="Input file pattern")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    in_dir = args.in_dir.expanduser().resolve()
    out_dir = (args.out_dir or in_dir).expanduser().resolve()
    grid = (args.grid or (in_dir / "RECO_ssp1_2_6_mri_esm2_0_tr_monthly_1901.nc")).expanduser().resolve()
    mask = args.mask.expanduser().resolve()
    var = args.var

    log("Starting NEE upscaling script (Python)")
    log(f"Input directory: {in_dir}")
    log(f"Reference grid: {grid}")
    log(f"Mask file: {mask}")
    log(f"Output directory: {out_dir}")
    log(f"Variable: {var}")

    require_tools(["ncks", "ncrename", "ncap2", "ncatted", "ncrcat", "gdalinfo", "gdalwarp", "ncdump"])

    if not in_dir.is_dir():
        raise RuntimeError(f"Input directory does not exist: {in_dir}")
    if not grid.is_file():
        raise RuntimeError(f"Reference grid not found: {grid}")
    if not mask.is_file():
        raise RuntimeError(f"Mask file not found: {mask}")
    out_dir.mkdir(parents=True, exist_ok=True)

    grid_extent, grid_res, te_srs = parse_grid_metadata(grid)

    nee_files = sorted(in_dir.glob(args.pattern))
    if not nee_files:
        raise RuntimeError(f"No yearly NEE monthly files found in: {in_dir}")
    log(f"Found {len(nee_files)} yearly files")

    with tempfile.TemporaryDirectory(prefix="tmp_upscale_nee_", dir=out_dir) as td:
        tmp_dir = Path(td)
        log(f"Temporary directory: {tmp_dir}")

        for idx, in_file in enumerate(nee_files, start=1):
            base_name = in_file.stem
            year = base_name.split("_")[-1]
            out_file = out_dir / f"{base_name}_upscaled.nc"

            log("==================================================")
            log(f"File {idx}/{len(nee_files)}: {in_file.name}")
            log(f"Year: {year}")

            nee_rename = tmp_dir / "nee_rename.nc"
            mask_crop = tmp_dir / "mask_crop.nc"
            tem_nee = tmp_dir / "tem_nee.nc"
            slice_nc = tmp_dir / "slice.nc"
            upscaled_nc = tmp_dir / "upscaled.nc"
            concat_nc = tmp_dir / "concat.nc"

            run_cmd(["ncrename", "-O", "-h", "-d", "x,Xd", "-d", "y,Yd", str(in_file), str(nee_rename)])
            run_cmd(["ncks", "-O", "-h", "-x", "-v", "x,y,X,Y", str(nee_rename), str(nee_rename)])
            run_cmd(["ncrename", "-O", "-h", "-d", "Xd,X", "-d", "Yd,Y", str(nee_rename), str(nee_rename)])

            run_cmd(["ncks", "-O", "-h", "-d", "Y,29,1961", "-d", "X,50,2290", str(mask), str(mask_crop)])
            run_cmd(["ncks", "-A", "-h", "-x", "-v", "lambert_azimuthal_equal_area", str(nee_rename), str(mask_crop)])
            shutil.move(str(mask_crop), str(tem_nee))
            run_cmd(
                [
                    "ncrename",
                    "-O",
                    "-h",
                    "-d",
                    "X,longitude",
                    "-d",
                    "Y,latitude",
                    "-v",
                    "X,longitude",
                    "-v",
                    "Y,latitude",
                    str(tem_nee),
                    str(tem_nee),
                ]
            )

            nt = parse_time_length(tem_nee)
            if nt <= 0:
                log(f"Skipping {in_file.name}; invalid time length: {nt}")
                continue
            if out_file.exists():
                out_file.unlink()

            for m in range(nt):
                month_idx = m + 1
                log(f"  Month {month_idx}/{nt}")
                time_val = parse_time_value(tem_nee, m)

                run_cmd(["ncks", "-O", "-h", "-d", f"time,{m}", str(tem_nee), str(slice_nc)])
                run_cmd(
                    [
                        "gdalwarp",
                        "-overwrite",
                        "-of",
                        "netCDF",
                        "-r",
                        "bilinear",
                        "-s_srs",
                        "EPSG:6931",
                        "-t_srs",
                        "EPSG:4326",
                        "-te_srs",
                        te_srs,
                        "-tr",
                        str(grid_res[0]),
                        str(grid_res[1]),
                        "-te",
                        str(grid_extent[0]),
                        str(grid_extent[1]),
                        str(grid_extent[2]),
                        str(grid_extent[3]),
                        f"NETCDF:{slice_nc}:{var}",
                        str(upscaled_nc),
                    ]
                )

                run_cmd(
                    [
                        "ncap2",
                        "-O",
                        "-s",
                        f'defdim("time",1); time[time]={{{time_val}}};',
                        str(upscaled_nc),
                        str(upscaled_nc),
                    ]
                )
                run_cmd(["ncatted", "-O", "-h", "-a", "units,time,c,c,days since 1901-01-01", str(upscaled_nc), str(upscaled_nc)])
                run_cmd(["ncatted", "-O", "-h", "-a", "calendar,time,c,c,365_day", str(upscaled_nc), str(upscaled_nc)])
                run_cmd(["ncap2", "-O", "-s", "NEE[time,lat,lon]=Band1;", str(upscaled_nc), str(upscaled_nc)])
                run_cmd(["ncks", "-O", "-h", "-x", "-v", "Band1", str(upscaled_nc), str(upscaled_nc)])
                run_cmd(
                    [
                        "ncrename",
                        "-O",
                        "-h",
                        "-d",
                        "lat,latitude",
                        "-d",
                        "lon,longitude",
                        "-v",
                        "lat,latitude",
                        "-v",
                        "lon,longitude",
                        str(upscaled_nc),
                        str(upscaled_nc),
                    ]
                )
                run_cmd(["ncks", "-O", "-h", "--mk_rec_dmn", "time", str(upscaled_nc), str(upscaled_nc)])

                if m == 0:
                    shutil.copyfile(upscaled_nc, out_file)
                else:
                    run_cmd(["ncrcat", "-O", "-h", str(out_file), str(upscaled_nc), str(concat_nc)])
                    shutil.move(str(concat_nc), str(out_file))

            log(f"  Wrote: {out_file.name}")

    log(f"Done. Upscaled yearly NEE files are in: {out_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        log(f"ERROR: {exc}")
        raise SystemExit(1)
