# Description: this script merge outputs across multiple tiles,
# even when tiles are not adjacent. This script also summarize
# the outputs when specified by user.

import argparse
import glob
import os
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import xarray as xr

# Suppress xarray duplicate dimension warning (e.g. from netCDF 'string1' dims)
warnings.filterwarnings("ignore", message="Duplicate dimension names present", module="xarray")

SKIP_FILES = {
    "restart-sc.nc",
    "restart-tr.nc",
    "restart-eq.nc",
    "restart-sp.nc",
    "restart-pr.nc",
    "run_status.nc",
}


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Merge TEM outputs across multiple tiles for a given scenario"
    )
    parser.add_argument(
        "base_path",
        help="Base path containing tile directories (e.g., /mnt/exacloud/dteber_woodwellclimate_org/Alaska)",
    )
    parser.add_argument(
        "scenario",
        help="Scenario name to process (e.g., ssp1_2_6_mri_esm2_0)",
    )
    parser.add_argument(
        "--temdir",
        default="/opt/apps/dvm-dos-tem",
        help="Path to dvm-dos-tem directory (default: /opt/apps/dvm-dos-tem)",
    )
    parser.add_argument(
        "--run-stage",
        default="sc",
        choices=["eq", "sp", "tr", "sc"],
        help="Run stage to process: eq=equilibrium, sp=spinup, tr=transient, sc=scenario (default: sc)",
    )
    parser.add_argument(
        "--no-yearsynth",
        action="store_false",
        dest="yearsynth",
        help="Disable yearly synthesis of monthly outputs",
    )
    parser.add_argument(
        "--no-compsynth",
        action="store_false",
        dest="compsynth",
        help="Disable synthesis across compartments",
    )
    parser.add_argument(
        "--no-pftsynth",
        action="store_false",
        dest="pftsynth",
        help="Disable synthesis by PFT",
    )
    parser.add_argument(
        "--no-layersynth",
        action="store_false",
        dest="layersynth",
        help="Disable synthesis by layer",
    )
    parser.add_argument(
        "--workers",
        "-j",
        type=int,
        default=1,
        help="Parallel workers for monthly year-by-year merge (default: 1)",
    )
    parser.set_defaults(yearsynth=True, compsynth=True, pftsynth=True, layersynth=True)
    return parser.parse_args()


def get_tile_var_file(base_path, scenario, tile, run_stage, var):
    all_merged_dir = os.path.join(base_path, scenario, tile, "all_merged")
    var_files = glob.glob(os.path.join(all_merged_dir, f"{var}_*_{run_stage}.nc"))
    if not var_files:
        return None
    return var_files[0]


def apply_dimension_synthesis(out, var, var_spec, compsynth, pftsynth, layersynth):
    if var_spec is None:
        return out

    if "pftpart" in list(out[var].dims) and compsynth:
        if str(var_spec.get("Compartments", "")).lower() not in ["invalid", ""]:
            out = out.sum(dim="pftpart", skipna=True)
    if "pft" in list(out[var].dims) and pftsynth:
        if str(var_spec.get("PFT", "")).lower() not in ["invalid", ""]:
            out = out.sum(dim="pft", skipna=True)
    if "layer" in list(out[var].dims) and layersynth:
        if str(var_spec.get("Layers", "")).lower() not in ["invalid", ""]:
            out = out.sum(dim="layer", skipna=True)

    return out


def _process_one_year(payload):
    """Worker payload for one variable and one year."""
    year = payload["year"]
    var = payload["var"]
    base_path = payload["base_path"]
    scenario = payload["scenario"]
    run_stage = payload["run_stage"]
    synthdir = payload["synthdir"]
    tilelist = payload["tilelist"]
    canvas_path = payload["canvas_path"]
    var_spec = payload["var_spec"]
    compsynth = payload["compsynth"]
    pftsynth = payload["pftsynth"]
    layersynth = payload["layersynth"]

    crop_mask = xr.open_dataset(canvas_path)
    x_coord = "X" if "X" in crop_mask.coords else "x"
    y_coord = "Y" if "Y" in crop_mask.coords else "y"

    canevas = None
    for tile in tilelist:
        var_file = get_tile_var_file(base_path, scenario, tile, run_stage, var)
        if var_file is None:
            continue

        out = None
        msk = None
        try:
            out = xr.open_dataset(var_file)
            out = out.sel(time=out["time"].dt.year == year)
            if out.sizes.get("time", 0) == 0:
                out.close()
                continue
            out = out.load()

            if var in out.variables:
                out[var] = out[var].where(out[var] != -9999, np.nan)
            out = apply_dimension_synthesis(out, var, var_spec, compsynth, pftsynth, layersynth)

            msk = xr.open_dataset(os.path.join(base_path, scenario, tile, "run-mask.nc"))
            msk_x = "X" if "X" in msk.coords else "x"
            msk_y = "Y" if "Y" in msk.coords else "y"
            out = out.assign_coords(x=("x", msk[msk_x].values), y=("y", msk[msk_y].values))

            if canevas is None:
                varfv = out[var].encoding.get("_FillValue")
                varfv = np.nan if varfv is None else varfv
                dimname = list(out[var].dims)
                dimlengthlist = []
                for dim in dimname:
                    if dim == "x":
                        dimlengthlist.append(crop_mask[x_coord].shape[0])
                    elif dim == "y":
                        dimlengthlist.append(crop_mask[y_coord].shape[0])
                    else:
                        dimlengthlist.append(out[dim].shape[0])

                coords = {}
                for dim in dimname:
                    if dim == "x":
                        coords[dim] = crop_mask[x_coord].values
                    elif dim == "y":
                        coords[dim] = crop_mask[y_coord].values
                    else:
                        coords[dim] = out[dim].values

                data_vars = {var: (tuple(dimname), np.full(tuple(dimlengthlist), varfv))}
                canevas = xr.Dataset(data_vars, coords=coords)
                canevas.attrs = out.attrs
                varattrs = out[var].attrs.copy()
                varattrs["_FillValue"] = varfv
                canevas[var].attrs = varattrs
                canevas.encoding = out.encoding

            canevas = out.combine_first(canevas)
        except Exception as exc:
            return (int(year), None, f"{tile}: {exc}")
        finally:
            if out is not None:
                out.close()
            if msk is not None:
                msk.close()

    crop_mask.close()

    if canevas is None:
        return (int(year), None, None)

    crop_mask = xr.open_dataset(canvas_path)
    x_coord = "X" if "X" in crop_mask.coords else "x"
    y_coord = "Y" if "Y" in crop_mask.coords else "y"
    canevas["y"] = crop_mask[y_coord]
    canevas["x"] = crop_mask[x_coord]
    canevas["x"].attrs = crop_mask[x_coord].attrs
    canevas["y"].attrs = crop_mask[y_coord].attrs

    output_filename = f"{var}_{scenario}_{run_stage}_monthly_{int(year)}.nc"
    canevas.to_netcdf(os.path.join(synthdir, output_filename))
    canevas.close()
    crop_mask.close()

    return (int(year), output_filename, None)


def main():
    args = parse_arguments()
    base_path = args.base_path
    scenario = args.scenario
    run_stage = args.run_stage
    synthdir = os.path.join(base_path, "merged")
    temdir = args.temdir
    yearsynth = args.yearsynth
    compsynth = args.compsynth
    pftsynth = args.pftsynth
    layersynth = args.layersynth
    workers = max(1, int(args.workers))

    os.makedirs(synthdir, exist_ok=True)

    tilelist = []
    outflist = []
    scenario_path = os.path.join(base_path, scenario)
    if os.path.exists(scenario_path):
        for item in os.listdir(scenario_path):
            tile_path = os.path.join(scenario_path, item)
            if os.path.isdir(tile_path) and not item.startswith("."):
                all_merged_path = os.path.join(tile_path, "all_merged")
                if os.path.exists(all_merged_path):
                    tilelist.append(item)
                    for outf in os.listdir(all_merged_path):
                        out_path = os.path.join(all_merged_path, outf)
                        if (
                            os.path.isfile(out_path)
                            and outf.endswith(".nc")
                            and not outf.startswith(".")
                            and outf not in SKIP_FILES
                            and f"_{run_stage}.nc" in outf
                        ):
                            outflist.append(outf)

    outflist = list(set(outflist))
    listed_vars = list(set([item.split("_")[0] for item in outflist]))
    print(f"Found {len(tilelist)} tiles for scenario '{scenario}' with run stage '{run_stage}':")
    for tile in tilelist:
        print(f"  - {tile}")
    print(f"Found {len(listed_vars)} variables: {listed_vars}")

    if not tilelist:
        print("No tiles found. Exiting.")
        return

    xminlist, xmaxlist, yminlist, ymaxlist = [], [], [], []
    for tile in tilelist:
        mask_path = os.path.join(base_path, scenario, tile, "run-mask.nc")
        if not os.path.exists(mask_path):
            continue
        with xr.open_dataset(mask_path) as mask:
            if "X" in mask.coords:
                xminlist.append(mask.X.min().values.item())
                xmaxlist.append(mask.X.max().values.item())
            elif "x" in mask.coords:
                xminlist.append(mask.x.min().values.item())
                xmaxlist.append(mask.x.max().values.item())

            if "Y" in mask.coords:
                yminlist.append(mask.Y.min().values.item())
                ymaxlist.append(mask.Y.max().values.item())
            elif "y" in mask.coords:
                yminlist.append(mask.y.min().values.item())
                ymaxlist.append(mask.y.max().values.item())

    print(
        "yminlist: "
        + str(yminlist)
        + " ymaxlist: "
        + str(ymaxlist)
        + " xminlist: "
        + str(xminlist)
        + " xmaxlist: "
        + str(xmaxlist)
    )

    first_tile = tilelist[0]
    template_mask_path = os.path.join(base_path, scenario, first_tile, "run-mask.nc")
    with xr.open_dataset(template_mask_path) as template_mask:
        x_coord = "X" if "X" in template_mask.coords else "x"
        y_coord = "Y" if "Y" in template_mask.coords else "y"
        crop_mask = template_mask.sel(
            {
                x_coord: slice(min(xminlist), max(xmaxlist)),
                y_coord: slice(min(yminlist), max(ymaxlist)),
            }
        )
        crop_mask.to_netcdf(os.path.join(synthdir, "canvas.nc"))

    print(
        f"Canvas created with extent: x=[{min(xminlist):.2f}, {max(xmaxlist):.2f}], "
        f"y=[{min(yminlist):.2f}, {max(ymaxlist):.2f}]"
    )

    output_spec_path = os.path.join(temdir, "output_spec.csv")
    if not os.path.exists(output_spec_path):
        print("Warning: output_spec.csv not found, synthesis options will be disabled")
        print(output_spec_path, "does not exist")
        sys.exit()

    ovl = pd.read_csv(output_spec_path)
    print("Loaded output specification file")

    # Keep current fixed list behavior
    varlist = ["GPP", "RECO"]
    print("varlist:", varlist)

    var_specs = {}
    if "Name" in ovl.columns:
        for _, row in ovl.iterrows():
            var_specs[row["Name"]] = row

    canvas_path = os.path.join(synthdir, "canvas.nc")


    for var in varlist:
        print(f"Processing variable: {var}")
        tempres = None
        canevas = None
        first_tile_var_file = None

        for tile in tilelist:
            var_file = get_tile_var_file(base_path, scenario, tile, run_stage, var)
            if var_file is not None:
                first_tile_var_file = var_file
                tempres = os.path.basename(var_file).split("_")[1]
                break

        if first_tile_var_file is None:
            print(f"  Warning: No file found for variable {var}, skipping")
            print("********************************************************")
            continue

        var_spec = var_specs.get(var)

        # Memory-efficient path: monthly + no yearly synthesis => file per year
        if tempres == "monthly" and not yearsynth:
            print(f"  Using year-by-year merge (memory-efficient mode, workers={workers})")
            with xr.open_dataset(first_tile_var_file) as ds_ref:
                years = np.unique(ds_ref["time"].dt.year.values.astype(int))
            print(f"  Years to process: {len(years)} ({years[0]}-{years[-1]})")

            payloads = []
            for year in years:
                payloads.append(
                    {
                        "year": int(year),
                        "var": var,
                        "base_path": base_path,
                        "scenario": scenario,
                        "run_stage": run_stage,
                        "synthdir": synthdir,
                        "tilelist": tilelist,
                        "canvas_path": canvas_path,
                        "var_spec": None if var_spec is None else var_spec.to_dict(),
                        "compsynth": compsynth,
                        "pftsynth": pftsynth,
                        "layersynth": layersynth,
                    }
                )

            if workers == 1:
                for idx, payload in enumerate(payloads, start=1):
                    year, filename, err = _process_one_year(payload)
                    if err:
                        print(f"    Error year {year}: {err}")
                    elif filename:
                        print(f"    Saved year {year} ({idx}/{len(years)})")
            else:
                completed = 0
                with ProcessPoolExecutor(max_workers=workers) as executor:
                    futures = [executor.submit(_process_one_year, payload) for payload in payloads]
                    for future in as_completed(futures):
                        year, filename, err = future.result()
                        if err:
                            print(f"    Error year {year}: {err}")
                        elif filename:
                            completed += 1
                            print(f"    Saved year {year} ({completed}/{len(years)})")

            print("********************************************************")
            continue

        # Standard path: one merged file for variable
        with xr.open_dataset(canvas_path) as crop_mask:
            x_coord = "X" if "X" in crop_mask.coords else "x"
            y_coord = "Y" if "Y" in crop_mask.coords else "y"

            for t, tile in enumerate(tilelist, start=1):
                print(f"  Processing tile {t}/{len(tilelist)}: {tile}")
                var_file = get_tile_var_file(base_path, scenario, tile, run_stage, var)
                if var_file is None:
                    print(f"    Warning: No file found for variable {var} in tile {tile}")
                    continue

                out = None
                msk = None
                try:
                    out = xr.open_dataset(var_file)
                    if var in out.variables:
                        out[var] = out[var].where(out[var] != -9999, np.nan)

                    msk = xr.open_dataset(os.path.join(base_path, scenario, tile, "run-mask.nc"))
                    tempres = os.path.basename(var_file).split("_")[1]

                    if var_spec is not None:
                        units = var_spec.get("Units", "")
                        default_op = "sum" if "/time" in str(units) else "mean"

                        if tempres == "monthly" and yearsynth:
                            if (
                                str(var_spec.get("Monthly", "")).lower() == "m"
                                and str(var_spec.get("Yearly", "")).lower() == "y"
                            ):
                                tempres = "yearly"
                                if default_op == "sum":
                                    yearly_data = out[var].resample(time="Y").sum(skipna=True, min_count=1)
                                else:
                                    yearly_data = out[var].resample(time="Y").mean(skipna=True)
                                out = yearly_data.to_dataset()
                                if "units" in out[var].attrs:
                                    original_units = out[var].attrs["units"]
                                    if "/month" in original_units:
                                        out[var].attrs["units"] = original_units.replace("/month", "/year")
                                    elif "/time" in original_units and default_op == "sum":
                                        out[var].attrs["units"] = original_units.replace("/time", "/year")

                        out = apply_dimension_synthesis(out, var, var_spec, compsynth, pftsynth, layersynth)

                    msk_x = "X" if "X" in msk.coords else "x"
                    msk_y = "Y" if "Y" in msk.coords else "y"
                    out = out.assign_coords(x=("x", msk[msk_x].values), y=("y", msk[msk_y].values))

                    if canevas is None:
                        varfv = out[var].encoding.get("_FillValue")
                        if varfv is None:
                            varfv = np.nan

                        dimname = list(out[var].dims)
                        dimlengthlist = []
                        for dim in dimname:
                            if dim == "x":
                                dimlengthlist.append(crop_mask[x_coord].shape[0])
                            elif dim == "y":
                                dimlengthlist.append(crop_mask[y_coord].shape[0])
                            else:
                                dimlengthlist.append(out[dim].shape[0])

                        coords = {}
                        for dim in dimname:
                            if dim == "x":
                                coords[dim] = crop_mask[x_coord].values
                            elif dim == "y":
                                coords[dim] = crop_mask[y_coord].values
                            else:
                                coords[dim] = out[dim].values

                        data_vars = {var: (tuple(dimname), np.full(tuple(dimlengthlist), varfv))}
                        canevas = xr.Dataset(data_vars, coords=coords)
                        canevas.attrs = out.attrs
                        varattrs = out[var].attrs.copy()
                        varattrs["_FillValue"] = varfv
                        canevas[var].attrs = varattrs
                        canevas.encoding = out.encoding

                    canevas = out.combine_first(canevas)
                except Exception as exc:
                    print(f"    Error processing tile {tile}: {exc}")
                finally:
                    if out is not None:
                        out.close()
                    if msk is not None:
                        msk.close()

            if canevas is not None:
                canevas["y"] = crop_mask[y_coord]
                canevas["x"] = crop_mask[x_coord]
                canevas["x"].attrs = crop_mask[x_coord].attrs
                canevas["y"].attrs = crop_mask[y_coord].attrs
                output_filename = f"{var}_{scenario}_{run_stage}_{tempres}.nc"
                output_path = os.path.join(synthdir, output_filename)
                print(f"  Saving merged output: {output_filename}")
                canevas.to_netcdf(output_path)
                canevas.close()

        print("********************************************************")

    print(f"\nMerging complete! Output files saved to: {synthdir}")


if __name__ == "__main__":
    main()

