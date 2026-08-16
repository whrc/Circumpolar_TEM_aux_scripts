import argparse
import gc
import os
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from matplotlib.backends.backend_pdf import PdfPages

# NetCDF row/column dimension names (merged WIEMIP often uses Y/X, not y/x).
_ROW_DIM_CANDIDATES = ("y", "Y", "latitude", "lat")
_COL_DIM_CANDIDATES = ("x", "X", "longitude", "lon")
# Stay below this when loading arrays (4D files can be >10 GB if all layers are loaded).
_MAX_LOAD_GB = 6.0


def _resolve_row_col_dims(nc, variable_name):
    """Return (row_dim_name, col_dim_name) for the variable's horizontal grid."""
    var_dims = nc.variables[variable_name].dimensions
    for row in _ROW_DIM_CANDIDATES:
        if row not in var_dims:
            continue
        for col in _COL_DIM_CANDIDATES:
            if col in var_dims:
                return row, col
    raise KeyError(
        f"No recognized row/col dimensions for variable {variable_name!r}. "
        f"Variable dims: {var_dims!s}, file dimensions: {tuple(nc.dimensions.keys())}"
    )


def _find_extra_dim(var_dims, row_dim, col_dim):
    """Return (extra_dim_name, extra_dim_index) for layer/pft/etc., or (None, None)."""
    extras = [d for d in var_dims if d not in ("time", row_dim, col_dim)]
    if len(extras) == 1:
        return extras[0], var_dims.index(extras[0])
    return None, None


def extract_variable_name(filename):
    """Extracts the variable name from the filename before the first underscore `_`."""
    parts = filename.split("_")
    if parts:
        return parts[0]
    return None


def resolve_variable_name(nc, filename):
    """Resolve the data variable name from filename or file contents."""
    candidate = extract_variable_name(filename)
    if candidate and candidate in nc.variables and candidate not in nc.dimensions:
        return candidate

    stem = os.path.splitext(filename)[0]
    for suffix in ("_yearly_tr", "_monthly_tr", "_yearly", "_monthly"):
        if stem.endswith(suffix):
            name = stem[: -len(suffix)]
            if name in nc.variables and name not in nc.dimensions:
                return name

    for name, var in nc.variables.items():
        if name in nc.dimensions:
            continue
        if len(getattr(var, "dimensions", ())) >= 2:
            return name
    return None


def _estimate_gb(shape, itemsize=8):
    n = 1
    for dim in shape:
        n *= dim
    return n * itemsize / (1024**3)


def _clean_array(data, var):
    """Convert array to float64 with invalid values replaced by NaN."""
    if isinstance(data, np.ma.MaskedArray):
        data = np.ma.filled(data, np.nan)
    else:
        data = np.array(data, dtype=np.float64, copy=False)
        fill_value = getattr(var, "_FillValue", None)
        if fill_value is not None:
            data = np.where(data == fill_value, np.nan, data)
    return np.where(np.isclose(data, -9999.0), np.nan, data)


def _read_2d_slice(var, t_idx, extra_idx=0):
    if var.ndim == 4:
        data = var[t_idx, extra_idx, :, :]
    elif var.ndim == 3:
        data = var[t_idx, :, :]
    else:
        raise ValueError(f"Unsupported variable rank {var.ndim} for plotting")
    return _clean_array(data, var)


def _load_time_series(var, extra_idx=0):
    """
    Load a (time, row, col) array without pulling all layers from 4D variables.
    Falls back to chunked reads if the requested array would exceed _MAX_LOAD_GB.
    """
    if var.ndim == 4:
        load_shape = (var.shape[0], var.shape[2], var.shape[3])
        if _estimate_gb(load_shape, var.dtype.itemsize) <= _MAX_LOAD_GB:
            return _clean_array(var[:, extra_idx, :, :], var), False

        # Rare fallback for unusually large grids.
        t_size = var.shape[0]
        out = np.empty((t_size, var.shape[2], var.shape[3]), dtype=np.float64)
        for t_idx in range(t_size):
            out[t_idx] = _read_2d_slice(var, t_idx, extra_idx)
        return out, True

    if var.ndim == 3:
        if _estimate_gb(var.shape, var.dtype.itemsize) <= _MAX_LOAD_GB:
            return _clean_array(var[:], var), False
        t_size = var.shape[0]
        out = np.empty((t_size, var.shape[1], var.shape[2]), dtype=np.float64)
        for t_idx in range(t_size):
            out[t_idx] = _read_2d_slice(var, t_idx)
        return out, True

    raise ValueError(f"Unsupported variable rank {var.ndim} for plotting")


def _annualize_monthly(var_data, n_row, n_col):
    t_size = var_data.shape[0]
    years = t_size // 12
    var_data = var_data.reshape(years, 12, n_row, n_col)
    with np.errstate(all="ignore"):
        var_data = np.nanmean(var_data, axis=1)
    return var_data, np.arange(years)


def plot_variable(nc_file, variable_name):
    """
    Reads the specified variable from a NetCDF file, calculates mean over time,
    and returns a Matplotlib figure.
    """
    try:
        with Dataset(nc_file, "r") as nc:
            resolved_name = resolve_variable_name(nc, os.path.basename(nc_file))
            if resolved_name is None:
                print(f"No plottable variable found in {nc_file}")
                return None
            if resolved_name != variable_name:
                print(
                    f"Using variable {resolved_name!r} from {nc_file} "
                    f"(filename suggested {variable_name!r})"
                )
                variable_name = resolved_name

            if variable_name not in nc.variables:
                print(f"Variable {variable_name} not found in {nc_file}")
                return None

            if "time" not in nc.dimensions:
                print(f"No 'time' dimension in {nc_file}; skipping.")
                return None

            var = nc.variables[variable_name]
            row_dim, col_dim = _resolve_row_col_dims(nc, variable_name)
            n_row = nc.dimensions[row_dim].size
            n_col = nc.dimensions[col_dim].size
            extra_dim, _ = _find_extra_dim(var.dimensions, row_dim, col_dim)
            extra_idx = 0
            layer_extracted = extra_dim is not None

            if layer_extracted:
                print(f"Detected extra dimension {extra_dim!r}; using index 0")

            t_size = var.shape[0]
            print("time_dim: time")
            print(f"Time dimension size: {t_size}")

            var_data, loaded_chunked = _load_time_series(var, extra_idx)
            units = getattr(var, "units", "")
            if loaded_chunked:
                print("Loaded time series in chunks to limit memory use")
            del var
            gc.collect()

            if not np.any(np.isfinite(var_data)):
                print(
                    f"Warning: {variable_name} has no valid data "
                    f"(all values are masked/NaN). Skipping."
                )
                return None

            time_steps = np.arange(var_data.shape[0])
            averaging_info = ""
            if t_size > 500 and t_size % 12 == 0:
                years = t_size // 12
                print(f"Detected monthly data: {t_size} timesteps = {years} years")
                print("Applying annual averaging to reduce plot crowding...")
                var_data, time_steps = _annualize_monthly(var_data, n_row, n_col)
                averaging_info = " (Annual Avg)"
                print(f"Reduced to {var_data.shape[0]} annual timesteps")
            elif t_size == 12000:
                print("Detected special case: 12000 timesteps, applying custom averaging...")
                var_data = var_data.reshape(1000, 12, n_row, n_col)
                with np.errstate(all="ignore"):
                    var_data = np.nanmean(var_data, axis=1)
                time_steps = np.arange(var_data.shape[0])
                averaging_info = " (Averaged)"
                print(f"Reduced to {var_data.shape[0]} timesteps")

            with np.errstate(all="ignore"):
                mean_var_data = np.nanmean(var_data, axis=(1, 2))
                std_var_data = np.nanstd(var_data, axis=(1, 2))

            layer_suffix = " (Layer=0)" if layer_extracted else ""

            fig, axes = plt.subplots(1, 3, figsize=(12, 5))

            im0 = axes[0].imshow(
                np.flipud(var_data[0, :, :].T), cmap="viridis", origin="lower", aspect="auto"
            )
            axes[0].set_title(f"{variable_name} - First Year{layer_suffix}")
            axes[0].set_xlabel("X")
            axes[0].set_ylabel("Y")
            fig.colorbar(
                im0,
                ax=axes[0],
                label=f"{variable_name} ({units})" if units else variable_name,
            )

            imN = axes[1].imshow(
                np.flipud(var_data[-1, :, :].T), cmap="viridis", origin="lower", aspect="auto"
            )
            axes[1].set_title(f"{variable_name} - Last Year{layer_suffix}")
            axes[1].set_xlabel("X")
            axes[1].set_ylabel("Y")
            fig.colorbar(
                imN,
                ax=axes[1],
                label=f"{variable_name} ({units})" if units else variable_name,
            )

            axes[2].plot(time_steps, mean_var_data, color="b", label=f"Mean {variable_name}")
            axes[2].fill_between(
                time_steps,
                mean_var_data - std_var_data,
                mean_var_data + std_var_data,
                color="b",
                alpha=0.2,
                label="±1 Std Dev",
            )
            axes[2].set_xlabel("Time (years)")
            axes[2].set_ylabel(f"{variable_name} ({units})" if units else variable_name)
            axes[2].set_title(f"Mean {variable_name} Over Time{averaging_info}")
            axes[2].legend()

            plt.tight_layout()
            del var_data
            return fig

    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        return None


def generate_pdf(folder_path, output_pdf="summary_plots.pdf", dpi_png=150, skip_existing=False):
    """
    Loops through all NetCDF files in the folder, extracts variables, generates plots,
    and saves them in a multi-page PDF and one PNG per variable file.
    """
    nc_files = sorted(
        f for f in os.listdir(folder_path) if f.endswith(".nc") and f[0].isupper()
    )
    if not nc_files:
        print("No valid NetCDF files found in the specified folder.")
        return

    new_file_path = os.path.join(folder_path, output_pdf)

    with PdfPages(new_file_path) as pdf:
        for nc_file in nc_files:
            nc_file_path = os.path.join(folder_path, nc_file)
            stem, _ = os.path.splitext(nc_file)
            png_path = os.path.join(folder_path, f"{stem}_summary.png")
            variable_name = extract_variable_name(nc_file)

            if skip_existing and os.path.exists(png_path):
                img = plt.imread(png_path)
                fig = plt.figure(figsize=(12, 5))
                ax = fig.add_axes([0, 0, 1, 1])
                ax.imshow(img)
                ax.axis("off")
                pdf.savefig(fig)
                plt.close(fig)
                print(f"Reused existing PNG for {nc_file}")
                continue

            if not variable_name:
                continue

            print(f"Processing {nc_file}...")
            fig = plot_variable(nc_file_path, variable_name)
            if fig:
                pdf.savefig(fig)
                fig.savefig(png_path, dpi=dpi_png, bbox_inches="tight")
                print(f"Saved PNG: {png_path}")
                plt.close(fig)
                print(f"Added plot for {variable_name} from {nc_file}")
            gc.collect()

    print(f"Plots saved in {new_file_path} and matching *_summary.png files in the same folder.")


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Generate summary plots for NetCDF files in a folder. "
            "Creates a multi-page PDF and one PNG per variable."
        ),
        epilog=(
            "Example:\n"
            "  python plot_nc_all_files.py /path/to/all_merged_restored\n"
            "  python plot_nc_all_files.py /path/to/all_merged_restored --skip-existing"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "folder_path",
        help="Directory containing NetCDF files (only *.nc names starting with a capital letter are plotted)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Reuse existing *_summary.png files and only plot missing variables",
    )
    parser.add_argument(
        "--output-pdf",
        default="summary_plots.pdf",
        metavar="FILE",
        help="Output PDF filename inside folder_path (default: summary_plots.pdf)",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=150,
        metavar="N",
        help="Resolution for PNG output (default: 150)",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    generate_pdf(
        args.folder_path,
        output_pdf=args.output_pdf,
        dpi_png=args.dpi,
        skip_existing=args.skip_existing,
    )
