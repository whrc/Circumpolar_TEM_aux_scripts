import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from matplotlib.backends.backend_pdf import PdfPages

def extract_variable_name(filename):
    """Extracts the variable name from the filename before the first underscore `_`."""
    parts = filename.split("_")
    if parts:
        return parts[0]  # First part before `_`
    return None

def _clean_slice(data, fill_value):
    """Convert a raw NetCDF slice to a float array with NaN for invalid cells.

    Only the declared fill value and the common -9999 sentinel are masked out.
    Values < 0 are intentionally kept because many variables (e.g. soil
    temperatures) are physically negative.
    """
    if isinstance(data, np.ma.MaskedArray):
        data = data.filled(np.nan).astype(float)
    else:
        data = np.where(data == fill_value, np.nan, data.astype(float))
    data = np.where(np.isclose(data, -9999.0), np.nan, data)
    return data


def _read_spatial_slice(var, t, extra_dim_idx):
    """Return a (Y, X) array for timestep t, collapsing extra dim (layer/pft) at index 0."""
    if extra_dim_idx is not None:
        return var[t, 0, :, :]   # (time, extra, y, x) layout assumed
    return var[t, :, :]



def plot_variable(nc_file, variable_name):
    """
    Reads the specified variable from a NetCDF file in a memory-efficient way
    (one timestep or one year at a time) and returns a Matplotlib figure.
    """
    try:
        with Dataset(nc_file, "r") as nc:
            if variable_name not in nc.variables:
                print(f"Variable {variable_name} not found in {nc_file}")
                return None

            var = nc.variables[variable_name]
            var_dims = var.dimensions
            fill_value = getattr(var, "_FillValue", -9999.0)
            units = getattr(var, "units", "")

            time_dim = "time" if "time" in nc.dimensions else None
            t_size = nc.dimensions[time_dim].size
            print(f"time_dim: {time_dim}\nTime dimension size: {t_size}")

            # Detect extra dimension (layer or pft) – always at index 1 in our files
            extra_dim = next((d for d in ("layer", "pft") if d in var_dims), None)
            extra_dim_idx = 1 if extra_dim else None
            layer_suffix = f" ({extra_dim}=0)" if extra_dim else ""
            if extra_dim:
                print(f"Detected 4D data with dimensions: {var_dims} — using {extra_dim} index 0")

            # --- spatial maps ---
            # For monthly data: average the first/last 12 months into annual maps.
            # For yearly data: use the first/last timestep directly.
            is_monthly = t_size > 500 and t_size % 12 == 0
            if is_monthly:
                first_slices = np.stack(
                    [_clean_slice(_read_spatial_slice(var, m, extra_dim_idx), fill_value)
                     for m in range(12)],
                    axis=0,
                )
                last_slices = np.stack(
                    [_clean_slice(_read_spatial_slice(var, t_size - 12 + m, extra_dim_idx), fill_value)
                     for m in range(12)],
                    axis=0,
                )
                first_map = np.nanmean(first_slices, axis=0)
                last_map  = np.nanmean(last_slices,  axis=0)
            else:
                first_map = _clean_slice(_read_spatial_slice(var, 0,  extra_dim_idx), fill_value)
                last_map  = _clean_slice(_read_spatial_slice(var, -1, extra_dim_idx), fill_value)

            if not np.any(np.isfinite(first_map)):
                print(f"⚠️ Warning: {variable_name} has no valid data. Skipping.")
                return None

            # --- time series: row-by-row accumulation ---
            # Reading one y-row at a time (all timesteps for that row) aligns with
            # the (T_chunk, extra, 1, X) chunk layout, keeping peak RAM tiny and
            # avoiding repeated full-axis decompression.
            Y = nc.dimensions["y"].size
            X = nc.dimensions["x"].size

            sum_t   = np.zeros(t_size)   # accumulate spatial sum per timestep
            sum_t2  = np.zeros(t_size)   # for std: sum of squares
            cnt_t   = np.zeros(t_size, dtype=np.int64)

            print(f"Building time series (row-by-row, {Y} rows)...")
            for yi in range(Y):
                if extra_dim_idx is not None:
                    row = var[:, 0, yi, :]   # (time, X)
                else:
                    row = var[:, yi, :]       # (time, X)
                row = _clean_slice(row, fill_value)  # (time, X) float64
                valid = ~np.isnan(row)
                sum_t  += np.where(valid, row, 0.0).sum(axis=-1)
                sum_t2 += np.where(valid, row ** 2, 0.0).sum(axis=-1)
                cnt_t  += valid.sum(axis=-1)

            with np.errstate(invalid="ignore", divide="ignore"):
                mean_t = np.where(cnt_t > 0, sum_t / cnt_t, np.nan)
                # population std: sqrt(E[X²] - E[X]²)
                std_t  = np.where(cnt_t > 0,
                                  np.sqrt(np.maximum(0, sum_t2 / cnt_t - mean_t ** 2)),
                                  np.nan)

            if t_size > 500 and t_size % 12 == 0:
                years = t_size // 12
                print(f"Detected monthly data: {t_size} timesteps = {years} years")
                mean_var_data = np.nanmean(mean_t.reshape(years, 12), axis=1)
                std_var_data  = np.nanmean(std_t.reshape(years, 12),  axis=1)
                time_steps    = np.arange(years)
                averaging_info = " (Annual Avg)"
                print(f"✅ Reduced to {years} annual timesteps")
            else:
                mean_var_data = mean_t
                std_var_data  = std_t
                time_steps    = np.arange(t_size)
                averaging_info = ""

            # --- build figure ---
            fig, axes = plt.subplots(1, 3, figsize=(14, 5))

            # Shared colour scale so first/last maps are directly comparable
            all_vals = np.concatenate([first_map[np.isfinite(first_map)],
                                       last_map[np.isfinite(last_map)]])
            vmin = float(np.nanmin(all_vals)) if all_vals.size else 0
            vmax = float(np.nanmax(all_vals)) if all_vals.size else 1
            if vmin == vmax:
                vmax = vmin + 1  # avoid degenerate range

            def _scatter_map(ax, data, title):
                ys, xs = np.where(np.isfinite(data))
                vals = data[ys, xs]
                sc = ax.scatter(xs, ys, c=vals, cmap="viridis",
                                vmin=vmin, vmax=vmax,
                                s=6, linewidths=0, rasterized=True)
                ax.set_facecolor("lightgray")
                ax.set_xlim(0, data.shape[1])
                ax.set_ylim(0, data.shape[0])
                ax.set_title(title)
                ax.set_xlabel("X")
                ax.set_ylabel("Y")
                fig.colorbar(sc, ax=ax,
                             label=f"{variable_name} ({units})" if units else variable_name)

            _scatter_map(axes[0], first_map,
                         f"{variable_name} - First Year{layer_suffix}")
            _scatter_map(axes[1], last_map,
                         f"{variable_name} - Last Year{layer_suffix}")

            axes[2].plot(time_steps, mean_var_data, color="b", label=f"Mean {variable_name}")
            axes[2].fill_between(
                time_steps,
                mean_var_data - std_var_data,
                mean_var_data + std_var_data,
                color="b", alpha=0.2, label="±1 Std Dev",
            )
            axes[2].set_xlabel("Time (years)")
            axes[2].set_ylabel(f"{variable_name} ({units})" if units else variable_name)
            axes[2].set_title(f"Mean {variable_name} Over Time{averaging_info}")
            axes[2].legend()

            plt.tight_layout()
            return fig

    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_pdf(folder_path, output_pdf="summary_plots.pdf", single_file=None):
    """
    Loops through all NetCDF files in the folder (or just single_file if given),
    extracts variables, generates plots, and saves them in a multi-page PDF.
    """
    if single_file:
        if not single_file.endswith(".nc"):
            print(f"Error: '{single_file}' does not look like a NetCDF file.")
            return
        nc_files = [single_file]
        output_pdf = single_file.replace(".nc", ".pdf")
    else:
        nc_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".nc") and f[0].isupper()])
        if not nc_files:
            print("No valid NetCDF files found in the specified folder.")
            return

    new_file_path = os.path.join(folder_path, output_pdf)

    with PdfPages(new_file_path) as pdf:
        for nc_file in nc_files:
            nc_file_path = os.path.join(folder_path, nc_file)
            if not os.path.isfile(nc_file_path):
                print(f"File not found: {nc_file_path}")
                continue
            variable_name = extract_variable_name(nc_file)

            if variable_name:
                fig = plot_variable(nc_file_path, variable_name)
                if fig:
                    pdf.savefig(fig)
                    plt.close(fig)
                    print(f"Added plot for {variable_name} from {nc_file}")

    print(f"Plots saved in {new_file_path}")

if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        print("Usage: python plot_nc_all_files.py <folder_path> [filename.nc]")
        sys.exit(1)

    folder_path = sys.argv[1]
    single_file = sys.argv[2] if len(sys.argv) == 3 else None
    generate_pdf(folder_path, single_file=single_file)
