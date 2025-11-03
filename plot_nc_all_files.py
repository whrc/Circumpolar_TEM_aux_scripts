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

def plot_variable(nc_file, variable_name):
    """
    Reads the specified variable from a NetCDF file, calculates mean over time,
    and returns a Matplotlib figure.
    """
    try:
        with Dataset(nc_file, "r") as nc:
            # Check if variable exists
            if variable_name not in nc.variables:
                print(f"Variable {variable_name} not found in {nc_file}")
                return None

            # Extract dimensions
            time_dim = "time" if "time" in nc.dimensions else None
            Y = nc.dimensions['y'].size
            X = nc.dimensions['x'].size

            # Extract data
            var_data = nc.variables[variable_name][:]
            
            # Handle 4D data with layer dimension - extract layer 0
            layer_extracted = False
            if 'layer' in nc.dimensions:
                var_dims = nc.variables[variable_name].dimensions
                if len(var_dims) == 4 and 'layer' in var_dims:
                    layer_idx = var_dims.index('layer')
                    print(f"Detected 4D data with dimensions: {var_dims}")
                    print(f"Extracting layer index 0 from position {layer_idx}")
                    
                    # Extract layer 0 based on its position in dimensions
                    if layer_idx == 1:  # (time, layer, y, x)
                        var_data = var_data[:, 0, :, :]
                    elif layer_idx == 0:  # (layer, time, y, x) - unlikely but handle it
                        var_data = var_data[0, :, :, :]
                    elif layer_idx == 2:  # (time, y, layer, x) - unlikely but handle it
                        var_data = var_data[:, :, 0, :]
                    elif layer_idx == 3:  # (time, y, x, layer) - unlikely but handle it
                        var_data = var_data[:, :, :, 0]
                    
                    layer_extracted = True
                    print(f"✅ Extracted layer 0, new shape: {var_data.shape}")
            
            # Handle masked arrays properly - convert masked values to NaN
            if isinstance(var_data, np.ma.MaskedArray):
                var_data = np.ma.filled(var_data, np.nan)
            else:
                # For non-masked arrays, replace fill values with NaN
                fill_value = nc.variables[variable_name]._FillValue if hasattr(nc.variables[variable_name], "_FillValue") else np.nan
                var_data = np.where(var_data == fill_value, np.nan, var_data)
            
            # Check if dataset has any valid data
            if not np.any(np.isfinite(var_data)):
                print(f"⚠️ Warning: {variable_name} has no valid data (all values are masked/NaN). Skipping.")
                return None

            print('time_dim:',time_dim)
            t_size = nc.dimensions[time_dim].size
            time_steps = np.arange(t_size)
            print(f"Time dimension size: {t_size}")
            
            # Check if this is monthly data that would benefit from annual averaging
            # Monthly data typically has > 500 timesteps and is divisible by 12
            if t_size > 500 and t_size % 12 == 0:
                years = t_size // 12
                print(f"Detected monthly data: {t_size} timesteps = {years} years")
                print("Applying annual averaging to reduce plot crowding...")
                
                # Reshape: (t_size, Y, X) → (years, 12, Y, X)
                var_data = var_data.reshape(years, 12, Y, X)
                
                # Compute annual mean along the monthly axis
                var_data = np.nanmean(var_data, axis=1)  # Shape: (years, Y, X)
                time_steps = np.arange(var_data.shape[0])
                
                print(f"✅ Reduced to {var_data.shape[0]} annual timesteps")
            elif t_size == 12000:
                print("Detected special case: 12000 timesteps, applying custom averaging...")
                # Keep the original logic for 12000 timesteps if it's different
                var_data = var_data.reshape(1000, 12, Y, X)
                var_data = np.nanmean(var_data, axis=1)
                time_steps = np.arange(var_data.shape[0])
                print(f"✅ Reduced to {var_data.shape[0]} timesteps")

            # Compute mean var_data over X and Y for each time step
            mean_var_data = np.nanmean(var_data, axis=(1, 2))  # Shape: (time,)
            std_var_data = np.nanstd(var_data, axis=(1, 2))  # Standard deviation for shading
            
            # Determine time label based on averaging applied
            original_t_size = nc.dimensions[time_dim].size
            time_label = "Time (years)"
            averaging_info = ""
            if original_t_size > 500 and original_t_size % 12 == 0:
                averaging_info = " (Annual Avg)"
            elif original_t_size == 12000:
                averaging_info = " (Averaged)"
            
            # Plot
            fig, axes = plt.subplots(1, 3, figsize=(12, 5))

            # Add layer indicator to titles if layer was extracted
            layer_suffix = " (Layer=0)" if layer_extracted else ""

            # Plot var_data at first time step
            im0 = axes[0].imshow(np.fliplr(var_data[0,:,:].T), cmap="viridis", origin="lower", aspect="auto")
            axes[0].set_title(f"{variable_name} - First Year{layer_suffix}")
            axes[0].set_xlabel("X")
            axes[0].set_ylabel("Y")
            # Get units from variable if available
            units = getattr(nc.variables[variable_name], 'units', '')
            fig.colorbar(im0, ax=axes[0], label=f"{variable_name} ({units})" if units else variable_name)

            # Plot var_data at last time step
            imN = axes[1].imshow(np.fliplr(var_data[-1,:,:].T), cmap="viridis", origin="lower", aspect="auto")
            axes[1].set_title(f"{variable_name} - Last Year{layer_suffix}")
            axes[1].set_xlabel("X")
            axes[1].set_ylabel("Y")
            fig.colorbar(imN, ax=axes[1], label=f"{variable_name} ({units})" if units else variable_name)

            axes[2].plot(time_steps, mean_var_data, color="b", label=f"Mean {variable_name}")
            axes[2].fill_between(time_steps, mean_var_data - std_var_data, mean_var_data + std_var_data, color="b", alpha=0.2, label="±1 Std Dev")

            # Labels and titles
            axes[2].set_xlabel(time_label)
            axes[2].set_ylabel(f"{variable_name} ({units})" if units else variable_name)
            axes[2].set_title(f"Mean {variable_name} Over Time{averaging_info}")
            axes[2].legend()


            plt.tight_layout()

            return fig

    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        return None

def generate_pdf(folder_path, output_pdf="summary_plots.pdf"):
    """
    Loops through all NetCDF files in the folder, extracts variables, generates plots,
    and saves them in a multi-page PDF.
    """
    nc_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".nc") and f[0].isupper()])  # Only files starting with a capital letter, sorted alphabetically
    if not nc_files:
        print("No valid NetCDF files found in the specified folder.")
        return
    new_file_path = os.path.join(folder_path, output_pdf)
    
    with PdfPages(new_file_path) as pdf:
        for nc_file in nc_files:
            nc_file_path = os.path.join(folder_path, nc_file)
            variable_name = extract_variable_name(nc_file)

            if variable_name:
                fig = plot_variable(nc_file_path, variable_name)
                if fig:
                    pdf.savefig(fig)  # Save the figure to PDF
                    plt.close(fig)
                    print(f"Added plot for {variable_name} from {nc_file}")

    print(f"Plots saved in {output_pdf}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python plot_all_nc_files.py <folder_path>")
        sys.exit(1)

    folder_path = sys.argv[1]
    generate_pdf(folder_path)
