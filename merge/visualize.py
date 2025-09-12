#!/usr/bin/env python3
"""
Simple visualization script for merged TEM NetCDF output files.

Usage:
    python visualize.py <path_to_netcdf_files>

Example:
    python visualize.py /path/to/merged/files
"""

import os
import glob
import sys
import xarray as xr
import numpy as np

def get_netcdf_files(netcdf_path):
    """Get list of NetCDF files, excluding canvas files."""
    nc_files = glob.glob(os.path.join(netcdf_path, '*.nc'))
    return [f for f in nc_files if not os.path.basename(f).startswith('canvas')]

def analyze_file(file_path):
    """Analyze a single NetCDF file and print information."""
    print(f"\n{'='*60}")
    print(f"File: {os.path.basename(file_path)}")
    print(f"{'='*60}")
    
    try:
        ds = xr.open_dataset(file_path)
        
        # Find the main variable (skip coordinate variables)
        main_vars = [var for var in ds.data_vars if var not in ['lambert_azimuthal_equal_area']]
        if not main_vars:
            print("No main data variables found")
            ds.close()
            return
        
        var_name = main_vars[0]
        data = ds[var_name]
        
        # Basic info
        print(f"Variable: {var_name}")
        print(f"Shape: {data.shape}")
        print(f"Dimensions: {data.dims}")
        print(f"Units: {data.attrs.get('units', 'N/A')}")
        print(f"Long name: {data.attrs.get('long_name', 'N/A')}")
        
        # Statistics
        print(f"\nStatistics:")
        print(f"  Min: {float(data.min().values):.4f}")
        print(f"  Max: {float(data.max().values):.4f}")
        print(f"  Mean: {float(data.mean().values):.4f}")
        print(f"  Std: {float(data.std().values):.4f}")
        
        # Handle coordinates
        if 'X' in ds.coords:
            x_coord, y_coord = 'X', 'Y'
        elif 'x' in ds.coords:
            x_coord, y_coord = 'x', 'y'
        else:
            print("Warning: Could not find x,y coordinates")
            ds.close()
            return
        
        # Spatial extent
        print(f"\nSpatial extent:")
        print(f"  X range: {float(ds[x_coord].min()):.0f} to {float(ds[x_coord].max()):.0f}")
        print(f"  Y range: {float(ds[y_coord].min()):.0f} to {float(ds[y_coord].max()):.0f}")
        
        # Temporal info
        if 'time' in data.dims:
            print(f"\nTemporal info:")
            print(f"  Time steps: {data.sizes['time']}")
            print(f"  Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
            
            # Show some time points
            if data.sizes['time'] > 1:
                print(f"\nSample time series (spatial average):")
                # Use dimension names for spatial averaging, not coordinate names
                spatial_dims = [dim for dim in data.dims if dim not in ['time']]
                spatial_avg = data.mean(dim=spatial_dims)
                n_samples = min(10, data.sizes['time'])
                step = max(1, data.sizes['time'] // n_samples)
                
                for i in range(0, data.sizes['time'], step):
                    time_val = ds.time.values[i]
                    avg_val = float(spatial_avg.isel(time=i).values)
                    print(f"    {time_val}: {avg_val:.4f}")
        
        # Try simple plotting if matplotlib is available
        try:
            import matplotlib.pyplot as plt
            
            # Create output directory
            output_dir = "plots"
            os.makedirs(output_dir, exist_ok=True)
            
            # Plot spatial map (latest time step if temporal)
            if 'time' in data.dims:
                plot_data = data.isel(time=-1)  # Last time step
                time_info = f"_time_{data.sizes['time']-1}"
            else:
                plot_data = data
                time_info = ""
            
            fig, ax = plt.subplots(figsize=(10, 8))
            plot_data.plot(ax=ax, cmap='viridis')
            plt.title(f"{var_name} - {os.path.basename(file_path)}")
            
            output_file = os.path.join(output_dir, f"{var_name}{time_info}.png")
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            plt.close()
            print(f"\nSpatial plot saved: {output_file}")
            
            # Plot time series if temporal data exists
            if 'time' in data.dims and data.sizes['time'] > 1:
                spatial_dims = [dim for dim in data.dims if dim not in ['time']]
                spatial_avg = data.mean(dim=spatial_dims)
                
                fig, ax = plt.subplots(figsize=(12, 6))
                spatial_avg.plot(ax=ax)
                plt.title(f"{var_name} - Spatial Average Time Series")
                plt.grid(True, alpha=0.3)
                
                ts_output_file = os.path.join(output_dir, f"{var_name}_timeseries.png")
                plt.savefig(ts_output_file, dpi=150, bbox_inches='tight')
                plt.close()
                print(f"Time series plot saved: {ts_output_file}")
                
        except ImportError:
            print("\nMatplotlib not available - skipping plots")
        except Exception as e:
            print(f"\nError creating plots: {e}")
        
        # Export simple CSV if temporal data
        if 'time' in data.dims and data.sizes['time'] > 1:
            try:
                spatial_dims = [dim for dim in data.dims if dim not in ['time']]
                spatial_avg = data.mean(dim=spatial_dims)
                
                # Create simple CSV data
                csv_data = []
                for i in range(data.sizes['time']):
                    time_val = str(ds.time.values[i])
                    avg_val = float(spatial_avg.isel(time=i).values)
                    csv_data.append(f"{time_val},{avg_val}")
                
                csv_file = os.path.join("plots", f"{var_name}_timeseries.csv")
                with open(csv_file, 'w') as f:
                    f.write("time,spatial_average\n")
                    f.write("\n".join(csv_data))
                
                print(f"Time series CSV saved: {csv_file}")
                
            except Exception as e:
                print(f"Error saving CSV: {e}")
        
        ds.close()
        
    except Exception as e:
        print(f"Error analyzing file: {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python visualize.py <path_to_netcdf_files>")
        print("Example: python visualize.py /path/to/merged/files")
        return 1
    
    netcdf_path = sys.argv[1]
    
    if not os.path.exists(netcdf_path):
        print(f"Error: Path {netcdf_path} does not exist")
        return 1
    
    # Get all NetCDF files
    nc_files = get_netcdf_files(netcdf_path)
    
    if not nc_files:
        print(f"No NetCDF files found in {netcdf_path}")
        return 1
    
    print(f"Found {len(nc_files)} NetCDF files in {netcdf_path}")
    
    # Analyze each file
    for file_path in sorted(nc_files):
        analyze_file(file_path)
    
    print(f"\n{'='*60}")
    print("Analysis complete!")
    if os.path.exists("plots"):
        print("Check the 'plots' directory for visualizations and CSV exports")
    
    return 0

if __name__ == "__main__":
    exit(main())
