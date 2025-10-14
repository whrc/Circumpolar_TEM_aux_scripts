#!/usr/bin/env python3
"""
Generic script to plot any variable from NetCDF files with interactive time slider and animation
Author: Generated for EJafarov
Usage: python plot_netcdf.py <file_path> [variable_name]

Features:
- Interactive time slider for manual navigation
- Play/Pause button for automatic animation
- Speed control slider (50-1000ms intervals)
- Auto-detection of main variables
- Statistics display (min, max, mean, valid pixels)
- Smart coordinate handling and cropping
"""

import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from matplotlib.widgets import Slider, Button
import sys
import argparse
import matplotlib.animation as animation

def detect_main_variable(ds):
    """
    Automatically detect the main data variable to plot
    
    Parameters:
    ds (xarray.Dataset): The dataset
    
    Returns:
    str: Name of the main variable to plot
    """
    # Skip coordinate variables and common metadata variables
    skip_vars = {'lambert_azimuthal_equal_area', 'crs', 'spatial_ref', 'time_bnds'}
    
    # Get data variables (not coordinates)
    data_vars = [var for var in ds.data_vars if var not in skip_vars]
    
    if not data_vars:
        raise ValueError("No suitable data variables found in the dataset")
    
    # If only one data variable, use it
    if len(data_vars) == 1:
        return data_vars[0]
    
    # Prefer variables with time dimension and 2+ spatial dimensions
    time_vars = []
    for var in data_vars:
        var_dims = ds[var].dims
        if 'time' in var_dims and len(var_dims) >= 3:  # time + 2 spatial dims
            time_vars.append(var)
    
    if time_vars:
        return time_vars[0]  # Return first suitable time-varying variable
    
    # Fallback to first data variable
    return data_vars[0]

def plot_netcdf_interactive(file_path, variable_name=None):
    """
    Plot any NetCDF variable with interactive time slider
    
    Parameters:
    file_path (str): Path to the NetCDF file
    variable_name (str, optional): Specific variable to plot. If None, auto-detect.
    """
    
    # Check if file exists
    if not Path(file_path).exists():
        print(f"Error: File {file_path} does not exist")
        return
    
    try:
        # Open the NetCDF file
        print(f"Opening file: {file_path}")
        ds = xr.open_dataset(file_path)
        
        # Print basic info about the dataset
        print(f"Dataset dimensions: {dict(ds.dims)}")
        print(f"Dataset variables: {list(ds.data_vars)}")
        
        # Determine which variable to plot
        if variable_name is None:
            variable_name = detect_main_variable(ds)
            print(f"Auto-detected variable: {variable_name}")
        else:
            if variable_name not in ds.variables:
                print(f"Error: Variable '{variable_name}' not found in dataset")
                print(f"Available variables: {list(ds.data_vars)}")
                return
        
        # Get the variable
        var_data = ds[variable_name]
        print(f"{variable_name} shape: {var_data.shape}")
        print(f"{variable_name} dimensions: {var_data.dims}")
        
        # Check if we can create a time slider
        has_time = 'time' in var_data.dims
        if has_time:
            time_size = var_data.sizes['time']
            print(f"Time dimension size: {time_size}")
            
            # Determine start year based on time dimension size
            if time_size == 124:
                start_year = 1901
                print(f"Using start year: {start_year} (124 time steps)")
            elif time_size == 76:
                start_year = 2024
                print(f"Using start year: {start_year} (76 time steps)")
            else:
                # Default fallback for other sizes
                start_year = 2024
                print(f"Using default start year: {start_year} ({time_size} time steps)")
        else:
            print("No time dimension found - will create static plot")
            start_year = None
        
        # Create the figure with subplots
        if has_time:
            fig, (ax_main, ax_timeseries) = plt.subplots(2, 1, figsize=(12, 12), 
                                                        gridspec_kw={'height_ratios': [3, 1]})
            plt.subplots_adjust(bottom=0.2, hspace=0.3)
        else:
            fig, ax_main = plt.subplots(figsize=(12, 10))
            ax_timeseries = None
        
        # Function to apply rotations (optional - can be disabled)
        def apply_rotations(data):
            if len(data.dims) == 2:
                # Rotate counterclockwise 90 degrees twice (180 degrees total)
                data = data.transpose().isel(x=slice(None, None, -1))
                data = data.transpose().isel(y=slice(None, None, -1))
            return data
        
        # Determine initial data to plot
        if has_time:
            initial_time = 0
            var_t = var_data.isel(time=initial_time)
        else:
            var_t = var_data
            initial_time = None
        
        # Apply rotations if data is 2D
        if len(var_t.dims) == 2:
            var_t = apply_rotations(var_t)
        
        # Create initial plot
        im = var_t.plot(ax=ax_main, cmap='viridis', add_colorbar=True)
        
        # Set plot properties (auto-detect limits or use defaults)
        if len(var_t.dims) == 2:
            # Try to set reasonable limits, fallback to cropped view if coordinates are indices
            x_coord_name = var_t.dims[1]
            y_coord_name = var_t.dims[0]
            x_coords = var_t.coords[x_coord_name]
            y_coords = var_t.coords[y_coord_name]
            
            # Check if coordinates look like indices (0, 1, 2, ...) or real coordinates
            if (x_coords.max() - x_coords.min()) > 1000:  # Likely real coordinates
                ax_main.set_xlim(x_coords.min().values, x_coords.max().values)
                ax_main.set_ylim(y_coords.min().values, y_coords.max().values)
            else:  # Likely indices, use cropped view
                ax_main.set_xlim(150, min(600, x_coords.max().values))
                ax_main.set_ylim(100, min(650, y_coords.max().values))
        
        ax_main.grid(True, alpha=0.3)
        ax_main.set_xlabel('X coordinate')
        ax_main.set_ylabel('Y coordinate')
        
        # Initial title and stats
        if has_time:
            # Calculate year using dynamic start year
            year = start_year + initial_time
            title = f'{variable_name} - Year: {year}\nFile: {file_path}'
        else:
            title = f'{variable_name}\nFile: {file_path}'
        ax_main.set_title(title)
        
        # Create statistics text box
        stats_text = ax_main.text(0.02, 0.98, '', transform=ax_main.transAxes, 
                                 verticalalignment='top', 
                                 bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # Function to update statistics
        def update_stats(data):
            data_values = data.values
            valid_data = data_values[~np.isnan(data_values)]
            if len(valid_data) > 0:
                stats_str = f'Min: {valid_data.min():.2f}\n'
                stats_str += f'Max: {valid_data.max():.2f}\n'
                stats_str += f'Mean: {valid_data.mean():.2f}\n'
                stats_str += f'Valid pixels: {len(valid_data)}'
                stats_text.set_text(stats_str)
        
        # Update initial stats
        update_stats(var_t)
        
        # Create time series plot if time dimension exists
        if has_time:
            # Calculate spatial mean for all time steps
            spatial_means = []
            years = []
            for t in range(time_size):
                data_t = var_data.isel(time=t)
                if len(data_t.dims) == 2:
                    data_t = apply_rotations(data_t)
                # Calculate spatial mean (ignoring NaN values)
                mean_val = data_t.mean(skipna=True).values
                spatial_means.append(mean_val)
                years.append(start_year + t)
            
            # Plot time series
            line, = ax_timeseries.plot(years, spatial_means, 'b-', linewidth=2)
            ax_timeseries.set_xlabel('Year')
            ax_timeseries.set_ylabel(f'Spatial Mean {variable_name}')
            ax_timeseries.grid(True, alpha=0.3)
            ax_timeseries.set_title(f'Time Evolution of Spatially Averaged {variable_name}')
            
            # Add vertical line to show current time
            current_year = start_year + initial_time
            vline = ax_timeseries.axvline(x=current_year, color='red', linestyle='--', linewidth=2)
            
            # Store for updates
            timeseries_data = {'line': line, 'vline': vline, 'years': years, 'means': spatial_means}
        
        # Create slider and animation controls only if there's a time dimension
        if has_time:
            # Adjust layout for controls
            plt.subplots_adjust(bottom=0.2)
            
            # Create slider
            ax_slider = plt.axes([0.1, 0.1, 0.65, 0.03])
            slider = Slider(ax_slider, 'Time Index', 0, time_size-1, 
                           valinit=initial_time, valfmt='%d')
            
            # Create play/pause button
            ax_button = plt.axes([0.8, 0.1, 0.08, 0.04])
            button = Button(ax_button, 'Play')
            
            # Create speed control
            ax_speed = plt.axes([0.1, 0.05, 0.3, 0.03])
            speed_slider = Slider(ax_speed, 'Speed (ms)', 50, 1000, 
                                valinit=200, valfmt='%d')
            
            # Animation state
            animation_state = {'is_playing': False, 'anim': None}
            
            # Update function for slider
            def update_plot(time_idx):
                if isinstance(time_idx, (int, float)):
                    time_idx = int(time_idx)
                else:
                    time_idx = int(slider.val)
                
                # Get new data
                var_new = var_data.isel(time=time_idx)
                if len(var_new.dims) == 2:
                    var_new = apply_rotations(var_new)
                
                # Clear and replot
                ax_main.clear()
                var_new.plot(ax=ax_main, cmap='viridis', add_colorbar=False)
                
                # Restore plot properties
                if len(var_new.dims) == 2:
                    x_coord_name = var_new.dims[1]
                    y_coord_name = var_new.dims[0]
                    x_coords = var_new.coords[x_coord_name]
                    y_coords = var_new.coords[y_coord_name]
                    
                    if (x_coords.max() - x_coords.min()) > 1000:
                        ax_main.set_xlim(x_coords.min().values, x_coords.max().values)
                        ax_main.set_ylim(y_coords.min().values, y_coords.max().values)
                    else:
                        ax_main.set_xlim(150, min(600, x_coords.max().values))
                        ax_main.set_ylim(100, min(650, y_coords.max().values))
                
                ax_main.grid(True, alpha=0.3)
                ax_main.set_xlabel('X coordinate')
                ax_main.set_ylabel('Y coordinate')
                
                # Update title with dynamic start year
                year = start_year + time_idx
                ax_main.set_title(f'{variable_name} - Year: {year}\nFile: {file_path}')
                
                # Update time series vertical line
                if has_time:
                    timeseries_data['vline'].set_xdata([year, year])
                
                # Update statistics
                update_stats(var_new)
                
                # Update slider position if called from animation
                if slider.val != time_idx:
                    slider.set_val(time_idx)
                
                # Redraw
                fig.canvas.draw()
            
            # Animation function
            def animate(frame):
                current_time = int(slider.val)
                next_time = (current_time + 1) % time_size
                update_plot(next_time)
                return []
            
            # Play/Pause button callback
            def toggle_animation(event):
                if animation_state['is_playing']:
                    # Stop animation
                    if animation_state['anim']:
                        animation_state['anim'].event_source.stop()
                    animation_state['is_playing'] = False
                    button.label.set_text('Play')
                else:
                    # Start animation
                    interval = int(speed_slider.val)
                    animation_state['anim'] = animation.FuncAnimation(
                        fig, animate, interval=interval, blit=False, repeat=True)
                    animation_state['is_playing'] = True
                    button.label.set_text('Pause')
                fig.canvas.draw()
            
            # Speed change callback
            def update_speed(val):
                if animation_state['is_playing'] and animation_state['anim']:
                    # Restart animation with new speed
                    animation_state['anim'].event_source.stop()
                    interval = int(speed_slider.val)
                    animation_state['anim'] = animation.FuncAnimation(
                        fig, animate, interval=interval, blit=False, repeat=True)
            
            # Connect callbacks
            slider.on_changed(lambda val: update_plot(val) if not animation_state['is_playing'] else None)
            button.on_clicked(toggle_animation)
            speed_slider.on_changed(update_speed)
        
        # Show the interactive plot
        plt.show()
        
        # Close the dataset when done
        ds.close()
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(
        description='Plot NetCDF variables with interactive time slider',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python plot_netcdf.py data.nc                    # Auto-detect variable
  python plot_netcdf.py data.nc VEGNTOT           # Plot specific variable
  python plot_netcdf.py data.nc --list            # List available variables
        """
    )
    
    parser.add_argument('file_path', help='Path to NetCDF file')
    parser.add_argument('variable', nargs='?', default=None, 
                       help='Variable name to plot (optional, will auto-detect if not provided)')
    parser.add_argument('--list', action='store_true', 
                       help='List available variables and exit')
    
    args = parser.parse_args()
    
    # Handle --list option
    if args.list:
        try:
            ds = xr.open_dataset(args.file_path)
            print(f"Available variables in {args.file_path}:")
            for var in ds.data_vars:
                var_info = ds[var]
                print(f"  {var}: {var_info.dims} - {var_info.shape}")
                if hasattr(var_info, 'long_name'):
                    print(f"    Description: {var_info.long_name}")
                if hasattr(var_info, 'units'):
                    print(f"    Units: {var_info.units}")
                print()
            ds.close()
        except Exception as e:
            print(f"Error reading file: {e}")
        return
    
    # Plot the data
    plot_netcdf_interactive(args.file_path, args.variable)

if __name__ == "__main__":
    main()
