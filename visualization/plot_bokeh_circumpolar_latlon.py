#!/usr/bin/env python3
"""
CIRCUMPOLAR Interactive Bokeh NetCDF plotter with Lat/Lon grid
Usage: 
  python plot_bokeh_circumpolar_latlon.py <file_path> [variable_name]
  python plot_bokeh_circumpolar_latlon.py --compare <file1> <file2>

Features:
- Correct circumpolar projection display (flipped along X-axis)
- Latitude/Longitude grid lines on map (no axis ticks)
- Fast image-based rendering (10-100x faster!)
- Interactive zoom and pan
- Time slider and animation
- 2:1 aspect ratio (width:height)
- Comparison mode with discrete blue-white-red colormap (6 colors)
- Side-by-side comparison with shared color scale
"""

import xarray as xr
import numpy as np
from pathlib import Path
import argparse
from bokeh.plotting import figure
from bokeh.layouts import column, row
from bokeh.models import (
    ColumnDataSource, 
    Slider, 
    Button, 
    Div, 
    ColorBar, 
    LinearColorMapper,
    Label,
    Legend,
)
from bokeh.palettes import Viridis256
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
import warnings
warnings.filterwarnings('ignore')

def create_discrete_bluewhitered_palette(n_colors=6):
    """
    Create a discrete blue-white-red color palette
    
    Parameters:
    -----------
    n_colors : int
        Number of discrete colors (default: 6)
    
    Returns:
    --------
    list : List of hex color strings
    """
    # Create a diverging colormap from blue to white to red
    colors_list = []
    n_half = n_colors // 2
    
    # Blue to white
    for i in range(n_half):
        ratio = (i + 1) / (n_half + 1)
        r = int(255 * ratio + 0 * (1 - ratio))
        g = int(255 * ratio + 0 * (1 - ratio))
        b = 255
        colors_list.append(f'#{r:02x}{g:02x}{b:02x}')
    
    # White to red
    for i in range(n_colors - n_half):
        ratio = (i + 1) / (n_colors - n_half + 1)
        r = 255
        g = int(255 * (1 - ratio))
        b = int(255 * (1 - ratio))
        colors_list.append(f'#{r:02x}{g:02x}{b:02x}')
    
    return colors_list


def detect_main_variable(ds):
    """Automatically detect the main data variable to plot"""
    skip_vars = {'lambert_azimuthal_equal_area', 'crs', 'spatial_ref', 'time_bnds'}
    data_vars = [var for var in ds.data_vars if var not in skip_vars]
    
    if not data_vars:
        raise ValueError("No suitable data variables found in the dataset")
    
    if len(data_vars) == 1:
        return data_vars[0]
    
    time_vars = []
    for var in data_vars:
        var_dims = ds[var].dims
        if 'time' in var_dims and len(var_dims) >= 3:
            time_vars.append(var)
    
    if time_vars:
        return time_vars[0]
    
    return data_vars[0]


def calculate_nee(gpp_file=None, reco_file=None, output_file=None):
    """
    Calculate NEE (Net Ecosystem Exchange) as RECO - GPP
    
    Parameters:
    -----------
    gpp_file : str, optional
        Path to GPP NetCDF file. Defaults to ../Alaska/merged/GPP_ssp5_8_5_mri_esm2_0_sc_yearly.nc
    reco_file : str, optional
        Path to RECO NetCDF file. Defaults to ../Alaska/merged/RECO_ssp5_8_5_mri_esm2_0_sc_yearly.nc
    output_file : str, optional
        Path to save NEE NetCDF file. If None, saves to ../Alaska/merged/NEE_ssp5_8_5_mri_esm2_0_sc_yearly.nc
    
    Returns:
    --------
    str : Path to the created NEE file
    """
    from pathlib import Path
    
    # Set default paths
    if gpp_file is None:
        gpp_file = Path(__file__).parent / "../Alaska/merged/GPP_ssp5_8_5_mri_esm2_0_sc_yearly.nc"
    if reco_file is None:
        reco_file = Path(__file__).parent / "../Alaska/merged/RECO_ssp5_8_5_mri_esm2_0_sc_yearly.nc"
    if output_file is None:
        output_file = Path(__file__).parent / "../Alaska/merged/NEE_ssp5_8_5_mri_esm2_0_sc_yearly.nc"
    
    # Convert to Path objects
    gpp_file = Path(gpp_file)
    reco_file = Path(reco_file)
    output_file = Path(output_file)
    
    print(f"\n{'='*60}")
    print(f"Calculating NEE (Net Ecosystem Exchange)")
    print(f"{'='*60}")
    print(f"Formula: NEE = RECO - GPP")
    print(f"\nInput files:")
    print(f"  GPP:  {gpp_file}")
    print(f"  RECO: {reco_file}")
    print(f"Output file:")
    print(f"  NEE:  {output_file}")
    
    # Check if files exist
    if not gpp_file.exists():
        raise FileNotFoundError(f"GPP file not found: {gpp_file}")
    if not reco_file.exists():
        raise FileNotFoundError(f"RECO file not found: {reco_file}")
    
    # Load datasets
    print("\nLoading GPP dataset...")
    ds_gpp = xr.open_dataset(gpp_file)
    print(f"  Variables: {list(ds_gpp.data_vars)}")
    print(f"  Dimensions: {dict(ds_gpp.dims)}")
    
    print("\nLoading RECO dataset...")
    ds_reco = xr.open_dataset(reco_file)
    print(f"  Variables: {list(ds_reco.data_vars)}")
    print(f"  Dimensions: {dict(ds_reco.dims)}")
    
    # Get variable names (assume first data variable is the main one)
    gpp_var = detect_main_variable(ds_gpp)
    reco_var = detect_main_variable(ds_reco)
    
    print(f"\nUsing variables:")
    print(f"  GPP variable:  {gpp_var}")
    print(f"  RECO variable: {reco_var}")
    
    # Get the data arrays
    gpp_data = ds_gpp[gpp_var]
    reco_data = ds_reco[reco_var]
    
    # Check dimensions match
    if gpp_data.shape != reco_data.shape:
        raise ValueError(f"Shape mismatch: GPP {gpp_data.shape} != RECO {reco_data.shape}")
    
    print(f"\nData shape: {gpp_data.shape}")
    print(f"Dimensions: {gpp_data.dims}")
    
    # Calculate NEE = GPP - RECO
    print("\nCalculating NEE = RECO - GPP...")
    nee_data = reco_data - gpp_data
    
    # Create new dataset with NEE
    ds_nee = xr.Dataset()
    ds_nee['NEE'] = nee_data
    
    # Copy coordinates and attributes from GPP dataset
    for coord in ds_gpp.coords:
        if coord not in ds_nee.coords:
            ds_nee.coords[coord] = ds_gpp.coords[coord]
    
    # Copy grid mapping variable if it exists
    if 'lambert_azimuthal_equal_area' in ds_gpp:
        ds_nee['lambert_azimuthal_equal_area'] = ds_gpp['lambert_azimuthal_equal_area']
    
    # Set attributes for NEE variable
    ds_nee['NEE'].attrs['long_name'] = 'Net Ecosystem Exchange'
    ds_nee['NEE'].attrs['units'] = gpp_data.attrs.get('units', 'gC/m2/year')
    ds_nee['NEE'].attrs['description'] = 'Net Ecosystem Exchange calculated as RECO - GPP'
    ds_nee['NEE'].attrs['formula'] = 'NEE = RECO - GPP'
    
    # Copy global attributes
    ds_nee.attrs.update(ds_gpp.attrs)
    ds_nee.attrs['title'] = 'Net Ecosystem Exchange (NEE)'
    ds_nee.attrs['history'] = f'Created by calculating NEE = RECO - GPP from {reco_file.name} and {gpp_file.name}'
    
    # Calculate and print statistics
    print("\nNEE Statistics:")
    nee_values = nee_data.values
    valid_nee = nee_values[~np.isnan(nee_values)]
    if len(valid_nee) > 0:
        print(f"  Min:  {valid_nee.min():.4f}")
        print(f"  Max:  {valid_nee.max():.4f}")
        print(f"  Mean: {valid_nee.mean():.4f}")
        print(f"  Std:  {valid_nee.std():.4f}")
        print(f"  Valid pixels: {len(valid_nee):,}")
    
    # Save to file
    print(f"\nSaving NEE dataset to: {output_file}")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    ds_nee.to_netcdf(output_file)
    
    # Close datasets
    ds_gpp.close()
    ds_reco.close()
    ds_nee.close()
    
    print(f"\n✓ Successfully created NEE file!")
    print(f"{'='*60}\n")
    
    return str(output_file)


def laea_to_latlon(x, y, lat0=90.0, lon0=0.0, a=6378137.0, f=1/298.257223563):
    """
    Convert Lambert Azimuthal Equal Area coordinates to lat/lon
    Uses WGS84 ellipsoid parameters from the NetCDF file
    
    Parameters:
    x, y: LAEA coordinates in meters
    lat0, lon0: Center of projection (90.0, 0.0 for North Pole)
    a: Semi-major axis in meters (WGS84: 6378137.0)
    f: Flattening (WGS84: 1/298.257223563)
    
    Returns:
    lat, lon in degrees
    """
    # For polar aspect (lat0=90), use spherical approximation with authalic radius
    # The authalic radius gives equal-area property
    e2 = 2*f - f*f  # eccentricity squared
    e = np.sqrt(e2)
    
    # Authalic radius (equal-area sphere radius)
    q_p = (1 - e2) * (1/(1-e2) - (1/(2*e)) * np.log((1-e)/(1+e)))
    R_q = a * np.sqrt(q_p / 2)
    
    lat0_rad = np.radians(lat0)
    lon0_rad = np.radians(lon0)
    
    rho = np.sqrt(x**2 + y**2)
    c = 2 * np.arcsin(rho / (2 * R_q))
    
    # Avoid division by zero
    with np.errstate(divide='ignore', invalid='ignore'):
        lat = np.arcsin(np.cos(c) * np.sin(lat0_rad) + 
                       (y * np.sin(c) * np.cos(lat0_rad)) / rho)
        lon = lon0_rad + np.arctan2(x * np.sin(c), 
                                     rho * np.cos(lat0_rad) * np.cos(c) - 
                                     y * np.sin(lat0_rad) * np.sin(c))
    
    # Handle center point (rho=0)
    if np.isscalar(rho):
        if rho == 0:
            lat = lat0_rad
            lon = lon0_rad
    else:
        lat = np.where(rho == 0, lat0_rad, lat)
        lon = np.where(rho == 0, lon0_rad, lon)
    
    return np.degrees(lat), np.degrees(lon)


def latlon_to_laea(lat, lon, lat0=90.0, lon0=0.0, a=6378137.0, f=1/298.257223563):
    """
    Convert lat/lon to Lambert Azimuthal Equal Area coordinates
    Uses WGS84 ellipsoid parameters from the NetCDF file
    
    Parameters:
    lat, lon: latitude and longitude in degrees
    lat0, lon0: Center of projection (90.0, 0.0 for North Pole)
    a: Semi-major axis in meters (WGS84: 6378137.0)
    f: Flattening (WGS84: 1/298.257223563)
    
    Returns:
    x, y in LAEA coordinates (meters)
    """
    # For polar aspect (lat0=90), use spherical approximation with authalic radius
    e2 = 2*f - f*f  # eccentricity squared
    e = np.sqrt(e2)
    
    # Authalic radius (equal-area sphere radius)
    q_p = (1 - e2) * (1/(1-e2) - (1/(2*e)) * np.log((1-e)/(1+e)))
    R_q = a * np.sqrt(q_p / 2)
    
    lat_rad = np.radians(lat)
    lon_rad = np.radians(lon)
    lat0_rad = np.radians(lat0)
    lon0_rad = np.radians(lon0)
    
    k = np.sqrt(2 / (1 + np.sin(lat0_rad) * np.sin(lat_rad) + 
                      np.cos(lat0_rad) * np.cos(lat_rad) * np.cos(lon_rad - lon0_rad)))
    
    x = R_q * k * np.cos(lat_rad) * np.sin(lon_rad - lon0_rad)
    y = R_q * k * (np.cos(lat0_rad) * np.sin(lat_rad) - 
                   np.sin(lat0_rad) * np.cos(lat_rad) * np.cos(lon_rad - lon0_rad))
    
    return x, y


def create_comparison_app(file_path1, file_path2, variable_name=None, colorbar_range=None, n_discrete_colors=None):
    """Create Bokeh application comparing two NetCDF files side by side
    
    Parameters:
    -----------
    file_path1 : str
        Path to first NetCDF file
    file_path2 : str
        Path to second NetCDF file
    variable_name : str, optional
        Variable name to plot (auto-detected if None)
    colorbar_range : tuple of (float, float), optional
        Custom colorbar range as (vmin, vmax). If None, calculated from data.
    n_discrete_colors : int, optional
        Number of discrete colors for colorbar. If None, use continuous colorbar.
    """
    
    if not Path(file_path1).exists():
        print(f"Error: File {file_path1} does not exist")
        return None
    if not Path(file_path2).exists():
        print(f"Error: File {file_path2} does not exist")
        return None
    
    try:
        print(f"Opening file 1: {file_path1}")
        ds1 = xr.open_dataset(file_path1)
        print(f"Opening file 2: {file_path2}")
        ds2 = xr.open_dataset(file_path2)
        
        # Determine which variable to plot
        if variable_name is None:
            variable_name = detect_main_variable(ds1)
            print(f"Auto-detected variable: {variable_name}")
        
        var_data1 = ds1[variable_name]
        var_data2 = ds2[variable_name]
        
        print(f"File 1 - {variable_name} shape: {var_data1.shape}")
        print(f"File 2 - {variable_name} shape: {var_data2.shape}")
        
        # Check time dimension
        has_time = 'time' in var_data1.dims
        if has_time:
            time_size = var_data1.sizes['time']
            if time_size == 124:
                start_year = 1901
            elif time_size == 76:
                start_year = 2024
            else:
                start_year = 2024
            print(f"Time dimension size: {time_size}, start year: {start_year}")
        else:
            start_year = None
            time_size = 1
        
        # Get coordinate information from first file
        if 'X' in ds1.coords and 'Y' in ds1.coords:
            x_coords = ds1.coords['X'].values
            y_coords = ds1.coords['Y'].values
        else:
            x_dim = [d for d in var_data1.dims if d in ['x', 'X']][0]
            y_dim = [d for d in var_data1.dims if d in ['y', 'Y']][0]
            x_coords = var_data1.coords[x_dim].values
            y_coords = var_data1.coords[y_dim].values
        
        x_min, x_max = float(x_coords.min()), float(x_coords.max())
        y_min, y_max = float(y_coords.min()), float(y_coords.max())
        dw = x_max - x_min
        dh = y_max - y_min
        
        # Calculate spatial means for both files
        print("\nPre-calculating spatial means for both files...")
        spatial_means1 = []
        spatial_means2 = []
        years = []
        if has_time:
            for t in range(time_size):
                mean1 = float(var_data1.isel(time=t).mean(skipna=True).values)
                mean2 = float(var_data2.isel(time=t).mean(skipna=True).values)
                spatial_means1.append(mean1)
                spatial_means2.append(mean2)
                years.append(start_year + t)
        
        def make_document(doc):
            """Create the Bokeh document for comparison"""
            
            state = {
                'current_time': 0,
                'is_playing': False,
                'callback_id': None
            }
            
            def get_current_data():
                """Get data for current time step from both files"""
                if has_time:
                    data1 = var_data1.isel(time=state['current_time'])
                    data2 = var_data2.isel(time=state['current_time'])
                else:
                    data1 = var_data1
                    data2 = var_data2
                return data1, data2
            
            # Get initial data
            initial_data1, initial_data2 = get_current_data()
            data_values1 = initial_data1.values
            data_values2 = initial_data2.values
            
            # Calculate shared color scale across both datasets
            if colorbar_range is not None:
                vmin, vmax = colorbar_range
                print(f"Using custom color scale: {vmin:.2f} to {vmax:.2f}")
            else:
                print("Calculating shared color scale...")
                all_valid = []
                if has_time:
                    for t in range(min(time_size, 10)):
                        sample1 = var_data1.isel(time=t).values
                        sample2 = var_data2.isel(time=t).values
                        valid1 = sample1[~np.isnan(sample1)]
                        valid2 = sample2[~np.isnan(sample2)]
                        if len(valid1) > 0:
                            all_valid.extend(valid1[:5000])
                        if len(valid2) > 0:
                            all_valid.extend(valid2[:5000])
                else:
                    valid1 = data_values1[~np.isnan(data_values1)]
                    valid2 = data_values2[~np.isnan(data_values2)]
                    all_valid.extend(valid1[:10000])
                    all_valid.extend(valid2[:10000])
                
                if len(all_valid) > 0:
                    vmin = float(np.percentile(all_valid, 2))
                    vmax = float(np.percentile(all_valid, 98))
                else:
                    vmin, vmax = 0, 1
                
                print(f"Shared color scale: {vmin:.2f} to {vmax:.2f}")
            
            # Select color palette based on discrete option
            if n_discrete_colors is not None:
                palette = create_discrete_bluewhitered_palette(n_colors=n_discrete_colors)
                print(f"Using discrete blue-white-red palette with {n_discrete_colors} colors")
            else:
                palette = Viridis256
                print(f"Using continuous Viridis palette")
            
            # Create color mapper
            color_mapper = LinearColorMapper(
                palette=palette, 
                low=vmin, 
                high=vmax,
                nan_color='white'
            )
            
            # Prepare image data for both files
            img_data1 = [np.flipud(np.fliplr(data_values1))]
            img_data2 = [np.flipud(np.fliplr(data_values2))]
            
            image_source1 = ColumnDataSource(data={
                'image': img_data1,
                'x': [x_min],
                'y': [y_min],
                'dw': [dw],
                'dh': [dh]
            })
            
            image_source2 = ColumnDataSource(data={
                'image': img_data2,
                'x': [x_min],
                'y': [y_min],
                'dw': [dw],
                'dh': [dh]
            })
            
            # Create plots with smaller dimensions for side-by-side display
            plot_height = 600
            plot_width = 700
            
            # Plot 1
            p1 = figure(
                width=plot_width,
                height=plot_height,
                title=f"{Path(file_path1).name} - Year: {start_year if start_year else 'N/A'}",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                toolbar_location="right",
                x_range=(x_min, x_max),
                y_range=(y_min, y_max)
            )
            p1.xaxis.visible = False
            p1.yaxis.visible = False
            
            p1.image(
                image='image',
                x='x',
                y='y',
                dw='dw',
                dh='dh',
                source=image_source1,
                color_mapper=color_mapper
            )
            
            # Add colorbar to first plot
            color_bar1 = ColorBar(
                color_mapper=color_mapper,
                width=15,
                location=(0, 0),
                title=variable_name
            )
            p1.add_layout(color_bar1, 'right')
            
            # Plot 2
            p2 = figure(
                width=plot_width,
                height=plot_height,
                title=f"{Path(file_path2).name} - Year: {start_year if start_year else 'N/A'}",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                toolbar_location="right",
                x_range=p1.x_range,  # Link to first plot
                y_range=p1.y_range   # Link to first plot
            )
            p2.xaxis.visible = False
            p2.yaxis.visible = False
            
            p2.image(
                image='image',
                x='x',
                y='y',
                dw='dw',
                dh='dh',
                source=image_source2,
                color_mapper=color_mapper
            )
            
            # Add colorbar to second plot
            color_bar2 = ColorBar(
                color_mapper=color_mapper,
                width=15,
                location=(0, 0),
                title=variable_name
            )
            p2.add_layout(color_bar2, 'right')
            
            # Create time series plot with both datasets
            if has_time:
                ts_source1 = ColumnDataSource(data={
                    'years': years,
                    'means': spatial_means1
                })
                
                ts_source2 = ColumnDataSource(data={
                    'years': years,
                    'means': spatial_means2
                })
                
                current_year_source1 = ColumnDataSource(data={
                    'x': [years[0]],
                    'y': [spatial_means1[0]]
                })
                
                current_year_source2 = ColumnDataSource(data={
                    'x': [years[0]],
                    'y': [spatial_means2[0]]
                })
                
                p_ts = figure(
                    width=plot_width * 2,
                    height=250,
                    title=f'Spatial Mean {variable_name} Over Time - Comparison',
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    x_axis_label='Year',
                    y_axis_label=f'Mean {variable_name}'
                )
                
                # Plot both time series
                line1 = p_ts.line('years', 'means', source=ts_source1, line_width=2, color='navy')
                line2 = p_ts.line('years', 'means', source=ts_source2, line_width=2, color='darkred')
                p_ts.circle('x', 'y', source=current_year_source1, size=10, color='blue')
                p_ts.circle('x', 'y', source=current_year_source2, size=10, color='red')
                
                # Create legend outside the plot
                legend = Legend(items=[
                    (Path(file_path1).name, [line1]),
                    (Path(file_path2).name, [line2])
                ], location="center", click_policy="hide")
                p_ts.add_layout(legend, 'right')
            
            # Update function
            def update_plot():
                data1, data2 = get_current_data()
                data_values1 = data1.values
                data_values2 = data2.values
                
                # Update images
                image_source1.data['image'] = [np.flipud(np.fliplr(data_values1))]
                image_source2.data['image'] = [np.flipud(np.fliplr(data_values2))]
                
                # Update titles
                if has_time:
                    year = start_year + state['current_time']
                    p1.title.text = f"{Path(file_path1).name} - Year: {year}"
                    p2.title.text = f"{Path(file_path2).name} - Year: {year}"
                
                # Update time series markers
                if has_time:
                    year = start_year + state['current_time']
                    current_year_source1.data = {
                        'x': [year],
                        'y': [spatial_means1[state['current_time']]]
                    }
                    current_year_source2.data = {
                        'x': [year],
                        'y': [spatial_means2[state['current_time']]]
                    }
            
            # Time controls
            if has_time:
                time_slider = Slider(
                    start=0,
                    end=time_size - 1,
                    value=0,
                    step=1,
                    title="Time Index"
                )
                
                def slider_update(attr, old, new):
                    if not state['is_playing']:
                        state['current_time'] = int(time_slider.value)
                        update_plot()
                
                time_slider.on_change('value', slider_update)
                
                play_button = Button(label="▶ Play", width=100, button_type="success")
                
                def toggle_play():
                    if state['is_playing']:
                        state['is_playing'] = False
                        play_button.label = "▶ Play"
                        play_button.button_type = "success"
                        if state['callback_id']:
                            doc.remove_periodic_callback(state['callback_id'])
                            state['callback_id'] = None
                    else:
                        state['is_playing'] = True
                        play_button.label = "⏸ Pause"
                        play_button.button_type = "warning"
                        
                        def animate():
                            state['current_time'] = (state['current_time'] + 1) % time_size
                            time_slider.value = state['current_time']
                            update_plot()
                        
                        speed_ms = int(speed_slider.value)
                        state['callback_id'] = doc.add_periodic_callback(animate, speed_ms)
                
                play_button.on_click(toggle_play)
                
                speed_slider = Slider(
                    start=50,
                    end=1000,
                    value=200,
                    step=50,
                    title="Animation Speed (ms)"
                )
                
                def speed_update(attr, old, new):
                    if state['is_playing']:
                        doc.remove_periodic_callback(state['callback_id'])
                        
                        def animate():
                            state['current_time'] = (state['current_time'] + 1) % time_size
                            time_slider.value = state['current_time']
                            update_plot()
                        
                        speed_ms = int(speed_slider.value)
                        state['callback_id'] = doc.add_periodic_callback(animate, speed_ms)
                
                speed_slider.on_change('value', speed_update)
                
                info_div = Div(text=f"""
                <div style="background-color: #e8f4f8; padding: 8px; border-radius: 5px; margin-bottom: 10px;">
                    <b>🔄 Comparison Mode:</b> Side-by-side with shared color scale<br>
                    <b>🎨 Colors:</b> Discrete blue-white-red palette (6 colors)<br>
                    <b>📊 Time Series:</b> Both datasets plotted together<br>
                    <b>🔗 Linked Views:</b> Zoom and pan synchronized
                </div>
                """, width=plot_width * 2)
                
                controls = row(play_button, time_slider, speed_slider)
                layout = column(
                    Div(text=f"<h2>Comparison: {Path(file_path1).name} vs {Path(file_path2).name}</h2>"),
                    info_div,
                    row(p1, p2),
                    p_ts,
                    controls
                )
            else:
                layout = column(
                    Div(text=f"<h2>Comparison: {Path(file_path1).name} vs {Path(file_path2).name}</h2>"),
                    row(p1, p2)
                )
            
            doc.add_root(layout)
            doc.title = f"Comparison - {variable_name}"
        
        return make_document
        
    except Exception as e:
        print(f"Error processing files: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_bokeh_app(file_path, variable_name=None, colorbar_range=None, n_discrete_colors=None):
    """Create Bokeh application with circumpolar projection and lat/lon grid
    
    Parameters:
    -----------
    file_path : str
        Path to NetCDF file
    variable_name : str, optional
        Variable name to plot (auto-detected if None)
    colorbar_range : tuple of (float, float), optional
        Custom colorbar range as (vmin, vmax). If None, calculated from data.
    n_discrete_colors : int, optional
        Number of discrete colors for colorbar. If None, use continuous colorbar.
    """
    
    if not Path(file_path).exists():
        print(f"Error: File {file_path} does not exist")
        return None
    
    try:
        print(f"Opening file: {file_path}")
        ds = xr.open_dataset(file_path)
        
        print(f"Dataset dimensions: {dict(ds.dims)}")
        print(f"Dataset variables: {list(ds.data_vars)}")
        
        # Determine which variable to plot
        if variable_name is None:
            variable_name = detect_main_variable(ds)
            print(f"Auto-detected variable: {variable_name}")
        else:
            if variable_name not in ds.variables:
                print(f"Error: Variable '{variable_name}' not found")
                return None
        
        var_data = ds[variable_name]
        print(f"{variable_name} shape: {var_data.shape}")
        print(f"{variable_name} dimensions: {var_data.dims}")
        
        # Check time dimension
        has_time = 'time' in var_data.dims
        if has_time:
            time_size = var_data.sizes['time']
            print(f"Time dimension size: {time_size}")
            
            if time_size == 124:
                start_year = 1901
            elif time_size == 76:
                start_year = 2024
            else:
                start_year = 2024
            print(f"Using start year: {start_year}")
        else:
            start_year = None
            time_size = 1
        
        # Get coordinate information
        print("\nProjection info:")
        if 'lambert_azimuthal_equal_area' in ds:
            laea = ds.lambert_azimuthal_equal_area
            print(f"  Projection: {laea.attrs.get('grid_mapping_name', 'N/A')}")
            print(f"  Center latitude: {laea.attrs.get('latitude_of_projection_origin', 'N/A')}")
            print(f"  Center longitude: {laea.attrs.get('longitude_of_projection_origin', 'N/A')}")
        
        # Get initial data
        if has_time:
            var_t = var_data.isel(time=0)
        else:
            var_t = var_data
        
        # Get X and Y coordinates
        if 'X' in ds.coords and 'Y' in ds.coords:
            x_coords = ds.coords['X'].values
            y_coords = ds.coords['Y'].values
            print(f"\nUsing projection coordinates:")
            print(f"  X range: {x_coords.min():.0f} to {x_coords.max():.0f} meters")
            print(f"  Y range: {y_coords.min():.0f} to {y_coords.max():.0f} meters")
        else:
            x_dim = [d for d in var_t.dims if d in ['x', 'X']][0]
            y_dim = [d for d in var_t.dims if d in ['y', 'Y']][0]
            x_coords = var_t.coords[x_dim].values
            y_coords = var_t.coords[y_dim].values
        
        # Get data extent
        x_min, x_max = float(x_coords.min()), float(x_coords.max())
        y_min, y_max = float(y_coords.min()), float(y_coords.max())
        dw = x_max - x_min
        dh = y_max - y_min
        
        print(f"\nData will be displayed in circumpolar projection with lat/lon grid")
        print(f"Image will be flipped along X-axis for correct orientation")
        print(f"Rendering {var_t.shape} array using fast image method")
        
        # Calculate lat/lon bounds
        corners_x = [x_min, x_max, x_min, x_max]
        corners_y = [y_min, y_min, y_max, y_max]
        corner_lats, corner_lons = laea_to_latlon(np.array(corners_x), np.array(corners_y))
        print(f"\nApproximate geographic bounds:")
        print(f"  Latitude: {corner_lats.min():.1f}°N to {corner_lats.max():.1f}°N")
        print(f"  Longitude: {corner_lons.min():.1f}°E to {corner_lons.max():.1f}°E")
        
        # Calculate spatial means for time series
        print("\nPre-calculating spatial means...")
        spatial_means = []
        years = []
        if has_time:
            for t in range(time_size):
                data_t = var_data.isel(time=t)
                mean_val = float(data_t.mean(skipna=True).values)
                spatial_means.append(mean_val)
                years.append(start_year + t)
        
        def make_document(doc):
            """Create the Bokeh document"""
            
            state = {
                'current_time': 0,
                'is_playing': False,
                'callback_id': None
            }
            
            def get_current_data():
                """Get data for current time step"""
                if has_time:
                    data = var_data.isel(time=state['current_time'])
                else:
                    data = var_data
                return data
            
            # Get initial data
            initial_data = get_current_data()
            data_values = initial_data.values
            
            # Calculate color scale
            if colorbar_range is not None:
                vmin, vmax = colorbar_range
                print(f"Using custom color scale: {vmin:.2f} to {vmax:.2f}")
            else:
                print("Calculating color scale...")
                all_valid = []
                if has_time:
                    for t in range(min(time_size, 10)):
                        sample = var_data.isel(time=t).values
                        valid = sample[~np.isnan(sample)]
                        if len(valid) > 0:
                            all_valid.extend(valid[:10000])
                else:
                    valid = data_values[~np.isnan(data_values)]
                    all_valid.extend(valid[:10000])
                
                if len(all_valid) > 0:
                    vmin = float(np.percentile(all_valid, 2))
                    vmax = float(np.percentile(all_valid, 98))
                else:
                    vmin, vmax = 0, 1
                
                print(f"Color scale: {vmin:.2f} to {vmax:.2f}")
            
            # Select color palette based on discrete option
            if n_discrete_colors is not None:
                palette = create_discrete_bluewhitered_palette(n_colors=n_discrete_colors)
                print(f"Using discrete blue-white-red palette with {n_discrete_colors} colors")
            else:
                palette = Viridis256
                print(f"Using continuous Viridis palette")
            
            # Create color mapper
            color_mapper = LinearColorMapper(
                palette=palette, 
                low=vmin, 
                high=vmax,
                nan_color='white'
            )
            
            # Prepare image data - FLIP ALONG X-AXIS (left-right)
            # Also flip Y-axis so north is up
            img_data = [np.flipud(np.fliplr(data_values))]
            
            image_source = ColumnDataSource(data={
                'image': img_data,
                'x': [x_min],
                'y': [y_min],
                'dw': [dw],
                'dh': [dh]
            })
            
            # Create main plot with 2:1 aspect ratio (width:height)
            plot_height = 700
            plot_width = 1400  # 2:1 ratio
            
            p = figure(
                width=plot_width,
                height=plot_height,
                title=f"{variable_name} - Circumpolar View - Year: {start_year if start_year else 'N/A'}",
                tools="pan,wheel_zoom,box_zoom,reset,save",
                active_scroll="wheel_zoom",
                toolbar_location="right",
                x_range=(x_min, x_max),
                y_range=(y_min, y_max)
            )
            
            # Hide axis ticks and labels
            p.xaxis.visible = False
            p.yaxis.visible = False
            
            # Use image glyph
            p.image(
                image='image',
                x='x',
                y='y',
                dw='dw',
                dh='dh',
                source=image_source,
                color_mapper=color_mapper
            )
            
            # Add latitude circles
            print("Adding lat/lon grid...")
            lat_circles = [50, 55, 60, 65, 70, 75, 80]
            lon_meridians = np.arange(-180, 180, 30)  # Every 30 degrees
            
            # Draw latitude circles
            for lat in lat_circles:
                lons = np.linspace(-180, 180, 100)
                lats = np.full_like(lons, lat)
                xs, ys = latlon_to_laea(lats, lons)
                p.line(xs, ys, line_color='gray', line_alpha=0.3, line_width=1, line_dash='dashed')
                
                # Add label
                x_label, y_label = latlon_to_laea(lat, 0)  # Label at 0° longitude
                label = Label(x=x_label, y=y_label, text=f'{lat}°N', 
                            text_font_size='8pt', text_color='gray', text_alpha=0.7)
                p.add_layout(label)
            
            # Draw longitude meridians
            for lon in lon_meridians:
                lats = np.linspace(40, 85, 50)
                lons = np.full_like(lats, lon)
                xs, ys = latlon_to_laea(lats, lons)
                p.line(xs, ys, line_color='gray', line_alpha=0.3, line_width=1, line_dash='dotted')
                
                # Add label at 50°N
                x_label, y_label = latlon_to_laea(50, lon)
                lon_text = f'{lon}°E' if lon >= 0 else f'{abs(lon)}°W'
                if lon == 0:
                    lon_text = '0°'
                elif lon == 180 or lon == -180:
                    lon_text = '180°'
                label = Label(x=x_label, y=y_label, text=lon_text,
                            text_font_size='8pt', text_color='gray', text_alpha=0.7)
                p.add_layout(label)
            
            # Add colorbar
            color_bar = ColorBar(
                color_mapper=color_mapper,
                width=15,
                location=(0, 0)
            )
            p.add_layout(color_bar, 'right')
            
            # Create time series plot
            if has_time:
                ts_source = ColumnDataSource(data={
                    'years': years,
                    'means': spatial_means
                })
                
                current_year_source = ColumnDataSource(data={
                    'x': [years[0]],
                    'y': [spatial_means[0]]
                })
                
                p_ts = figure(
                    width=plot_width,
                    height=200,
                    title=f'Spatial Mean {variable_name} Over Time',
                    tools="pan,wheel_zoom,box_zoom,reset,save",
                    x_axis_label='Year',
                    y_axis_label=f'Mean {variable_name}'
                )
                
                p_ts.line('years', 'means', source=ts_source, line_width=2, color='navy')
                p_ts.circle('x', 'y', source=current_year_source, size=10, color='red')
            
            # Statistics div
            def get_stats_html(data):
                data_values = data.values
                valid_data = data_values[~np.isnan(data_values)]
                if len(valid_data) > 0:
                    stats_html = f"""
                    <div style="background-color: #f0f0f0; padding: 10px; border-radius: 5px;">
                        <h3 style="margin-top: 0;">Statistics</h3>
                        <p><b>Min:</b> {valid_data.min():.2f}</p>
                        <p><b>Max:</b> {valid_data.max():.2f}</p>
                        <p><b>Mean:</b> {valid_data.mean():.2f}</p>
                        <p><b>Std:</b> {valid_data.std():.2f}</p>
                        <p><b>Valid pixels:</b> {len(valid_data):,}</p>
                    </div>
                    """
                else:
                    stats_html = "<p>No valid data</p>"
                return stats_html
            
            stats_div = Div(text=get_stats_html(initial_data), width=200)
            
            # Update function
            def update_plot():
                data = get_current_data()
                data_values = data.values
                
                # Update image - FLIP BOTH AXES
                image_source.data['image'] = [np.flipud(np.fliplr(data_values))]
                
                # Update title
                if has_time:
                    year = start_year + state['current_time']
                    p.title.text = f"{variable_name} - Circumpolar View - Year: {year}"
                
                # Update statistics
                stats_div.text = get_stats_html(data)
                
                # Update time series marker
                if has_time:
                    year = start_year + state['current_time']
                    current_year_source.data = {
                        'x': [year],
                        'y': [spatial_means[state['current_time']]]
                    }
            
            # Time controls
            if has_time:
                time_slider = Slider(
                    start=0,
                    end=time_size - 1,
                    value=0,
                    step=1,
                    title="Time Index"
                )
                
                def slider_update(attr, old, new):
                    if not state['is_playing']:
                        state['current_time'] = int(time_slider.value)
                        update_plot()
                
                time_slider.on_change('value', slider_update)
                
                play_button = Button(label="▶ Play", width=100, button_type="success")
                
                def toggle_play():
                    if state['is_playing']:
                        state['is_playing'] = False
                        play_button.label = "▶ Play"
                        play_button.button_type = "success"
                        if state['callback_id']:
                            doc.remove_periodic_callback(state['callback_id'])
                            state['callback_id'] = None
                    else:
                        state['is_playing'] = True
                        play_button.label = "⏸ Pause"
                        play_button.button_type = "warning"
                        
                        def animate():
                            state['current_time'] = (state['current_time'] + 1) % time_size
                            time_slider.value = state['current_time']
                            update_plot()
                        
                        speed_ms = int(speed_slider.value)
                        state['callback_id'] = doc.add_periodic_callback(animate, speed_ms)
                
                play_button.on_click(toggle_play)
                
                speed_slider = Slider(
                    start=50,
                    end=1000,
                    value=200,
                    step=50,
                    title="Animation Speed (ms)"
                )
                
                def speed_update(attr, old, new):
                    if state['is_playing']:
                        doc.remove_periodic_callback(state['callback_id'])
                        
                        def animate():
                            state['current_time'] = (state['current_time'] + 1) % time_size
                            time_slider.value = state['current_time']
                            update_plot()
                        
                        speed_ms = int(speed_slider.value)
                        state['callback_id'] = doc.add_periodic_callback(animate, speed_ms)
                
                speed_slider.on_change('value', speed_update)
                
                info_div = Div(text=f"""
                <div style="background-color: #e8f4f8; padding: 8px; border-radius: 5px; margin-bottom: 10px;">
                    <b>🌍 Circumpolar Projection:</b> Lambert Azimuthal Equal Area (North Pole centered)<br>
                    <b>📐 Grid:</b> Latitude circles (dashed) and longitude meridians (dotted)<br>
                    <b>⚡ Performance:</b> Fast rendering ({data_values.shape[0]} × {data_values.shape[1]} pixels)<br>
                    <b>🔄 Orientation:</b> Image flipped for correct display<br>
                    <b>📏 Aspect Ratio:</b> 2:1 (width:height), axis ticks hidden
                </div>
                """, width=plot_width + 200)
                
                controls = row(play_button, time_slider, speed_slider)
                layout = column(
                    Div(text=f"<h2>Circumpolar NetCDF Viewer with Lat/Lon Grid: {Path(file_path).name}</h2>"),
                    info_div,
                    row(p, stats_div),
                    p_ts,
                    controls
                )
            else:
                layout = column(
                    Div(text=f"<h2>Circumpolar NetCDF Viewer: {Path(file_path).name}</h2>"),
                    row(p, stats_div)
                )
            
            doc.add_root(layout)
            doc.title = f"Circumpolar View - {variable_name}"
        
        return make_document
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(
        description='Circumpolar Bokeh viewer with lat/lon grid - Flipped for correct orientation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Visualize a NetCDF file
  python plot_bokeh_circumpolar_latlon.py ../Alaska/merged/ALD_ssp5_8_5_mri_esm2_0_yearly.nc
  
  # Compare two NetCDF files side-by-side
  python plot_bokeh_circumpolar_latlon.py --compare pathtofile/nee1.nc pathtofile/nee2.nc
  
  # Compare with discrete colorbar (6 colors)
  python plot_bokeh_circumpolar_latlon.py --compare file1.nc file2.nc --discrete 6
  
  # Compare with custom colorbar range and discrete colors
  python plot_bokeh_circumpolar_latlon.py --compare file1.nc file2.nc --cb -200 100 --discrete 6
  
  # Single file with custom colorbar range
  python plot_bokeh_circumpolar_latlon.py data.nc --cb 0 500
  
  # List variables in a file
  python plot_bokeh_circumpolar_latlon.py data.nc --list
  
  # Calculate NEE from GPP and RECO
  python plot_bokeh_circumpolar_latlon.py --nee
  
  # Calculate NEE with custom files
  python plot_bokeh_circumpolar_latlon.py --nee --gpp-file path/to/gpp.nc --reco-file path/to/reco.nc

Features:
- Proper circumpolar projection (Lambert Azimuthal Equal Area)
- Image flipped along X-axis for correct orientation
- Latitude/longitude grid overlay (no axis ticks)
- Fast image rendering
- 2:1 aspect ratio (width:height)
- NEE calculation (Net Ecosystem Exchange = RECO - GPP)
- Comparison mode with optional discrete blue-white-red colormap
- Custom colorbar range specification
- Continuous (default) or discrete colorbars
        """
    )
    
    parser.add_argument('file_path', nargs='?', default=None, help='Path to NetCDF file')
    parser.add_argument('variable', nargs='?', default=None,
                       help='Variable name to plot')
    parser.add_argument('--compare', nargs=2, metavar=('FILE1', 'FILE2'),
                       help='Compare two NetCDF files side-by-side with shared color scale')
    parser.add_argument('--cb', '--colorbar', nargs=2, type=float, metavar=('MIN', 'MAX'),
                       help='Specify colorbar range as two values: --cb MIN MAX (e.g., --cb -200 100)')
    parser.add_argument('--discrete', type=int, metavar='N',
                       help='Use discrete colorbar with N colors (e.g., --discrete 6). Default is continuous.')
    parser.add_argument('--list', action='store_true',
                       help='List available variables and exit')
    parser.add_argument('--port', type=int, default=5006,
                       help='Port for Bokeh server (default: 5006)')
    parser.add_argument('--nee', action='store_true',
                       help='Calculate NEE (Net Ecosystem Exchange) as RECO - GPP from ../Alaska/merged/')
    parser.add_argument('--gpp-file', type=str, default=None,
                       help='Custom path to GPP NetCDF file (optional, for --nee)')
    parser.add_argument('--reco-file', type=str, default=None,
                       help='Custom path to RECO NetCDF file (optional, for --nee)')
    parser.add_argument('--output-file', type=str, default=None,
                       help='Custom output path for NEE file (optional, for --nee)')
    
    args = parser.parse_args()
    
    # Handle --compare flag
    if args.compare:
        file_path1, file_path2 = args.compare
        colorbar_range = tuple(args.cb) if args.cb else None
        n_discrete = args.discrete if args.discrete else None
        make_doc = create_comparison_app(file_path1, file_path2, args.variable, colorbar_range, n_discrete)
        
        if make_doc is None:
            print("Failed to create comparison application")
            return
        
        print(f"\n🔄 Starting Circumpolar Bokeh comparison server on port {args.port}...")
        print(f"Opening browser at http://localhost:{args.port}/")
        print("✓ Comparing two files side-by-side")
        print("✓ Shared color scale with discrete blue-white-red palette (6 colors)")
        print("✓ Both time series plotted together")
        print("✓ Synchronized zoom and pan")
        print("Press Ctrl+C to stop the server\n")
        
        apps = {'/': Application(FunctionHandler(make_doc))}
        server = Server(apps, port=args.port, allow_websocket_origin=[f"localhost:{args.port}"])
        server.start()
        server.io_loop.add_callback(server.show, "/")
        
        try:
            server.io_loop.start()
        except KeyboardInterrupt:
            print("\nShutting down server...")
        return
    
    # Handle --nee flag
    if args.nee:
        nee_file = calculate_nee(
            gpp_file=args.gpp_file,
            reco_file=args.reco_file,
            output_file=args.output_file
        )
        print(f"\nNEE file created: {nee_file}")
        print("\nYou can now visualize it with:")
        print(f"  python {Path(__file__).name} {nee_file}")
        return
    
    # Require file_path if not using --nee or --compare
    if args.file_path is None:
        parser.error("file_path is required when not using --nee or --compare flag")
    
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
    
    colorbar_range = tuple(args.cb) if args.cb else None
    n_discrete = args.discrete if args.discrete else None
    make_doc = create_bokeh_app(args.file_path, args.variable, colorbar_range, n_discrete)
    
    if make_doc is None:
        print("Failed to create Bokeh application")
        return
    
    print(f"\n🌍 Starting Circumpolar Bokeh server with Lat/Lon grid on port {args.port}...")
    print(f"Opening browser at http://localhost:{args.port}/")
    print("✓ Image flipped along X-axis for correct orientation")
    print("✓ Latitude/longitude grid overlay added (no axis ticks)")
    print("✓ 2:1 aspect ratio (width:height)")
    print("Press Ctrl+C to stop the server\n")
    
    apps = {'/': Application(FunctionHandler(make_doc))}
    
    server = Server(apps, port=args.port, allow_websocket_origin=[f"localhost:{args.port}"])
    server.start()
    
    server.io_loop.add_callback(server.show, "/")
    
    try:
        server.io_loop.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")


if __name__ == "__main__":
    main()

