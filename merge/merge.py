## Author: Helene Genet hgenet@alaska.edu
## Description: this script merge outputs across multiple tiles, 
# even when tiles are not adjacent. This script also summarize 
# the outputs when specified by user.


import os
import xarray as xr
import pandas as pd
# from osgeo import gdal
import numpy as np
import glob
import argparse

#Usage example: python merge.py Alaska ssp1_2_6_mri_esm2_0 --temdir path_to_outspec_file
# List of emission scenarios
#sclist = ['ssp1_2_6','ssp2_4_5','ssp3_7_0','ssp5_8_5']
# List of global climate model
#gcmlist = ['access_cm2','mri_esm2_0']
# List of output tiles to merge
#tilelist = ['H10_V14','H10_V18']


#### COMMAND LINE ARGUMENTS ####

def parse_arguments():
    parser = argparse.ArgumentParser(description='Merge TEM outputs across multiple tiles for a given scenario')
    parser.add_argument('base_path', help='Base path containing tile directories (e.g., /mnt/exacloud/dteber_woodwellclimate_org/Alaska)')
    parser.add_argument('scenario', help='Scenario name to process (e.g., ssp1_2_6_mri_esm2_0)')
    parser.add_argument('--temdir', default='/opt/dvm-dos-tem', 
                       help='Path to dvm-dos-tem directory (default: /opt/dvm-dos-tem)')
    parser.add_argument('--no-yearsynth', action='store_false', dest='yearsynth',
                       help='Disable yearly synthesis of monthly outputs')
    parser.add_argument('--no-compsynth', action='store_false', dest='compsynth',
                       help='Disable synthesis across compartments')
    parser.add_argument('--no-pftsynth', action='store_false', dest='pftsynth',
                       help='Disable synthesis by PFT')
    parser.add_argument('--no-layersynth', action='store_false', dest='layersynth',
                       help='Disable synthesis by layer')
    parser.set_defaults(yearsynth=True, compsynth=True, pftsynth=True, layersynth=True)
    return parser.parse_args()

# Parse command line arguments
args = parse_arguments()

### Paths (derived from command line arguments)
## Base path containing all tiles
base_path = args.base_path
## Scenario to process
scenario = args.scenario
## Path to the merging/synthesis directory
synthdir = os.path.join(base_path, 'merged')
## Path to dvm-dos-tem directory
temdir = args.temdir

### Synthesis level
# Do you want to synthesize the monthly outputs yearly?
yearsynth = args.yearsynth
# Do you want to synthesize the outputs across commpartment?
compsynth = args.compsynth
# Do you want to synthesize the outputs by PFT?
pftsynth = args.pftsynth
# Do you want to synthesize the outputs by layer?
layersynth = args.layersynth

# Create output directory if it doesn't exist
os.makedirs(synthdir, exist_ok=True)


#### LISTINGS ####

### Listing available tiles and output variables for the specified scenario
tilelist = []
outflist = []

# Find all tile directories (looking for H10_V## pattern instead of *_sc)
scenario_path = os.path.join(base_path, scenario)
if os.path.exists(scenario_path):
    for item in os.listdir(scenario_path):
        if (os.path.isdir(os.path.join(scenario_path, item))) & (not item.startswith('.')):
            tile_path = os.path.join(scenario_path, item)
            # Check if the all_merged directory exists
            all_merged_path = os.path.join(tile_path, 'all_merged')
            if os.path.exists(all_merged_path):
                tilelist.append(item)
                # Get output files from this tile's all_merged directory
                for outf in os.listdir(all_merged_path):
                    if (os.path.isfile(os.path.join(all_merged_path, outf))) & (outf.endswith('.nc')) & (not outf.startswith('.')):
                        # Skip restart files and run_status
                        if outf not in ['restart-sc.nc', 'restart-tr.nc', 'restart-eq.nc', 'restart-sp.nc', 'restart-pr.nc', 'run_status.nc']:
                            outflist.append(outf)

# Process the lists
outflist = list(set(outflist))
varlist = list(set([item.split('_')[0] for item in outflist]))

print(f"Found {len(tilelist)} tiles for scenario '{scenario}':")
for tile in tilelist:
    print(f"  - {tile}")
print(f"Found {len(varlist)} variables: {varlist}")


#### CREATE CANVAS ####

### Get the extent of all the tiles 
xminlist = []
xmaxlist = []
yminlist = []
ymaxlist = []
for tile in tilelist:
    # Path to the run-mask.nc file for this tile and scenario
    mask_path = os.path.join(base_path, scenario, tile, 'run-mask.nc')
    if os.path.exists(mask_path):
        mask = xr.open_dataset(mask_path)
        # Handle different coordinate naming conventions
        if 'X' in mask.coords:
            xminlist.append(mask.X.min().values.item())
            xmaxlist.append(mask.X.max().values.item())
        elif 'x' in mask.coords:
            xminlist.append(mask.x.min().values.item())
            xmaxlist.append(mask.x.max().values.item())
        
        if 'Y' in mask.coords:
            yminlist.append(mask.Y.min().values.item())
            ymaxlist.append(mask.Y.max().values.item())
        elif 'y' in mask.coords:
            yminlist.append(mask.y.min().values.item())
            ymaxlist.append(mask.y.max().values.item())
        mask.close()

print('yminlist: ' + str(yminlist) + ' ymaxlist: ' + str(ymaxlist) + ' xminlist: ' + str(xminlist) + ' xmaxlist: ' + str(xmaxlist))

### Create the canvas from the first available mask, cropped to total extent
# Use the first tile's mask as the template and extend it to cover all tiles
first_tile = tilelist[0]
template_mask_path = os.path.join(base_path, scenario, first_tile, 'run-mask.nc')
template_mask = xr.open_dataset(template_mask_path)

# Determine coordinate names
x_coord = 'X' if 'X' in template_mask.coords else 'x'
y_coord = 'Y' if 'Y' in template_mask.coords else 'y'

# Create a canvas that covers the extent of all tiles
canvas_x = template_mask[x_coord].sel({x_coord: slice(min(xminlist), max(xmaxlist))})
canvas_y = template_mask[y_coord].sel({y_coord: slice(min(yminlist), max(ymaxlist))})

# Create the canvas dataset
crop_mask = template_mask.sel({x_coord: slice(min(xminlist), max(xmaxlist)), 
                               y_coord: slice(min(yminlist), max(ymaxlist))})
crop_mask.to_netcdf(os.path.join(synthdir, 'canvas.nc'))
template_mask.close()

print(f"Canvas created with extent: x=[{min(xminlist):.2f}, {max(xmaxlist):.2f}], y=[{min(yminlist):.2f}, {max(ymaxlist):.2f}]")


#### MERGING OUTPUTS ####

### Reading the outvarlist file to a dataframe (if it exists)
ovl = None
if os.path.exists(os.path.join(temdir, 'output_spec.csv')):
    ovl = pd.read_csv(os.path.join(temdir, 'output_spec.csv'))
    print("Loaded output specification file")
else:
    print("Warning: output_spec.csv not found, synthesis options will be disabled")
    print(os.path.join(temdir, 'output_spec.csv'), 'does not exist')
    sys.exit()


## Variable loop 
for var in varlist:
    print(f'Processing variable: {var}')
    tempres = None
    
    for t in range(len(tilelist)):
        tile = tilelist[t]
        print(f'  Processing tile {t+1}/{len(tilelist)}: {tile}')
        
        # Construct path to the all_merged directory for this tile and scenario
        all_merged_dir = os.path.join(base_path, scenario, tile, 'all_merged')

        # Find the variable file
        var_files = glob.glob(os.path.join(all_merged_dir, var + '*.nc'))
        if not var_files:
            print(f'    Warning: No file found for variable {var} in tile {tile}')
            continue
            
        var_file = var_files[0]  # Take the first match
 
        # Read in the tile data and mask
        try:
            out = xr.open_dataset(var_file)
            
            # Convert -9999 fill values to NaN for VEGC variable
            if var in out.variables:
                out[var] = out[var].where(out[var] != -9999, np.nan)
                print(f'    Converted -9999 values to NaN for {var}')
            
            #NB!here might be a problem, this mask file is for input, where in our case we might need mask file for output
            #so we might need to merge the mask file from split outputs
            mask_file = os.path.join(base_path, scenario, tile, 'run-mask.nc')
            msk = xr.open_dataset(mask_file)
            
            # Read in temporal resolution from filename
            tempres = os.path.basename(var_file).split('_')[1]
        
            
            # Apply synthesis operations if output spec is available
            if ovl is not None and var in ovl['Name'].values:
                var_spec = ovl[ovl['Name'] == var].iloc[0]
                
                # Monthly to yearly synthesis
                if (tempres == 'monthly') & (yearsynth == True):
                    if var_spec['Yearsynth'] != 'invalid':
                        tempres = 'yearly'
                        op = var_spec['Yearsynth']
                        codstg = f"out = out['{var}'].resample(time='Y').{op}(skipna=False).to_dataset()"
                        exec(codstg)
                        
                # Compartment synthesis
                if ('pftpart' in list(out[var].dims)) & (compsynth == True):
                    if var_spec['Yearsynth'] != 'invalid':
                        op = var_spec['Yearsynth']
                        codstg = f"out = out.{op}(dim = 'pftpart', skipna=False)"
                        exec(codstg)
                        
                # PFT synthesis
                if ('pft' in list(out[var].dims)) & (pftsynth == True):
                    if 'Pftsynth' in var_spec and var_spec['Pftsynth'] != 'invalid':
                        op = var_spec['Pftsynth']
                        codstg = f"out = out.{op}(dim = 'pft', skipna=False)"
                        exec(codstg)
                        
                # Layer synthesis
                if ('layer' in list(out[var].dims)) & (layersynth == True):
                    if var_spec['Yearsynth'] != 'invalid':
                        op = var_spec['Yearsynth']
                        codstg = f"out = out.{op}(dim = 'layer', skipna=False)"
                        exec(codstg)
            
            # Handle coordinate naming conventions and add coordinate values
            x_coord = 'X' if 'X' in msk.coords else 'x'
            y_coord = 'Y' if 'Y' in msk.coords else 'y'
            
            out = out.assign_coords(x=("x", msk[x_coord].values), y=("y", msk[y_coord].values))
                     
            # Create the canvas dataset for combining (only for first tile)
            if t == 0:
                # Get fill value from output file
                varfv = out[var].encoding.get('_FillValue')
                if varfv is None:
                    varfv = np.nan

                # Identify dimensions associated with this variable
                dimname = list(out[var].dims)
                           
                # Create the empty dataset that will be the canvas for combining tiles
                dimlengthlist = []
                for dim in dimname:
                    if dim == 'x':
                        l = crop_mask[x_coord].shape[0]
                    elif dim == 'y':
                        l = crop_mask[y_coord].shape[0]
                    else:
                        l = out[dim].shape[0]
                    dimlengthlist.append(l)            

                # Create the coordinates dataset
                coords = {}
                for i in range(len(dimname)):
                    if dimname[i] == 'x':
                        coords[dimname[i]] = crop_mask[x_coord].values
                    elif dimname[i] == 'y':
                        coords[dimname[i]] = crop_mask[y_coord].values
                    else:
                        coords[dimname[i]] = out[dimname[i]].values
                
                # Create the variable dataset
                data_vars = {var: (tuple(dimname), np.full(tuple(dimlengthlist), varfv))}
                
                
                # Create the canvas
                canevas = xr.Dataset(data_vars, coords=coords)
                canevas.attrs = out.attrs
                varattrs = out[var].attrs.copy()
                varattrs['_FillValue'] = varfv
                canevas[var].attrs = varattrs
                canevas.encoding = out.encoding
            
            # Combining the tile dataset to the canvas
            canevas = out.combine_first(canevas)

            
            # Close datasets to free memory
            out.close()
            msk.close()
            
            
        except Exception as e:
            print(f'    Error processing tile {tile}: {e}')
            continue
    print('********************************************************')

    # Finalize the canvas coordinates and save
    if 'canevas' in locals():
        canevas['y'] = crop_mask[y_coord]
        canevas['x'] = crop_mask[x_coord]
        canevas['x'].attrs = crop_mask[x_coord].attrs
        canevas['y'].attrs = crop_mask[y_coord].attrs
               
        # Create output filename
        output_filename = f"{var}_{scenario}_{tempres}.nc"
        output_path = os.path.join(synthdir, output_filename)
        
        print(f'  Saving merged output: {output_filename}')
        canevas.to_netcdf(output_path)
        canevas.close()
        
        # Clean up for next variable
        del canevas

print(f"\nMerging complete! Output files saved to: {synthdir}")
