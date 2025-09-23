# TEM Output Tile Merging

This directory contains scripts for downloading and merging TEM (Terrestrial Ecosystem Model) output tiles from Google Cloud Storage.

## Overview

The merging process consists of two main steps:
1. **Download tiles** from the cloud storage bucket
2. **Merge the tiles** into a single output dataset

## Prerequisites

- Python 3.x with required packages (xarray, pandas, numpy)
- Google Cloud SDK with `gsutil` installed and configured
- Access to the required Google Cloud Storage buckets:
  - `gs://circumpolar_model_output/`
  - `gs://regionalinputs/CIRCUMPOLAR/`

## Instructions

### Step 0: Setup Environment (Before Starting)

Before running any scripts, create and activate the conda environment:

```bash
# Create the environment with a custom name
conda env create -f environment.yml -n merge_env

# Activate the environment
conda activate merge_env
```

### Step 1: Download Tiles

1. **Configure the download script**: Edit `download_tiles.py` to specify your desired:
   - `region` (e.g., "Alaska")
   - `scenario_name` (e.g., "ssp1_2_6_mri_esm2_0")
   - `tile_list` (e.g., ['H10_V14', 'H10_V15', 'H10_V16'])

2. **Run the download script**:
   ```bash
   python download_tiles.py
   ```

   This script will:
   - Create the necessary directory structure (`region/scenario_name/tile_id/`)
   - Check if tiles already exist locally
   - Download missing tiles from the cloud storage:
     - Model outputs: `gs://circumpolar_model_output/Alaska-v1/merged_tiles/scenario_name/tile_id/all_merged`
     - Run masks: `gs://regionalinputs/CIRCUMPOLAR/tile_id/run-mask.nc`

### Step 2: Merge Tiles

Run the merge script with the appropriate parameters:

```bash
python merge.py Alaska ssp1_2_6_mri_esm2_0 --temdir path_to_outspec_file
```

**Parameters:**
- `Alaska`: Region name (must match the region used in download_tiles.py)
- `ssp1_2_6_mri_esm2_0`: Scenario name (must match the scenario used in download_tiles.py)
- `--temdir path_to_outspec_file`: Path to the directory containing `output_spec.csv`

**Optional flags:**
- `--no-yearsynth`: Disable yearly synthesis of monthly outputs
- `--no-compsynth`: Disable synthesis across compartments
- `--no-pftsynth`: Disable synthesis by PFT
- `--no-layersynth`: Disable synthesis by layer

## Output

The merged files will be saved in the `merged/` subdirectory within the region folder:
```
Alaska/
├── merged/
│   ├── canvas.nc
│   ├── VEGC_ssp1_2_6_mri_esm2_0_yearly.nc
│   ├── SOC_ssp1_2_6_mri_esm2_0_yearly.nc
│   └── ... (other variables)
└── ssp1_2_6_mri_esm2_0/
    ├── H10_V14/
    ├── H10_V15/
    └── ... (downloaded tiles)
```

## Example Workflow

```bash
# 1. Configure and download tiles
python download_tiles.py

# 2. Merge the downloaded tiles
python merge.py Alaska ssp1_2_6_mri_esm2_0 --temdir /path/to/dvm-dos-tem

# 3. Plot the merged output
cd ../ && python plot_nc_all_files.py merge/Alaska/merged
```

## Troubleshooting

- **gsutil not found**: Install Google Cloud SDK and authenticate with `gcloud auth login`
- **Access denied**: Ensure you have proper permissions for the storage buckets
- **Missing output_spec.csv**: The merge script requires this file to be present in the specified `--temdir` path
- **Memory issues**: For large datasets, consider processing fewer tiles at once

## File Structure

- `download_tiles.py`: Downloads tiles from Google Cloud Storage
- `merge.py`: Merges multiple tiles into unified datasets
- `output_spec.csv`: Specification file for output variables and synthesis operations
- `environment.yml`: Conda environment specification (if present)
