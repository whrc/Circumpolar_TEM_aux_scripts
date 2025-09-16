#!/bin/bash

# Simple script to upload all tiles and scenarios to Google Cloud Storage
# This version uses the exact same commands as your manual process
# Usage: ./upload_simple.sh <base_directory>

set -e  # Exit on any error

# Check if base directory argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <base_directory>"
    echo "Example: $0 /mnt/exacloud/dteber_woodwellclimate_org/Alaska"
    exit 1
fi

# Base configuration
BASE_DIR="$1"
GCS_BASE="gs://circumpolar_model_output/Alaska-v1"

# Validate that the base directory exists
if [ ! -d "$BASE_DIR" ]; then
    echo "Error: Directory '$BASE_DIR' does not exist"
    exit 1
fi

# Change to base directory
cd "$BASE_DIR"

echo "Starting upload process..."
echo "Current directory: $(pwd)"

# Define scenarios (based on your current structure)
scenarios=(
    "ssp1_2_6_access_cm2"
    "ssp1_2_6_mri_esm2_0"
    "ssp2_4_5_access_cm2"
    "ssp2_4_5_mri_esm2_0"
    "ssp3_7_0_access_cm2"
    "ssp3_7_0_mri_esm2_0"
    "ssp5_8_5_access_cm2"
    "ssp5_8_5_mri_esm2_0"
)

# Function to upload
upload_scenario() {
    local scenario="$1"
    echo "=== Processing scenario: $scenario ==="
    
    # Upload merged_tiles for this scenario
    if [ -d "merged_tiles/$scenario" ]; then
        echo "Uploading merged_tiles/$scenario..."
        gsutil -m cp -r "merged_tiles/$scenario/" "$GCS_BASE/merged_tiles/"
    else
        echo "Warning: merged_tiles/$scenario not found"
    fi
    
    # Upload split_tiles for this scenario  
    if [ -d "split_tiles/$scenario" ]; then
        echo "Uploading split_tiles/$scenario..."
        gsutil -m cp -r "split_tiles/$scenario/" "$GCS_BASE/split_tiles/"
    else
        echo "Warning: split_tiles/$scenario not found"
    fi
    
    echo "Completed scenario: $scenario"
    echo
}

# Upload all scenarios
for scenario in "${scenarios[@]}"; do
    upload_scenario "$scenario"
done

echo "All uploads completed!"
