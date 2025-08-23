"""
Generate climate scenarios by copying and organizing climate data files.

This script creates scenario folders with matched projected climate and CO2 files
based on their SSP scenario codes (e.g., ssp1_2_6, ssp2_4_5, etc.).

Usage:
    python generate_climate_scenarios.py <tile_path> <output_path>

Arguments:
    <tile_path>         Path to the directory containing the source climate data files.
    <output_path>       Path to the directory where scenario folders will be created.

Example:
    python generate_climate_scenarios.py /mnt/exacloud/data/H10_V17/ /mnt/exacloud/dteber_woodwellclimate_org/scenarios/H10_V17/

The script will create 8 scenario folders, each containing:
- One projected climate file matched with its corresponding CO2 file
- All standard data files (historic climate, vegetation, soil, etc.)
"""


import shutil
import sys
import os

FILES_TO_COPY = [
    "co2.nc",
    "drainage.nc",
    "fri-fire.nc",
    "historic-climate.nc",
    "historic-explicit-no-fire.nc",
    "projected-explicit-no-fire.nc",
    "topo.nc",
    "vegetation.nc",
    "run-mask.nc",
    "soil-texture.nc",
]

PROJECTED_CLIMATE_FILES = [
    "projected-climate_ssp1_2_6_access_cm2.nc",
    "projected-climate_ssp1_2_6_mri_esm2_0.nc",
    "projected-climate_ssp2_4_5_access_cm2.nc",
    "projected-climate_ssp2_4_5_mri_esm2_0.nc",
    "projected-climate_ssp3_7_0_access_cm2.nc",
    "projected-climate_ssp3_7_0_mri_esm2_0.nc",
    "projected-climate_ssp5_8_5_access_cm2.nc",
    "projected-climate_ssp5_8_5_mri_esm2_0.nc",
]

PROJECTED_CO2_FILES = [
    "projected-co2_ssp1_2_6.nc",
    "projected-co2_ssp2_4_5.nc",
    "projected-co2_ssp3_7_0.nc",
    "projected-co2_ssp5_8_5.nc",
]

def generate_projected_climate_scenarios(tile_path, output_path):
    scenario_names = []
    
    # Match each climate file with its corresponding CO2 file based on SSP scenario
    for projected_climate_file in PROJECTED_CLIMATE_FILES:
        # Extract SSP scenario from climate filename (e.g., "ssp1_2_6" from "projected-climate_ssp1_2_6_access_cm2.nc")
        ssp_scenario = projected_climate_file.replace("projected-climate_", "").split("_")[0:3]  # Gets ['ssp1', '2', '6']
        ssp_scenario_str = "_".join(ssp_scenario)  # Reconstructs "ssp1_2_6"
        
        # Find matching CO2 file
        projected_co2_file = f"projected-co2_{ssp_scenario_str}.nc"
        
        # Verify the CO2 file exists in our list
        if projected_co2_file not in PROJECTED_CO2_FILES:
            print(f"Warning: No matching CO2 file found for {projected_climate_file}")
            continue
 
        # Folder name uses only the climate model/scenario (no CO2 suffix)
        #folder_name = projected_climate_file.replace("projected-climate_", "").replace(".nc", "")
        #path = os.path.join(output_path, folder_name)

        projected_climate_scenario = projected_climate_file.replace("projected-climate", "").replace(".nc", "").strip("_")
        projected_co2_scenario = projected_co2_file.replace("projected-co2", "").replace(".nc", "").strip("_")

        merged_folder_name = f"{projected_climate_scenario}"
        path = os.path.join(output_path, merged_folder_name)
        if not os.path.exists(path):
            os.makedirs(path)

        shutil.copy(os.path.join(tile_path, projected_climate_file), os.path.join(path, "projected-climate.nc"))
        shutil.copy(os.path.join(tile_path, projected_co2_file), os.path.join(path, "projected-co2.nc"))

        for file in FILES_TO_COPY:
            new_file_name = file
            if file == "projected-explicit-no-fire.nc":
                new_file_name = "projected-explicit-fire.nc"
            elif file == "historic-explicit-no-fire.nc":
                new_file_name = "historic-explicit-fire.nc"

            shutil.copy(os.path.join(tile_path, file), os.path.join(path, new_file_name))

        scenario_names.append(merged_folder_name)

    return scenario_names


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python main.py <tile_image_path> <output_path>\nPlease provide a path to the tile image and the output path.")
        sys.exit(1)

    tile_path = sys.argv[1]
    output_path = sys.argv[2]

    print("Generating projected climate scenarios...")
    scenario_names = generate_projected_climate_scenarios(
        tile_path,
        output_path
    )

    print("Done!")
