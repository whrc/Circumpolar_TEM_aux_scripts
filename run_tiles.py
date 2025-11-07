#!/usr/bin/env python
import subprocess
import os
import argparse

#before running script make sure that folder structure 
# Region (e.g. Alaska):
#    input_tiles [original input tiles]
#    tile#ids_sc (e.g. H10_V14_sc, H10_V15_sc, ...)
#    LOGS will be created if does not exists
#in the future tile#ids_sc will moved to the correspoding case folder
#e.g. olt_const, olt_nonconst,olt_nonconst_fire, ... 

# Parse command line arguments
parser = argparse.ArgumentParser(description='Run automation script for multiple tiles')
parser.add_argument('tile_name', help='Name of the tile to run')
args = parser.parse_args()

tile_name = args.tile_name
log_dir = "LOG"

os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"{tile_name}ssp26.log")
cmd = f"python ~/Circumpolar_TEM_aux_scripts/automation_script.py {tile_name} --mode full > {log_file} 2>&1"
# cmd = f"python ~/Circumpolar_TEM_aux_scripts/automation_script.py {tile_name} --mode sc -bucket circumpolar_model_output/recent2  --base-scenario-name ssp5_8_5_mri_esm2_0 > {log_file} 2>&1"
print(f"Running: {cmd}")
subprocess.run(cmd, shell=True)
