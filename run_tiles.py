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
parser.add_argument('tiles_file', help='Path to the text file containing tiles (one per line)')
args = parser.parse_args()

tiles_file = args.tiles_file
try:
    with open(tiles_file, 'r') as f:
        scs = [line.strip() for line in f.readlines() if line.strip()]
except FileNotFoundError:
    print(f"Error: {tiles_file} not found. Please create a file with tiles separated by new lines.")
    exit(1)
except Exception as e:
    print(f"Error reading {tiles_file}: {e}")
    exit(1)

if not scs:
    print(f"Error: No tiles found in {tiles_file}")
    exit(1)
log_dir = "LOG"

os.makedirs(log_dir, exist_ok=True)

for tile in scs:
    log_file = os.path.join(log_dir, f"{tile}.log")
    cmd = f"python ~/Circumpolar_TEM_aux_scripts/automation_script.py {tile} --mode full > {log_file} 2>&1"
   #cmd = f"python ~/Circumpolar_TEM_aux_scripts/automation_script.py {tile} --mode sc -bucket circumpolar_model_output/recent2  --base-scenario-name ssp5_8_5_mri_esm2_0 > {log_file} 2>&1"
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True)
