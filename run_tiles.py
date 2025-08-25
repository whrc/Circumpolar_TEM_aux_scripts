#!/usr/bin/env python
import subprocess
import subprocess
import os

#before running script make sure that folder structure 
# Region (e.g. Alaska):
#    input_tiles [original input tiles]
#    tile#ids_sc (e.g. H10_V14_sc, H10_V15_sc, ...)
#    LOGS will be created if does not exists
#in the future tile#ids_sc will moved to the correspoding case folder
#e.g. olt_const, olt_nonconst,olt_nonconst_fire, ... 


scs = ["H11_V15", "H11_V16", "H11_V17", "H11_V18"]
log_dir = "LOG"

# Create the LOG directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

for tile in scs:
    log_file = os.path.join(log_dir, f"{tile}.log")
    cmd = f"python automation_script_v1.py {tile} --mode sc > {log_dir}/{log_file} 2>&1"
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True)
