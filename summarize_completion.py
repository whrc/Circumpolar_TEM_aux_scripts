from textwrap import shorten
import subprocess
import os

# List of tiles
scs = ['H10_V15', 'H11_V14', 'H7_V16', 'H8_V14'
]

log_dir = "LOG"
os.makedirs(log_dir, exist_ok=True)

# Print header with ID column
print(f"| {'ID':^3} | {'Path to Tile':<40} | {'# Completed Cells':^19} | {'# Total Cells':^16} | {'Completion (%)':^16} | {'Mean Run Time (s)':^19} |")
print(f"|{'-'*5}|{'-'*42}|{'-'*21}|{'-'*18}|{'-'*18}|{'-'*21}|")

for idx, tile in enumerate(scs, 1):
    path = f"{tile}_sc/ssp1_2_6_access_cm2_split"
    cmd = f"python ~/Circumpolar_TEM_aux_scripts/check_tile_run_completion.py {path}"

    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        output = result.stdout.strip().split("\n")

        completed_cells = "?"
        total_cells = "?"
        completion = ""
        mean_runtime = ""

        for line in output:
            line = line.strip()

            if line.replace(" ", "").isdigit() and " " in line:
                parts = line.split()
                if len(parts) == 2:
                    completed_cells, total_cells = parts

            elif "Overall Completion" in line:
                completion = line.split(":")[-1].strip().replace("%", "")

            elif "Mean total runtime" in line:
                mean_runtime = line.split(":")[-1].strip().replace("seconds", "")

        print(f"| {idx:^3} | {path:<40} | {completed_cells:^19} | {total_cells:^16} | {completion:^16} | {mean_runtime:^19} |")

    except Exception as e:
        print(f"| {idx:^3} | {path:<40} | {'ERROR':^19} | {'ERROR':^16} | {'ERROR':^16} | {'ERROR':^19} |")
