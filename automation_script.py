import os
import sys
import subprocess
import time
from pathlib import Path

def run_cmd(command):
    print(f"[RUN] {command}")
    subprocess.run(command, shell=True, check=True)

def pull_tile(tile_name):
    run_cmd(f"gsutil -m cp -r gs://regionalinputs/CIRCUMPOLAR/{tile_name} .")

def run_gapfill(tile_name):
    run_cmd(f"python process_climate_data_gapfill.py {tile_name}")

def generate_scenarios(tile_name):
    run_cmd(f"python generate_climate_scenarios.py {tile_name} {tile_name}_sc")

def split_base_scenario(path_to_folder, tile_name, base_scenario_name):
    input_path = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}"
    output_path = f"{input_path}_split"
    run_cmd(f"bp batch split -i {input_path} -b {output_path}  --p 100 --e 2000 --s 200 --t 123 --n 76")
    return output_path

def run_batch_scenario(split_path):
    run_cmd(f"bp batch run -b {split_path}")

def wait_for_jobs():
    print("[WAIT] Waiting for jobs to finish...")
    while True:
        result = subprocess.run(["squeue", "-u", os.getenv("USER")], capture_output=True, text=True)
        if len(result.stdout.strip().splitlines()) <= 1:
            print("[DONE] All jobs finished.")
            break
        print("[INFO] Still running... will check again in 5 minutes.")
        time.sleep(300)

def merge_and_plot(split_path):
    run_cmd(f"bp batch merge -b {split_path}")
    run_cmd(f"python plot_nc_all_files.py {split_path}/all_merged/")

def split_rest_scenarios(path_to_folder, tile_name, base_scenario_name):
    scenario_dir = Path(path_to_folder) / f"{tile_name}_sc"
    scenarios = [
        d.name for d in scenario_dir.iterdir()
        if d.is_dir() and d.name.startswith("ssp") and d.name != base_scenario_name
    ]
    #filter scenario folder names ending on _split 
    scenarios_nosplit = [s for s in scenarios if "_split" not in s]
    
    for scenario in scenarios_nosplit:
        input_path = scenario_dir / scenario
        output_path = f"{input_path}_split"
        run_cmd(f"bp batch split -i {input_path} -b {output_path}")
    return scenarios_nosplit

def modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios):
    for scenario in scenarios:
        base_folder = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}_split"
        scenario_folder = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        run_cmd(f"python generate_next_scenario.py {base_folder} {scenario_folder}")

def process_remaining_scenarios(path_to_folder, tile_name, scenarios):
    for scenario in scenarios:
        split_path = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        run_batch_scenario(split_path)
        wait_for_jobs()
        merge_and_plot(split_path)

def main():
    if len(sys.argv) < 2:
        print("Usage: python automation_script.py <tile_name>")
        sys.exit(1)

    tile_name = sys.argv[1]
    base_scenario_name = "ssp1_2_6_access_cm2__ssp1_2_6"  # Change if needed
    path_to_folder = os.getcwd()

    pull_tile(tile_name)
    run_gapfill(tile_name)
    generate_scenarios(tile_name)

    base_split_path = split_base_scenario(path_to_folder, tile_name, base_scenario_name)
    run_batch_scenario(base_split_path)
    wait_for_jobs()
    merge_and_plot(base_split_path)

    scenarios = split_rest_scenarios(path_to_folder, tile_name, base_scenario_name)
    modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios)
    process_remaining_scenarios(path_to_folder, tile_name, scenarios)

if __name__ == "__main__":
    main()

