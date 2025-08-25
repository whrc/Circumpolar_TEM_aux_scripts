import os
import sys
import subprocess
import time
from pathlib import Path
import re
import argparse

def run_cmd(command, auto_yes=False):
    print(f"[RUN] {command}")
    try:
        if auto_yes:
            # Feed "y\n" automatically
            subprocess.run(command, shell=True, check=True, input=b"y\n")
        else:
            subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed: {command}")
        print(f"[CODE] Exit status: {e.returncode}")
        print(f"[INFO] Continuing execution...")

def pull_tile(tile_name):
    # Create the input_tiles directory if it doesn't exist
    # This assumes that the cur_dir is a Region (e.g. Alaska)
    input_tiles_dir="input_tiles"
    os.makedirs(log_dir, exist_ok=True)
    run_cmd(f"gsutil -m cp -r gs://regionalinputs/CIRCUMPOLAR/{tile_name} {input_tiles_dir}/.")

def run_gapfill(tile_name):
    input_tiles_dir="input_tiles"
    run_cmd(f"python process_climate_data_gapfill.py {input_tiles_dir}/{tile_name}")

def generate_scenarios(tile_name):
    input_tiles_dir="input_tiles"
    run_cmd(f"python generate_climate_scenarios.py {input_tiles_dir}/{tile_name} {tile_name}_sc")

def split_base_scenario(path_to_folder, tile_name, base_scenario_name):
    input_path = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}"
    output_path = f"{input_path}_split"
    run_cmd(f"bp batch split -i {input_path} -b {output_path} --p 100 --e 2000 --s 200 --t 123 --n 76")
    return output_path

def check_run_completion(folder_path):
    try:
        # Run the check_runs.py script and capture output
        result = subprocess.run(
            ["python", "check_tile_run_completion.py", folder_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=True
        )

        # Search for "Overall Completion: XX.XX%"
        match = re.search(r"Overall Completion:\s+(\d+(?:\.\d+)?)%", result.stdout)
        if match:
            completion = float(match.group(1))
            return completion
    except subprocess.CalledProcessError:
        pass

    return None

def run_batch_scenario(split_path):
    #before submitting batches check for completion
    completion = check_run_completion(split_path)
    #if complete,skip: `batch run`
    if completion > 90.0:
        print(f"batch_completion = {completion:.2f}%")
        print(f"Skipping the batch run step")
    else:
        print("Could not determine completion.",completion)
        print(f"Executing batch run... ")
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
    plot_file = Path(f"{split_path}/all_merged/summary_plots.pdf")
    #plot_file = f"{split_path}/all_merged/summary_plots.pdf"
    if plot_file.exists():
        print(f"Skipping the merge. {plot_file} exists.")
    else:
        run_cmd(f"bp batch merge -b {split_path}", auto_yes=True)
        run_cmd(f"python plot_nc_all_files.py {split_path}/all_merged/")

def split_rest_scenarios(path_to_folder, tile_name, base_scenario_name):
    scenario_dir = Path(path_to_folder) / f"{tile_name}_sc"
    scenarios = [
        d.name for d in scenario_dir.iterdir()
        if d.is_dir() and d.name.startswith("ssp") and d.name != base_scenario_name
    ]
    scenarios_nosplit = [s for s in scenarios if "_split" not in s]
    
    for scenario in scenarios_nosplit:
        input_path = scenario_dir / scenario
        output_path = f"{input_path}_split"
        run_cmd(f"bp batch split -i {input_path} -b {output_path}")
    return scenarios_nosplit

def modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios):
    base_folder = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}_split"
    for scenario in scenarios:
        scenario_folder = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        run_cmd(f"python generate_next_scenario.py {base_folder} {scenario_folder}")

def process_remaining_scenarios(path_to_folder, tile_name, scenarios):
    for scenario in scenarios:
        split_path = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        run_batch_scenario(split_path)
        wait_for_jobs()
        merge_and_plot(split_path)

def main():
    parser = argparse.ArgumentParser(
        description="Run automation for a tile. Use --mode sc (default) or --mode full."
    )
    parser.add_argument("tile_name", help="Tile name, e.g., H10_V16")
    parser.add_argument(
        "--mode",
        choices=["sc", "full"],
        default="sc",
        help="Execution mode: 'sc' runs scenario-only steps; 'full' runs end-to-end.",
    )
    parser.add_argument(
        "--base-scenario-name",
        default="ssp1_2_6_access_cm2",
        help="Base scenario folder name (default: ssp1_2_6_access_cm2)",
    )
    args = parser.parse_args()

    tile_name = args.tile_name
    base_scenario_name = args.base_scenario_name
    path_to_folder = os.getcwd()

    if args.mode == "full":
        print("[MODE] full — running end-to-end pipeline")

        # formerly-commented steps
        pull_tile(tile_name)
        run_gapfill(tile_name)
        generate_scenarios(tile_name)

        # full pipeline uses your splitter to derive the base scenario path
        base_split_path = split_base_scenario(path_to_folder, tile_name, base_scenario_name)
        run_batch_scenario(base_split_path)
        wait_for_jobs()
        merge_and_plot(base_split_path)

        # scenario processing
        scenarios = split_rest_scenarios(path_to_folder, tile_name, base_scenario_name)
        print(scenarios)
        modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios)
        process_remaining_scenarios(path_to_folder, tile_name, scenarios)

    elif args.mode == "base":
        print("[MODE] base — running base-scenario-only steps")

        # formerly-commented steps
        pull_tile(tile_name)
        run_gapfill(tile_name)
        generate_scenarios(tile_name)

        # full pipeline uses your splitter to derive the base scenario path
        base_split_path = split_base_scenario(path_to_folder, tile_name, base_scenario_name)
        run_batch_scenario(base_split_path)
        wait_for_jobs()
        merge_and_plot(base_split_path)

    else:
        print("[MODE] sc — running scenario-only steps")

        # use path_to_folder for base_split_path as requested
        base_split_path = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}"

        scenarios = split_rest_scenarios(path_to_folder, tile_name, base_scenario_name)
        print(scenarios)
        modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios)
        process_remaining_scenarios(path_to_folder, tile_name, scenarios)


if __name__ == "__main__":
    main()


