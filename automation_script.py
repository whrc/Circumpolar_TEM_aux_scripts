import os
import sys
import subprocess
import time
import logging
import argparse
from pathlib import Path
import re
from datetime import datetime

def run_cmd(command, auto_yes=False):
    logging.info(f"[RUN] {command}")
    try:
        if auto_yes:
            # Feed "y\n" automatically
            subprocess.run(command, shell=True, check=True, input=b"y\n")
        else:
            subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"[ERROR] Command failed: {command}")
        logging.error(f"[CODE] Exit status: {e.returncode}")
        logging.info(f"[INFO] Continuing execution...")

def pull_tile(tile_name):
    run_cmd(f"gsutil -m cp -r gs://regionalinputs/CIRCUMPOLAR/{tile_name} /mnt/exacloud/data/")

def run_gapfill(tile_name):
    run_cmd(f"python ~/Circumpolar_TEM_aux_scripts/process_climate_data_gapfill.py {tile_name}")

def generate_scenarios(tile_name):
    run_cmd(f"python ~/Circumpolar_TEM_aux_scripts/generate_climate_scenarios.py {tile_name} {tile_name}_sc")

def split_base_scenario(path_to_folder, tile_name, base_scenario_name):
    input_path = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}"
    output_path = f"{input_path}_split"
    run_cmd(f"bp batch split -i {input_path} -b {output_path} -sp dask --p 100 --e 2000 --s 200 --t 123 --n 76")
    return output_path

def check_run_completion(folder_path):
    try:
        # Run the check_runs.py script and capture output
        result = subprocess.run(
            ["python", "~/Circumpolar_TEM_aux_scripts/check_tile_run_completion.py", folder_path],
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
    if completion == 100.0:
        print(f"batch_completion = {completion:.2f}%")
        print(f"Skipping the batch run step")
    else:
        print("Could not determine completion.",completion)
        print(f"Executing batch run... ")
        run_cmd(f"bp batch run -b {split_path}")

def wait_for_jobs():
    logging.info("[WAIT] Waiting for jobs to finish...")
    while True:
        result = subprocess.run(["squeue", "-u", os.getenv("USER")], capture_output=True, text=True)
        if len(result.stdout.strip().splitlines()) <= 1:
            logging.info("[DONE] All jobs finished.")
            break
        logging.info("[INFO] Still running... will check again in 5 minutes.")
        time.sleep(300)

def merge_and_plot(split_path):
    plot_file = f"{split_path}/all_merged/summary_plots.pdf"
    if plot_file.exists():
        print(f"Skipping the merge. {plot_file} exists.")
    else:
        run_cmd(f"bp batch merge -b {split_path}", auto_yes=True)
        run_cmd(f"python ~/Circumpolar_TEM_aux_scripts/plot_nc_all_files.py {split_path}/all_merged/")

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
        run_cmd(f"bp batch split -i {input_path} -b {output_path} -sp dask")
    return scenarios_nosplit

def modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios):
    for scenario in scenarios:
        base_folder = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}_split"
        scenario_folder = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        run_cmd(f"python ~/Circumpolar_TEM_aux_scripts/generate_next_scenario.py {base_folder} {scenario_folder}")

def process_remaining_scenarios(path_to_folder, tile_name, scenarios):
    for scenario in scenarios:
        split_path = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        
        # Check if scenario has already run by looking for summary_plots.pdf in all_merged/ folder
        summary_plots_path = f"{split_path}/all_merged/summary_plots.pdf"
        if os.path.exists(summary_plots_path):
            logging.warning(f"[SKIP] Scenario {scenario} has already run (found {summary_plots_path}). Moving to next scenario.")
            continue
        
        run_batch_scenario(split_path)
        wait_for_jobs()
        merge_and_plot(split_path)

def setup_logging(tile_name):
    """Set up logging configuration to write to file and console"""
    log_filename = f"automation_{tile_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(os.getcwd(), log_filename)
    
    # Configure logging to write to both file and console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logging.info(f"Logging initialized. Log file: {log_path}")
    return log_path

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Automation script for TEM climate scenario processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Run modes:
  --base           Run only the base scenario (pull tile, gapfill, generate scenarios, 
                   split and run base scenario). Stop before processing other scenarios.
  --scenarios      Run only the scenario processing (assumes base run is complete).
                   Split, modify, and process all remaining scenarios.
  --full          Run everything from base to all scenarios (default behavior).

Examples:
  python automation_script.py my_tile --base
  python automation_script.py my_tile --scenarios  
  python automation_script.py my_tile --full
  python automation_script.py my_tile  # same as --full
        """
    )
    
    parser.add_argument("tile_name", help="Name of the tile to process")
    
    # Create mutually exclusive group for run modes
    run_mode = parser.add_mutually_exclusive_group()
    run_mode.add_argument("--base", action="store_true", 
                         help="Run only the base scenario processing")
    run_mode.add_argument("--scenarios", action="store_true",
                         help="Run only the scenarios processing (assumes base is complete)")
    run_mode.add_argument("--full", action="store_true", 
                         help="Run everything from base to all scenarios (default)")
    
    args = parser.parse_args()
    
    # Default to full run if no mode specified
    if not (args.base or args.scenarios or args.full):
        args.full = True
    
    tile_name = args.tile_name
    log_path = setup_logging(tile_name)
    print(f"Log file created at: {log_path}")
    
    base_scenario_name = "ssp1_2_6_access_cm2__ssp1_2_6"  # Change if needed
    path_to_folder = os.getcwd()
    
    # Determine run mode for logging
    if args.base:
        run_mode_str = "base"
    elif args.scenarios:
        run_mode_str = "scenarios"
    else:
        run_mode_str = "full"
    
    logging.info(f"Starting automation script for tile: {tile_name}")
    logging.info(f"Run mode: {run_mode_str}")
    logging.info(f"Working directory: {path_to_folder}")
    logging.info(f"Base scenario: {base_scenario_name}")

    # BASE RUN SECTION
    if args.base or args.full:
        logging.info("=== STARTING BASE RUN ===")
        pull_tile(tile_name)
        run_gapfill(tile_name)
        generate_scenarios(tile_name)

        base_split_path = split_base_scenario(path_to_folder, tile_name, base_scenario_name)
        run_batch_scenario(base_split_path)
        wait_for_jobs()
        merge_and_plot(base_split_path)
        logging.info("=== BASE RUN COMPLETED ===")
        
        if args.base:
            logging.info("Base run completed. Exiting.")
            return

    # SCENARIOS RUN SECTION  
    if args.scenarios or args.full:
        # For scenarios-only mode, check if base run has been completed
        if args.scenarios:
            base_summary_path = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}_split/all_merged/summary_plots.pdf"
            if not os.path.exists(base_summary_path):
                logging.error(f"[ERROR] Base run has not been completed. Missing file: {base_summary_path}")
                logging.error(f"Please run the base scenario first using: python automation_script.py {tile_name} --base")
                print(f"ERROR: Base run must be completed before running scenarios.")
                print(f"Missing file: {base_summary_path}")
                print(f"Please run: python automation_script.py {tile_name} --base")
                sys.exit(1)
            logging.info("Base run verification passed - found base scenario summary_plots.pdf")
        
        logging.info("=== STARTING SCENARIOS RUN ===")
        scenarios = split_rest_scenarios(path_to_folder, tile_name, base_scenario_name)
        logging.info(f"Found {len(scenarios)} scenarios to process: {scenarios}")
        
        modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios)
        process_remaining_scenarios(path_to_folder, tile_name, scenarios)
        logging.info("=== SCENARIOS RUN COMPLETED ===")
    
    logging.info("Automation script completed successfully")

if __name__ == "__main__":
    main()
