import os
import sys
import subprocess
import time
from pathlib import Path
import re
import argparse
import shutil

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
    os.makedirs(input_tiles_dir, exist_ok=True)
    run_cmd(f"gsutil -m cp -r gs://regionalinputs/CIRCUMPOLAR/{tile_name} {input_tiles_dir}/.")

def pull_exisitng_tile_output_from_bucket(bucket_path, tile_name):
    """
    Pulls exisitng tile from the bucket (we do this for check or fresh resubmit of failed batches)
    If bucket or tile folder is not found, error is handled.
    """
    os.makedirs(tile_name+'_sc', exist_ok=True)

    try:
        print(f"Pulling gs://{bucket_path}/{tile_name} ...")
        run_cmd(f"gsutil -m cp -r gs://{bucket_path}/{tile_name}/* {tile_name}_sc/")
        print("Download completed successfully.")
    except Exception as e:
        if "No URLs matched" in str(e) or "BucketNotFoundException" in str(e):
            print(f"Bucket or path gs://{bucket_path}/{tile_name} not found.")
        else:
            print(f"An unexpected error occurred while accessing the bucket.")
        print(f"Details: {e}")


def sync_scenario_to_bucket(bucket_name, tile_name, scenario):
    """
    Checks if gs://bucket_name/tile_name/scenario exists.
    If it does, deletes it, then uploads local tile_name/scenario to the same location.
    """
    gcs_path = f"gs://{bucket_name}/{tile_name}/{scenario}"
    local_path = f"{tile_name}_sc/{scenario}"

    # Step 1: Check if the scenario exists in the bucket
    check_cmd = f"gsutil ls {gcs_path}"
    try:
        subprocess.run(check_cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"[SYNC] Found existing scenario at {gcs_path}, deleting it...")
        
        # Step 2: If found, delete the existing folder
        delete_cmd = f"gsutil -m rm -r {gcs_path}"
        print('[SYNC] Deleting: ', delete_cmd)
        subprocess.run(delete_cmd, shell=True, check=True)
    except subprocess.CalledProcessError:
        print(f"[SYNC] Scenario not found in bucket: {gcs_path} — skipping deletion.")

    # Step 3: Upload the local scenario
    print(f"[SYNC] Uploading {local_path} to {gcs_path} ...")
    upload_cmd = f"gsutil -m cp -r {local_path} {gcs_path}"
    print('[SYNC] Copying: ', upload_cmd)
    try:
        subprocess.run(upload_cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌[SYNC] Upload failed: {e}")


def remove_tile(tile_name):
    """Remove the downloaded tile to save space after processing."""
    input_tiles_dir = "input_tiles"
    tile_path = os.path.join(input_tiles_dir, tile_name)
    
    if os.path.exists(tile_path):
        print(f"[CLEANUP] Removing tile directory: {tile_path}")
        shutil.rmtree(tile_path)
        print(f"[CLEANUP] Successfully removed {tile_path}")
    else:
        print(f"[INFO] Tile directory {tile_path} not found, skipping removal")

def conform_runmask(tile_name):
    """Run runmask.py to conform masks to available parameters for all scenarios."""
    print("[RUNMASK] Conforming run masks to available parameters...")
    
    # Set up environment with PYTHONPATH
    env = os.environ.copy()
    dvm_scripts_path = "/opt/apps/dvm-dos-tem/scripts"
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{dvm_scripts_path}:{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = dvm_scripts_path
    
    # Parameters directory
    params_dir = "/opt/apps/dvm-dos-tem/parameters/"
    runmask_script = "/opt/apps/dvm-dos-tem/scripts/util/runmask.py"
    
    # Find all scenario directories
    scenario_base_dir = f"{tile_name}_sc"
    if not os.path.exists(scenario_base_dir):
        print(f"[WARNING] Scenario directory {scenario_base_dir} not found, skipping runmask")
        return
    
    for scenario_dir in os.listdir(scenario_base_dir):
        scenario_path = os.path.join(scenario_base_dir, scenario_dir)
        if os.path.isdir(scenario_path) and not scenario_dir.endswith("_split"):
            vegetation_file = os.path.join(scenario_path, "vegetation.nc")
            runmask_file = os.path.join(scenario_path, "run-mask.nc")
            
            if os.path.exists(vegetation_file) and os.path.exists(runmask_file):
                print(f"[RUNMASK] Processing scenario: {scenario_dir}")
                cmd = [
                    "python", runmask_script,
                    "--conform-mask-to-available-params",
                    params_dir,
                    vegetation_file,
                    runmask_file
                ]
                try:
                    subprocess.run(cmd, env=env, check=True)
                    
                    # Replace original run-mask.nc with the filtered version
                    filtered_runmask_file = os.path.join(scenario_path, "run-mask_cmtfilter.nc")
                    if os.path.exists(filtered_runmask_file):
                        print(f"[RUNMASK] Replacing run-mask.nc with filtered version for {scenario_dir}")
                        os.remove(runmask_file)  # Remove original
                        os.rename(filtered_runmask_file, runmask_file)  # Rename filtered to original name
                        print(f"[RUNMASK] Successfully updated run-mask.nc for {scenario_dir}")
                    else:
                        print(f"[WARNING] Expected filtered file run-mask_cmtfilter.nc not found for {scenario_dir}")
                        
                except subprocess.CalledProcessError as e:
                    print(f"[ERROR] Runmask failed for {scenario_dir}: {e}")
            else:
                print(f"[WARNING] Missing vegetation.nc or run-mask.nc in {scenario_dir}")
    
    print("[RUNMASK] Completed runmask conforming")

def run_gapfill(tile_name):
    input_tiles_dir="input_tiles"
    run_cmd(f"python ~/Circumpolar_TEM_aux_scripts/process_climate_data_gapfill.py {input_tiles_dir}/{tile_name}")

def generate_scenarios(tile_name):
    input_tiles_dir="input_tiles"
    run_cmd(f"python ~/Circumpolar_TEM_aux_scripts/generate_climate_scenarios.py {input_tiles_dir}/{tile_name} {tile_name}_sc")

def split_base_scenario(path_to_folder, tile_name, base_scenario_name):
    input_path = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}"
    output_path = f"{input_path}_split"
    run_cmd(f"bp batch split -i {input_path} -b {output_path} --p 100 --e 2000 --s 200 --t 124 --n 76")
    return output_path


def check_run_completion(folder_path):
    try:
        # Expand the home directory path properly
        script_path = os.path.expanduser("~/Circumpolar_TEM_aux_scripts/check_tile_run_completion.py")

        # Run the check_tile_run_completion.py script and capture output
        result = subprocess.run(
            ["python", script_path, folder_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,  # Changed from DEVNULL to see errors
            text=True,
            check=True
        )
        print(f"[C CHECK] Script output: {result.stdout}")

        # Search for "Overall Completion: XX.XX%"
        match = re.search(r"Overall Completion:\s+(\d+(?:\.\d+)?)%", result.stdout)
        if match:
            completion = float(match.group(1))
            return completion
        else:
            print(f"[C DEBUG] No completion match found in output")

    except subprocess.CalledProcessError as e:
        print(f"[C ERROR] Script failed for {folder_path}: {e.stderr}")
    except Exception as e:
        print(f"[C ERROR] Unexpected error for {folder_path}: {e}")

    return None


def resubmit_unfinished_jobs(split_path):
    """Run the resubmit_unfinished.py script to resubmit any unfinished jobs."""
    print(f"[RESUBMIT] Checking for unfinished jobs in {split_path}")
    resubmit_script = os.path.expanduser("~/Circumpolar_TEM_aux_scripts/resubmit_unfinished.py")
    run_cmd(f"python {resubmit_script} {split_path}")

def resubmit_unfinished_jobs_fresh(split_path):
    """Run the resubmit_unfinished.py script to resubmit any unfinished jobs."""
    print(f"[RESUBMIT FRESH] Checking for unfinished jobs in {split_path}")
    resubmit_script = os.path.expanduser("~/Circumpolar_TEM_aux_scripts/resubmit_unfinished_fresh.py")
    run_cmd(f"python {resubmit_script} {split_path}")

def run_batch_scenario(split_path):
    #before submitting batches check for completion
    completion = check_run_completion(split_path)
    #if complete,skip: `batch run`
    if completion is not None and completion > 90.0:
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

def trim_sc_files(sc_path):

    try:
        print(f"Triming files in :{sc_path} ...")
        run_cmd(f"python  ~/Circumpolar_TEM_aux_scripts/trim_batch_files.py {sc_path}")
        print("Triming completed successfully.")
    except Exception as e:
        print(f"An unexpected error occurred while trimming.")
        print(f"Details: {e}")


def merge_and_plot(split_path):
    plot_file = Path(f"{split_path}/all_merged/summary_plots.pdf")
    #plot_file = f"{split_path}/all_merged/summary_plots.pdf"
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
    #currently we go only for 3 scenarious
    scenarios_nosplit = [s for s in scenarios if "_split" not in s]
    scenarios_nosplit = ['ssp5_8_5_mri_esm2_0']

    for scenario in scenarios_nosplit:
        input_path = scenario_dir / scenario
        output_path = f"{input_path}_split"
        run_cmd(f"bp batch split -i {input_path} -b {output_path}")
    return scenarios_nosplit

def modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios):
    base_folder = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}_split"
    for scenario in scenarios:
        scenario_folder = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        run_cmd(f"python ~/Circumpolar_TEM_aux_scripts/generate_next_scenario.py {base_folder} {scenario_folder}")

def process_remaining_scenarios(path_to_folder, tile_name, scenarios):
    for scenario in scenarios:
        split_path = f"{path_to_folder}/{tile_name}_sc/{scenario}_split"
        run_batch_scenario(split_path)
        wait_for_jobs()
        resubmit_unfinished_jobs_fresh(split_path)
        wait_for_jobs()
        trim_sc_files(split_path)
        merge_and_plot(split_path)

def print_completion_status(path_to_folder, tile_name):
    """Print completion status for all scenarios using check_run_completion."""
    print(f"[STATUS] Checking completion status for {tile_name}")
    scenario_base_dir = f"{path_to_folder}/{tile_name}_sc"
    
    if not os.path.exists(scenario_base_dir):
        print(f"[WARNING] Scenario directory {scenario_base_dir} not found")
        return
    
    # Check all split scenarios
    for item in os.listdir(scenario_base_dir):
        item_path = os.path.join(scenario_base_dir, item)
        if os.path.isdir(item_path) and item.endswith("_split"):
            completion = check_run_completion(item_path)
            if completion is not None:
                print(f"[STATUS] {item}: {completion:.2f}% complete")
            else:
                print(f"[STATUS] {item}: Unable to determine completion status")

def finalize(path_to_folder, tile_name):
    """Copy the results to Google Cloud Storage once the run is finished."""
    print(f"[FINALIZE] Copying results to Google Cloud Storage for {tile_name}")
    source_path = f"{path_to_folder}/{tile_name}_sc"
    destination_path = f"gs://circumpolar_model_output/recent2/{tile_name}"
    
    if os.path.exists(source_path):
        run_cmd(f"gsutil -m cp -r {source_path} {destination_path}")
        print(f"[FINALIZE] Successfully copied {source_path} to {destination_path}")
        #delete folder to keep exascaler clean
        shutil.rmtree(source_path)
        print(f"✅  Folder deleted: {source_path}")
    else:
        print(f"[ERROR] Source path {source_path} does not exist, skipping finalize step")


def main():
    parser = argparse.ArgumentParser(
        description="Run automation for a tile. Use --mode sc (default) or --mode full."
    )
    #parser.add_argument("tile_name", help="Tile name, e.g., H10_V16")
    parser.add_argument("tile_name",  help="Tile name, e.g., H10_V16 (optional when using -bucket)")
    parser.add_argument(
        "--mode",
        choices=["sc", "full", "base"],
        default="sc",
        help="Execution mode: 'sc' runs scenario-only steps; 'full' runs end-to-end.",
    )
    parser.add_argument(
        "--base-scenario-name",
        default="ssp1_2_6_mri_esm2_0",
        help="Base scenario folder name (default: ssp1_2_6_access_cm2)",
    )
    parser.add_argument(
        "-bucket",
        "--bucket-path",
        help="Google Cloud Storage path to a file containing tile IDs (e.g., gs://bucket/tiles.txt). When specified, processes multiple tiles in sc mode.",
    )
    args = parser.parse_args()

    tile_name = args.tile_name
    base_scenario_name = args.base_scenario_name
    path_to_folder = os.getcwd()


    # For non-bucket modes, tile_name is required
    if not args.tile_name:
        print("[ERROR] tile_name is required when not using -bucket option")
        parser.print_help()
        sys.exit(1)
    
    if args.mode == "full":
        print("[MODE] full — running end-to-end pipeline")

        # formerly-commented steps
        pull_tile(tile_name)
        run_gapfill(tile_name)
        generate_scenarios(tile_name)
        conform_runmask(tile_name)
        remove_tile(tile_name)

        # full pipeline uses your splitter to derive the base scenario path
        base_split_path = split_base_scenario(path_to_folder, tile_name, base_scenario_name)
        run_batch_scenario(base_split_path)
        wait_for_jobs()
        resubmit_unfinished_jobs(base_split_path)
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
        conform_runmask(tile_name)
        remove_tile(tile_name)

        # full pipeline uses your splitter to derive the base scenario path
        base_split_path = split_base_scenario(path_to_folder, tile_name, base_scenario_name)
        run_batch_scenario(base_split_path)
        wait_for_jobs()
        resubmit_unfinished_jobs(base_split_path)
        wait_for_jobs()
        merge_and_plot(base_split_path)

    else:
        if args.bucket_path:
            if args.mode != "sc":
                print("[ERROR] -bucket option only works with --mode sc")
                sys.exit(1)

            print("[MODE] sc-bucket — processing multiple tiles from bucket")
            #ex: python ~/Circumpolar_TEM_aux_scripts/automation_script.py H8_V16 --mode sc -bucket circumpolar_model_output/recent2
            pull_exisitng_tile_output_from_bucket(args.bucket_path,tile_name)

            path_to_tile = os.path.join(path_to_folder, tile_name + '_sc')

            # Find all folders with '_split' in the name
            scenario_list = [
                name for name in os.listdir(path_to_tile)
                if os.path.isdir(os.path.join(path_to_tile, name)) and "_split" in name
            ]
            # Remove the base scenario name if it's in the list
            base_scenario_name += "_split"
            if base_scenario_name in scenario_list:
                scenario_list.remove(base_scenario_name)
            print("Scenarios to resubmit:", scenario_list)

            # Loop through each and resubmit
            for scenario_i in scenario_list:
                full_scenario_path = os.path.join(path_to_tile, scenario_i)
                print(full_scenario_path)
                resubmit_unfinished_jobs_fresh(full_scenario_path)
                wait_for_jobs()
                trim_sc_files(full_scenario_path)
                merge_and_plot(full_scenario_path)
                sync_scenario_to_bucket(args.bucket_path,tile_name,scenario_i)
        else:
            print("[MODE] sc — running scenario-only steps")

            # use path_to_folder for base_split_path as requested
            base_split_path = f"{path_to_folder}/{tile_name}_sc/{base_scenario_name}"
            scenarios = split_rest_scenarios(path_to_folder, tile_name, base_scenario_name)
            print("Scenarios to submit:", scenarios)
            modify_new_scenarios(path_to_folder, tile_name, base_scenario_name, scenarios)
            process_remaining_scenarios(path_to_folder, tile_name, scenarios)
        
    # print completion status before finalizing
    print_completion_status(path_to_folder, tile_name)
    
    # finalize by copying results to GCS
    if not args.bucket_path:
        finalize(path_to_folder, tile_name)


if __name__ == "__main__":
    main()


