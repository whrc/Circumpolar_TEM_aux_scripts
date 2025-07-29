#!/usr/bin/env python3
"""
Purpose:
    We needed this script to quickly duplicate a batch job and run it serially for debugging purposes.

Usage:
    python duplicated_batch_for_serial.py <src_path>

Example:
    python duplicated_batch_for_serial.py ./batch_job
    # This will create ./batch_job-serial-debug
"""

import os
import sys
import shutil
import re

def copy_and_modify_slurm_runner(src_path):
    # Generate destination path by adding -serial-debug suffix
    src_parent = os.path.dirname(os.path.abspath(src_path))
    src_name = os.path.basename(os.path.abspath(src_path))
    dst_path = os.path.join(src_parent, f"{src_name}-serial-debug")
    
    try:
        shutil.copytree(src_path, dst_path)
    except Exception as e:
        print(f"Error copying directory: {e}")
        sys.exit(1)

    slurm_runner_path = os.path.join(dst_path, "slurm_runner.sh")
    try:
        with open(slurm_runner_path, "r") as f:
            content = f.read()

        # Replace parallel execution with serial execution
        new_content = content.replace("--use-hwthread-cpus", "-np 1")
        
        # Change log level from disabled to debug
        new_content = new_content.replace("-l disabled", "-l debug")
        
        # Update job name to indicate this is a serial/debug version
        job_name_pattern = r'(#SBATCH --job-name=")([^"]+)(")'
        match = re.search(job_name_pattern, new_content)
        if match:
            original_name = match.group(2)
            new_name = f"{original_name}-serial-debug"
            new_content = re.sub(job_name_pattern, f'\\1{new_name}\\3', new_content)
        
        # Update log file path to include serial-debug suffix
        log_path_pattern = r'(#SBATCH -o )([^\s]+)'
        log_match = re.search(log_path_pattern, new_content)
        if log_match:
            original_log_path = log_match.group(2)
            new_log_path = f"{original_log_path}-serial-debug"
            new_content = re.sub(log_path_pattern, f'\\1{new_log_path}', new_content)

        with open(slurm_runner_path, "w") as f:
            f.write(new_content)
    except Exception as e:
        print(f"Error processing {slurm_runner_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python duplicated_batch_for_serial.py <src_path>")
        sys.exit(1)

    src_path = sys.argv[1]

    print("Copying the batch job and modifying slurm_runner.sh...")
    copy_and_modify_slurm_runner(src_path)
    print("Done!")
