# TEM Output Tile Debuging

This directory contains scripts for debuging TEM (Terrestrial Ecosystem Model) output tiles from Google Cloud Storage.

## Scripts

### fix_tile.py

Check tile completion status for SSP scenarios (ssp_1_2_6 and ssp_5_8_5) and automatically fix failed tiles.

**Usage:**
```bash
# Check tiles from a file
python debug/fix_tile.py tiles/file_name.txt

# Check a single tile
python debug/fix_tile.py --tile H7_V8

# Check and automatically fix failed tiles
python debug/fix_tile.py --tile H7_V8 --fix
```

**Options:**
- `--tile TILE, -t TILE`: Single tile name to check (e.g., H7_V8)
- `--fix`: Automatically pull and retry failed tiles
- `--submit`: Automatically submit SLURM jobs for retry batches (requires --fix)
- `--sync`: Sync results back to bucket after retry (requires --fix)
- `--bucket-path PATH`: GCS bucket path (default: circumpolar_model_output/recent2)
- `--partition, -p PARTITION`: SLURM partition for retry jobs (default: spot)
- `--nowalltime`: Remove #SBATCH --time lines from retry batch slurm scripts

**What it does:**
1. **Priority 1: Check local directory first** (`{tile_name}_sc` or `{tile_name}`)
   - If local directory exists: checks local completion for all scenarios
   - If local completion available: uses local data (shows "XX.XX% (local)")
   - If local >99% → Reports "PASSED (local)" and **skips retry**
   - If local ≤99% → Reports "FAILED" and adds to retry queue
2. **Priority 2: Check bucket only if needed**
   - If local directory doesn't exist: downloads run_status.nc and run-mask.nc from GCS bucket
   - If local check fails for a scenario: falls back to bucket data for that scenario
   - Uses bucket completion percentage when local is not available
3. For each scenario (ssp_1_2_6 and ssp_5_8_5):
   - Reports "PASSED" if completion >99%, otherwise "FAILED"
4. If `--fix` is enabled and scenarios truly failed:
   - Uses existing local directory if available (saves bandwidth)
   - Otherwise pulls tile from GCS bucket to `{tile_name}_sc` directory
   - Runs `batch_status_checker.py --individual-retry` only for scenarios that need fixing
   - Creates retry batches for incomplete runs
   - If `--submit` is enabled: automatically submits SLURM jobs for retry batches
   - If `--sync` is enabled: syncs results back to bucket after retry using `sync_tile_to_bucket.py`
   - Logs output to `LOG/{tile_name}_debug_{scenario}.log`

**Examples:**
```bash
# Check a single tile
python debug/fix_tile.py --tile H8_V16

# Check and fix a single tile (create retry batches only)
python debug/fix_tile.py --tile H7_V8 --fix

# Check, fix, and auto-submit SLURM jobs without walltime limits
python debug/fix_tile.py --tile H7_V8 --fix --submit --nowalltime

# Check, fix, submit, and sync results back to bucket
python debug/fix_tile.py --tile H7_V8 --fix --submit --sync

# Check completion for test tiles from file
python debug/fix_tile.py tiles/test_tile.txt

# Check, fix, submit, and sync all failed tiles with custom partition and no walltime
python debug/fix_tile.py tiles/unfinished_ak_can.txt --fix --submit --sync --partition dask --nowalltime

# Use custom bucket path
python debug/fix_tile.py --tile H7_V8 --fix --submit --bucket-path circumpolar_model_output/test
```

**Requirements:**
- Python 3.x with xarray, numpy, netCDF4
- Google Cloud SDK with gsutil installed and configured
- Access to GCS bucket: `gs://circumpolar_model_output/`

## Scripts

### batch_status_checker.py

Check run status of batches and optionally create retry batches for unfinished ones.

**Options:**
- `--individual-retry`: Create retry batches for all unfinished batches
- `--submit`: Automatically submit SLURM jobs for retry batches (requires --individual-retry)
- `--log-file PATH`: Save output to log file
- `-p, --partition PARTITION`: SLURM partition to use (default: dask)
- `--nowalltime`: Remove #SBATCH --time lines from retry batch slurm scripts

**Example:**
```bash
# Check batch completion and create retry batches without time limits
python debug/batch_status_checker.py /path/to/scenario --individual-retry --nowalltime --submit -p spot
```

## Instructions

The debuging process:
1. Check **LOG** files for completion information
2. If incomplete, use automation script with this configuration 
   ```bash
   python ~/Circumpolar_TEM_aux_scripts/automation_script.py --mode sc -bucket circumpolar_model_output/recent2 TILE_ID > LOG/TILE_ID.log 2>&1
   ```
   It will pull the correspoding `TILE_ID` and store in the current folder like `TILE_ID_sc`
3. Check for completion: 
   ```bash
   python ~/Circumpolar_TEM_aux_scripts/check_runs.py TILE_ID_sc/ssp5_8_5_mri_esm2_0_split/
   ```
4. If all batches are finished but merge is incomplete, (for scenarious) then it is likely files numbers in the batch do not match. Before eexecuting chages with the `sync_tile_to_bucket.py` use `-h` , and `--dry-run` to see if it doing what you expect.Run `sync_tile_to_bucket.py` with the following options:
   ```bash
   python ~/Circumpolar_TEM_aux_scripts/sync_tile_to_bucket.py TILE_ID --trim --merge ssp5_8_5_mri_esm2_0 /mnt/exacloud/ejafarov_woodwellclimate_org/fix_unfinished_job --force-merge
   ```  
5. Then sync the `all_merged` folder to the correspoding folder in the bucket
   ```bash
   python ~/Circumpolar_TEM_aux_scripts/sync_tile_to_bucket.py TILE_ID --sync --all_merged ssp5_8_5_mri_esm2_0 /mnt/exacloud/ejafarov_woodwellclimate_org/fix_unfinished_job 
   ```
   combine steps 4 and 5
   ```bash
   python ~/Circumpolar_TEM_aux_scripts/sync_tile_to_bucket.py TILE_ID --trim --merge --sync --all_merged ssp1_2_6_mri_esm2_0 /mnt/exacloud/ejafarov_woodwellclimate_org/fix_unfinished_job --force-merge
   ```
6. If instead `ssp1_2_6` is incomplete then do step **3** for `ssp1_2_6`. IF not completed or `<90%` completion, it is better to re-submit. NOTE: base-case is set to `ssp1_2_6`, so we need to re-assign the base-case. 
   ```bash
   python ~/Circumpolar_TEM_aux_scripts/automation_script.py --mode sc -bucket circumpolar_model_output/recent2 --base-scenario-name ssp5_8_5_access_cm2 TILE_ID > LOG/TILE_ID.log 2>&1
   ```


## Prerequisites

- Python 3.x with required packages (xarray, pandas, numpy)
- Google Cloud SDK with `gsutil` installed and configured
- Access to the required Google Cloud Storage buckets:
  - `gs://circumpolar_model_output/`
  - `gs://regionalinputs/CIRCUMPOLAR/`

