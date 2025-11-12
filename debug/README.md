# TEM Output Tile Debuging

This directory contains scripts for debuging TEM (Terrestrial Ecosystem Model) output tiles from Google Cloud Storage.

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

