Tools for the DVM-DOS-TEM Circumpolar run input/output data processing. 

# Circumpolar Run Work Plan

## The map of the Circumpolar

![Map of the Circumpolar](circ_map_ids.jpg)

## Empty tiles
|    |    |     |     |    |    |
|---------|---------|---------|---------|---------|---------|
| H10_V7  | H11_V19 | H11_V20 | H12_V18 | H12_V20 | H12_V4  |
| H12_V6  | H13_V20 | H14_V20 | H3_V14  | H9_V13  |         |

HG: All these tiles have land (as delineated in the example below - blue polygons).
They were thus included in the original circumpolar map where a 1 mile buffer were applied
to the boundary of the land (as delineated in the example below - pink polygons).
However, some of this land (e.g. tiny islands of the Aleutians) were not covered by the 
vegetation map for instance. 

![Example of an empty tile](empty_tile_example1.jpeg)

Empty tile could also result from being located entirely on Greenland icefield (e.g. H10_V7)

![Example of an empty tile](empty_tile_example2.jpeg)

One last case occuring in this list of empty tiles, is the lack of coverage from the 
vegetation map in the sourthern most regions (see example 3 below) 

![Example of an empty tile](empty_tile_example3.jpeg)


## Stage I: Alaska Tiles [constant OLT]

This document outlines the workflow for processing Alaska tiles as part of the Circumpolar run.

---

## Initial Tile Workload Assignment

| Elchin       | Valeria      | Doğukan     |
|--------------|--------------|-------------|
| H10_V14      | H11_V20      | H8_V14      |
| H10_V15      | H12_V18      | H8_V15      |
| H10_V16      | H12_V20      | H8_V16      |
| H10_V17      | H13_V20      | H8_V17      |
| H10_V18      | H14_V20      | H8_V18      |
| H10_V19      | H5_V15       | H9_V14      |
| H11_V14      | H5_V16       | H9_V15      |
| H11_V15      | H6_V15       | H9_V16      |
| H11_V16      | H6_V16       | H9_V17      |
| H11_V17      | H7_V15       | H9_V18      |
| H11_V18      | H7_V16       | H9_V19      |
| H11_V19      |              |             |

## After Tile Analysis
Removed tiles with run-status = 0 for all grid cells. 

## Tile Workload Assignment

| Elchin       | Valeria      | Doğukan     |
|--------------|--------------|-------------|
| H10_V14      |              | H8_V14      |
| H10_V15      |              | H8_V15      |
| H10_V16      |              | H8_V16      |
| H10_V17      |              | H8_V17      |
| H10_V18      |              | H8_V18      |
| H10_V19      | H5_V15       | H9_V14      |
| H11_V14      | H5_V16       | H9_V15      |
| H11_V15      | H6_V15       | H9_V16      |
| H11_V16      | H6_V16       | H9_V17      |
| H11_V17      | H7_V15       | H9_V18      |
| H11_V18      | H7_V16       | H9_V19      |
|              |              |             |


---

## Initial Setup

1. **Disable DSL in config**  
   Navigate to the `dvm-dos-tem/config/config.js` file and **turn off the DSL setting** for all stages. *(Note: the current `pr` branch may have it enabled.)*

HG: Why? It hasn't clearly been demonstrated that DSL was the reason for any error in the model. Cold climate simulations result in soil thermal computation being very slow, 
but the cause is very likely the cold climate, not the  DSL. So unless there is more evidence of serious issues with DSL, it should be turned on! We loose a lot of process-based dynamic by turning it off. Furthermore, all calibrations have been conducted with DSL on.

2. **Create Alaska Working Directory**  
   On `/mnt/exacloud`, create your working folder using your username:
   ```
   /mnt/exacloud/<yourname>_woodwellclimate_org/Alaska
   ```
   Copy your assigned tiles into this folder and navigate into the working tile directory.
   ```bash
   gsutil -m  cp -r gs://regionalinputs/CIRCUMPOLAR/<tile_name>
   ```
---

## Workflow Steps

### 1. Process Climate Data

It could be worse to check the `run-mask.nc` file in the tile. If all zeros, it means that all gridcells are off.
Run Hélène’s gap-filling script:

```bash
python process_climate_data_gapfill.py /path/to/tile
```

> **Note**: You may need to rename the gap-filled output file (this step will be improved in future versions).
HG: agreed - until the outputs have been thoroughly investigated - let's store the 
gap-filled data in separate files from the original downscaled climate data.
   
---

### 2. Analyze Input Data

Check for errors or anomalies (e.g., negative precipitation or NIRR values):

```bash
python analyze_TEM_nc.py /path/to/folder_or_file
```

---

### 3. Generate Climate Scenarios

Generate SSP1-2.6 scenarios using:

```bash
python generate_climate_scenarios.py /path/to/input_folder /path/to/output_folder
```

Only **`ssp1_2_6_access_cm2__ssp1_2_6`** will be used in this stage.

---

### 4. Split Scenarios into Batches

From the directory **above** the scenario folder, run:

```bash
bp batch split -i ssp1_2_6_access_cm2__ssp1_2_6 -b <path_to_folder>/<tile_name>_scen/ssp1_2_6_access_cm2__ssp1_2_6_split \
  --p 100 --e 2000 --s 200 --t 123 --n 76
```

This will create 100 batch folders in the `..._split` directory.

HG: transient run should be 124 years! The historical inputs run from January 1st 1901 to December 31st 2024, i.e. 124 years. 

---

### 5. Run Model Batches

- **Run all batches at once**:
  ```bash
  bp batch run -b /mnt/exacloud/your_folder/Alaska/tilename/ssp1_2_6_access_cm2__ssp1_2_6_split`
  ```

- **Run a single batch**:
  Navigate to the batch folder and run:
  ```bash
  sbatch slurm_runner.sh
  ```

---

### 6. Monitor Execution

Use `squeue` or `sacct` to monitor job progress:

```bash
squeue -u $USER
sacct -j <job_id>
python check_runs.py ssp1_2_6_access_cm2__ssp1_2_6_split
```

> An auxiliary monitoring script will be added to this repo soon.

---

### 7. Merge Output

Once all batch jobs are complete:

```bash
bp batch merge -b <tile_name>
```

---

### 8. Plotting Results

Once all batches are merged, run the plotting script.

```bash
python plot_nc_all_files.py full_path_your_tile/ssp1_2_6_access_cm2__ssp1_2_6_split/all_merged/
```

---

## Stage II: New scenario: ssp1_2_6_mri_esm2_0__ssp1_2_6

Once Stage I is complete, we can proceed to the next scenario.  
For this stage, there is **no need** to re-run the `-pr`, `-eq`, or `-sp` phases.  
Instead, we will use the script below to:  

1. Copy the `restart-tr.nc` file into each batch directory.  
2. Modify the Slurm job script in each batch to enable a restart run.  

Before running this script, the new scenario must first be split into batches — see [Step 4](#4-split-scenarios-into-batches) for details.  

```bash
python generate_next_scenario.py path_to_scenario/ssp1_2_6_access_cm2__ssp1_2_6_split path_to_next_scenario/..._split
```

After completing this step, repeat Steps **5** through **8** from **Stage I**.

The following command automates splitting and file copying steps for all scenarios. Keep this script in the folder above the scenario folder. 
```bash
python orchestrate_scenarios.py --path-to-folder /mnt/exacloud/ejafarov_woodwellclimate_org/Alaska/ \
--tile-dir H10_V14_sc --new-scenario-script generate_next_scenario.py
```

## Notes

- Make sure you are working within your own namespace/folder on Exacloud to avoid conflicts.
- Follow naming conventions carefully to ensure smooth merging and reproducibility.

---

## To Do

- [x] Add monitoring script
- [x] Finalize plotting script
- [x] Automate renaming of gap-filled files
- [ ] Automate new scenario generation and job submission script
