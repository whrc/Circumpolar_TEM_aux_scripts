EDA tools for the DVM-DOS-TEM Circumpolar run. 

# Circumpolar Run Work Plan

## Stage I: Alaska Tiles

This document outlines the workflow for processing Alaska tiles as part of the Circumpolar run.

---

## Tile Workload Assignment

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

---

## Initial Setup

1. **Disable DSL in config**  
   Navigate to the `dvm-dos-tem/config/config.js` file and **turn off the DSL setting** for all stages. *(Note: the current `pr` branch may have it enabled.)*

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

Run Hélène’s gap-filling script:

```bash
python process_climate_data_gapfill.py /path/to/tile
```

> **Note**: You may need to rename the gap-filled output file (this step will be improved in future versions).

It could be worse to check the `run-mask.nc` file in the tile. If all zeros, it means that all gridcells are off.   
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
bp batch split \
  -i ssp1_2_6_access_cm2__ssp1_2_6 \
  -b regionalinputs/<tile_name>_scen/ssp1_2_6_access_cm2__ssp1_2_6_split \
  --p 100 --e 2000 --s 200 --t 123 --n 76
```

This will create 100 batch folders in the `..._split` directory.

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

Plotting scripts are currently under development by Doğukan. This section will be updated once the plotting tool is finalized.

---

## Notes

- Make sure you are working within your own namespace/folder on Exacloud to avoid conflicts.
- Follow naming conventions carefully to ensure smooth merging and reproducibility.

---

## To Do

- [ ] Add monitoring script
- [ ] Finalize plotting script
- [ ] Automate renaming of gap-filled files

