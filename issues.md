
# 🚨 Issues Log

## Issue #1

**Tile:** `H10_V15`  
**Problem:** Merge fails for scenarios `ssp2_4_5_mri_esm2_0` and `ssp5_8_5_access_cm2`.

---

### ❌ `ssp2_4_5_mri_esm2_0`
- Contains `fail_log.txt` in batch output folders.
- Path: `H10_V15_sc_split/ssp2_4_5_mri_esm2_0__ssp2_4_5_split/batch_0/output/fail_log.txt`

**Error messages from log:**
```
EXCEPTION!! At pixel at (row, col): (0, 31) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 40) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 41) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 46) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 35) Exception from netcdf: NetCDF: Can't write file
EXCEPTION!! At pixel at (row, col): (0, 59) Exception from netcdf: NetCDF: Can't write file
EXCEPTION!! At pixel at (row, col): (0, 34) Exception from netcdf: NetCDF: Can't write file
EXCEPTION!! At pixel at (row, col): (0, 39) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 63) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 88) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 93) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 67) Exception from netcdf: NetCDF: Can't write file
EXCEPTION!! At pixel at (row, col): (0, 94) Exception from netcdf: NetCDF: Unknown file format
EXCEPTION!! At pixel at (row, col): (0, 72) Exception from netcdf: NetCDF: Can't write file
```

### ❓ `ssp5_8_5_access_cm2`
- Does **not** contain `fail_log.txt`, but merge fails.

---

### 🧪 Merge Command Output

```
>> bp batch merge -b H10_V15_sc_split/ssp5_8_5_access_cm2__ssp5_8_5_split
Checking to see if every batch folder has equal number of output files...
No batch folders found. Aborting.
```

```
>> bp batch merge -b H10_V15_sc_split/ssp2_4_5_mri_esm2_0__ssp2_4_5_split
Checking to see if every batch folder has equal number of output files...
No batch folders found. Aborting.
```
