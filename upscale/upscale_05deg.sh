#!/usr/bin/env bash

set -euo pipefail

### Description:
### Upscale yearly monthly NEE files (12 months per file) from the merged folder.
### Input example:
###   NEE_ssp1_2_6_mri_esm2_0_tr_monthly_1901.nc
### Output example:
###   NEE_ssp1_2_6_mri_esm2_0_tr_monthly_1901_upscaled.nc

########   USER SPECIFICATION   ########

# Use current working directory as base path
base_dir="$(pwd)"

# If running inside a conda environment, prefer its binaries.
if [[ -n "${CONDA_PREFIX:-}" && -d "${CONDA_PREFIX}/bin" ]]; then
  export PATH="${CONDA_PREFIX}/bin:${PATH}"
fi

# Directory with merged yearly NEE files
in_dir="${base_dir}/Circumpolar/merged"

# Reference grid used to define final 0.5-degree extent/resolution
grid="${base_dir}/sample_dataset.nc"

# Path to PPP mask in EPSG:6931 (update if needed)
mask="${base_dir}/aoi_5k_buff_6931_2_0.nc"

# Variable name in the input files
var='NEE'

# Optional output directory (defaults to input directory)
out_dir="${in_dir}"

########   CHECKS   ########

for cmd in ncks ncrename ncap2 ncatted ncrcat gdalinfo gdalwarp ncdump bc; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd"
    exit 1
  fi
done

echo "Using ncrename: $(command -v ncrename)"

# On macOS, detect missing dynamic library dependencies before first run.
if command -v otool >/dev/null 2>&1; then
  ncr_bin="$(command -v ncrename)"
  missing_dep=""
  if dep_lines="$(otool -L "$ncr_bin" 2>/dev/null)"; then
    while IFS= read -r dep; do
      [[ "$dep" == /* ]] || continue
      [[ "$dep" == /usr/lib/* ]] && continue
      if [[ ! -e "$dep" ]]; then
        missing_dep="$dep"
        break
      fi
    done < <(printf '%s\n' "$dep_lines" | awk 'NR>1 {print $1}')
  else
    echo "Warning: could not inspect ncrename dependencies with otool; continuing."
  fi

  if [[ -n "$missing_dep" ]]; then
    echo "ncrename has a missing runtime dependency:"
    echo "  $missing_dep"
    echo "Recommended fix (single consistent conda-forge env):"
    echo "  conda create -n nee_upscale -y -c conda-forge python=3.11 nco gdal libgdal-hdf5 netcdf4 cftime"
    echo "  conda run -n nee_upscale bash -c 'cd \"${base_dir}\" && export PATH=\"\$CONDA_PREFIX/bin:\$PATH\" && ./upscale_05deg.sh'"
    exit 1
  fi
fi

if [[ ! -d "$in_dir" ]]; then
  echo "Input directory does not exist: $in_dir"
  exit 1
fi

if [[ ! -f "$grid" ]]; then
  echo "Reference grid not found: $grid"
  exit 1
fi

if [[ ! -f "$mask" ]]; then
  echo "Mask file not found: $mask"
  exit 1
fi

if ! gdalinfo "$grid" >/dev/null 2>&1; then
  echo "gdalinfo cannot open grid file: $grid"
  echo "If message mentions missing HDF5 plugin, install it in your env:"
  echo "  conda install -n nee_upscale -c conda-forge libgdal-hdf5"
  exit 1
fi

mkdir -p "$out_dir"

########   GRID METADATA   ########

grid_o=($(gdalinfo "$grid" | sed -n -e '/^Origin = /p' | grep -Eo '[+-]?[0-9]+([.][0-9]+)?'))
grid_sz=($(gdalinfo "$grid" | sed -n -e '/^Size is /p' | grep -Eo '[+-]?[0-9]+([.][0-9]+)?'))
grid_res=($(gdalinfo "$grid" | sed -n -e '/^Pixel Size = /p' | grep -Eo '[+-]?[0-9]+([.][0-9]+)?'))

grid_left=$(echo "${grid_o[0]}" | bc)
grid_bottom=$(echo "${grid_o[1]}+${grid_res[1]}*${grid_sz[1]}" | bc)
grid_top=$(echo "${grid_o[1]}" | bc)
grid_right=$(echo "${grid_o[0]}+${grid_res[0]}*${grid_sz[0]}" | bc)
grid_6931=($grid_left $grid_bottom $grid_right $grid_top)

echo "Grid extent: ${grid_6931[*]}"
echo "Grid resolution: ${grid_res[*]}"

########   FILE LOOP (YEAR BY YEAR)   ########

shopt -s nullglob
nee_files=("${in_dir}"/NEE_*_monthly_[0-9][0-9][0-9][0-9].nc)
shopt -u nullglob

if [[ ${#nee_files[@]} -eq 0 ]]; then
  echo "No yearly NEE monthly files found in: $in_dir"
  exit 1
fi

tmp_dir="${out_dir}/tmp_upscale_nee_$$"
mkdir -p "$tmp_dir"
trap 'rm -rf "$tmp_dir"' EXIT

for in_file in "${nee_files[@]}"; do
  base_name="$(basename "$in_file" .nc)"
  year="${base_name##*_}"
  out_file="${out_dir}/${base_name}_upscaled.nc"

  echo "Processing year ${year}: $(basename "$in_file")"

  # Normalize dims to X/Y then merge onto cropped mask to carry grid coords
  ncrename -O -h -d x,Xd -d y,Yd "$in_file" "${tmp_dir}/nee_rename.nc"
  ncks -O -h -x -v x,y,X,Y "${tmp_dir}/nee_rename.nc" "${tmp_dir}/nee_rename.nc"
  ncrename -O -h -d Xd,X -d Yd,Y "${tmp_dir}/nee_rename.nc" "${tmp_dir}/nee_rename.nc"

  ncks -O -h -d Y,29,1961 -d X,50,2290 "$mask" "${tmp_dir}/mask_crop.nc"
  ncks -A -h "${tmp_dir}/nee_rename.nc" "${tmp_dir}/mask_crop.nc"
  mv "${tmp_dir}/mask_crop.nc" "${tmp_dir}/tem_nee.nc"
  ncrename -O -h -d X,longitude -d Y,latitude -v X,longitude -v Y,latitude "${tmp_dir}/tem_nee.nc" "${tmp_dir}/tem_nee.nc"

  # Number of monthly timesteps in this file (usually 12)
  nt=$(ncks -m -v time "${tmp_dir}/tem_nee.nc" | awk '/time =/ {gsub(/;/, "", $3); print $3; exit}')
  if [[ -z "${nt}" ]]; then
    echo "  Could not read time dimension for ${in_file}; skipping."
    continue
  fi

  rm -f "$out_file"

  for ((m=0; m<nt; m++)); do
    month_idx=$((m + 1))
    echo "  Month ${month_idx}/${nt}"

    # Keep original time value from source file
    time_val=$(ncks -H -C -v time -d time,"$m" "${tmp_dir}/tem_nee.nc" | awk -F= '/time\[/ {gsub(/[ ;]/, "", $2); print $2; exit}')

    ncks -O -h -d time,"$m" "${tmp_dir}/tem_nee.nc" "${tmp_dir}/slice.nc"
    gdalwarp -overwrite -of netCDF -r bilinear \
      -s_srs EPSG:6931 -t_srs EPSG:4326 \
      -tr "${grid_res[0]}" "${grid_res[1]}" \
      -te "${grid_6931[@]}" \
      "NETCDF:${tmp_dir}/slice.nc:${var}" "${tmp_dir}/upscaled.nc"

    ncap2 -O -s "defdim(\"time\",1); time[time]={${time_val}};" "${tmp_dir}/upscaled.nc" "${tmp_dir}/upscaled.nc"
    ncatted -O -h -a units,time,c,c,"days since 1901-01-01" "${tmp_dir}/upscaled.nc" "${tmp_dir}/upscaled.nc"
    ncatted -O -h -a calendar,time,c,c,"365_day" "${tmp_dir}/upscaled.nc" "${tmp_dir}/upscaled.nc"
    ncap2 -O -s "NEE[time,lat,lon]=Band1;" "${tmp_dir}/upscaled.nc" "${tmp_dir}/upscaled.nc"
    ncks -O -h -x -v Band1 "${tmp_dir}/upscaled.nc" "${tmp_dir}/upscaled.nc"
    ncrename -O -h -d lat,latitude -d lon,longitude -v lat,latitude -v lon,longitude "${tmp_dir}/upscaled.nc" "${tmp_dir}/upscaled.nc"
    ncks -O -h --mk_rec_dmn time "${tmp_dir}/upscaled.nc" "${tmp_dir}/upscaled.nc"

    if [[ $m -eq 0 ]]; then
      cp "${tmp_dir}/upscaled.nc" "$out_file"
    else
      ncrcat -O -h "$out_file" "${tmp_dir}/upscaled.nc" "${tmp_dir}/concat.nc"
      mv "${tmp_dir}/concat.nc" "$out_file"
    fi
  done

  echo "  Wrote: $(basename "$out_file")"
done

echo "Done. Upscaled yearly NEE files are in: $out_dir"
