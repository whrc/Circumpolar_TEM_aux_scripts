#!/bin/bash
set -e

cd "$HOME"

# Clone Circumpolar_TEM_aux_scripts if not already present
if [ -d "Circumpolar_TEM_aux_scripts" ]; then
    echo "Circumpolar_TEM_aux_scripts already exists — skipping clone."
else
    git clone https://github.com/whrc/Circumpolar_TEM_aux_scripts.git
fi

# Clone batch-processing if not already present
if [ -d "batch-processing" ]; then
    echo "batch-processing already exists — skipping clone."
else
    git clone https://github.com/Elchin/batch-processing.git
fi

echo Installing additional dependencies...
pip install --break-system-packages xarray netcdf4 h5netcdf
pip install commentjson --break-system-packages

echo install batch-processing
pipx install ~/batch-processing/ --editable 
pipx ensurepath
pipx inject batch-processing h5py
bp --help

echo setting tem folder and compiling...
USER_HOME=${HOME}
bp init --basedir ${USER_HOME} --compile

echo setup scripts and utils...
mkdir old_tem && cd old_tem
git clone https://github.com/uaf-arctic-eco-modeling/dvm-dos-tem.git
cd dvm-dos-tem
git checkout cbb3ba
cd ~/
cp -r old_tem/dvm-dos-tem/calibration dvm-dos-tem/.
mkdir dvm-dos-tem/scripts/util
cp -r  old_tem/dvm-dos-tem/scripts/util dvm-dos-tem/scripts/.

echo NOTE: First time istall requires re-login and re-run this script again
echo Completed.
