# TEM Lustre Modules setup

This directory contains scripts for setting up modules for TEM (Terrestrial Ecosystem Model).

## Default location (/mnt/exacloud)

```bash
export LUSTRE_HOME=/mnt/exacloud/lustre
export SW=$LUSTRE_HOME/software          # installed packages
export SRC=$LUSTRE_HOME/src              # downloaded tarballs + source
export MODULES=$LUSTRE_HOME/modulefiles  # Lmod modulefiles
chmod +x "$LUSTRE_HOME/install/install_dvmdostem_deps.sh"
```

## Set the dependancies ( might take some time).
```bash
bash "$LUSTRE_HOME/install/install_dvmdostem_deps.sh"
```

## Add these lines into slurm job file (modify the correspoding bp template)a
```bash
source /etc/profile.d/z00_lmod.sh
module purge
module use /mnt/exacloud/lustre/modulefiles

module load openmpi
module load dvmdostem-deps/2026-02
```

## Copy Makefile to dvm-dos-tem folder
```bash
make USEMPI=true
```
